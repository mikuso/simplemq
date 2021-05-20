const uuid = require('uuid');
const Connection = require('./connection');
const debug = require('debug')('simplemq:rpcclient');

class RPCClient {
    constructor(mq, options = {}) {
        this.mq = mq;
        this.options = options;
        this.replyConsumer = null;
        this.calls = new Map();
        this.replyId = uuid.v4();

        if (this.options.timeout == null) {
            this.options.timeout = 1000*30;
        }
        if (this.options.ackTimeout == null) {
            this.options.ackTimeout = 1000*2;
        }
    }

    async init() {
        this.replyConsumer = await this.mq.consume({
            expires: this.options.timeout,
            messageTtl: this.options.ackTimeout,
            exchange: 'simplemq.rpc',
            routingKey: this.replyId
        }, msg => {
            if (!msg) return;
            msg.ack();

            const call = this.calls.get(msg.properties.correlationId);
            if (!call) {
                debug(`Ignoring response for unknown call ${msg.properties.correlationId}`);
                return;
            }

            try {
                const body = msg.json;
                if (body.ack) {
                    // This is an acknowledgement that the call has been received
                    if (!call.ack) {
                        // but the call may not have asked for it (e.g. if timeout = 0)
                        return;
                    }
                    return call.ack();
                } else {
                    // This is the call result
                    this.calls.delete(msg.properties.correlationId);

                    if (body.error) {
                        // The call resulted in an error
                        const err = Error(body.error.message);
                        err.stack = call.stack;
                        err.cause = body.error.stack;
                        err.code = body.error.code;
                        err.details = body.error.details;
                        return call.reject(err);
                    } else {
                        // The call succeeded
                        return call.resolve(body.result);
                    }
                }
            } catch (err) {
                // Processing response failed.
                // Return error to the caller.
                return call.reject(err);
            }
        });
    }


    close() {
        if (this.replyConsumer) {
            this.replyConsumer.cancel();
        }
    }

    bindEx(exchange, routingKey) {
        const self = this;
        return new Proxy({}, {
            get: function(target, prop, receiver) {
                if (prop === 'toJSON') {
                    return () => { return; };
                }
                return function(...args){
                    return self.callEx(exchange, routingKey, prop, args);
                };
            }
        });
    }

    bind(queueName) {
        return this.bindEx('', queueName);
    }

    async callEx(exchangeName, routingKey, method, args = [], options = {}) {
        if (!Array.isArray(args)) {
            throw new TypeError(`args must be an Array`);
        }

        // optional options overrides
        const ackTimeout = options.ackTimeout || this.options.ackTimeout;
        const timeout = options.timeout || this.options.timeout;

        const id = uuid.v4();

        const call = {
            timeouts: [],
            stack: Error().stack,
        };
        call.settled = new Promise((res, rej) => {
            call.resolve = res;
            call.reject = rej;
        });
        this.calls.set(id, call);

        if (ackTimeout) {
            // set ack timeout
            const ato = setTimeout(() => {
                call.timeouts.forEach(clearTimeout);
                call.reject(Error("RPC ACK Timeout"));
            }, ackTimeout);
            call.timeouts.push(ato);
            call.ack = () => {
                clearTimeout(ato);
            };
        }

        if (timeout) {
            // set reply timeout
            const rto = setTimeout(() => {
                call.timeouts.forEach(clearTimeout);
                call.reject(Error("RPC Response Timeout"));
            }, timeout);
            call.timeouts.push(rto);
        }

        this.mq.publish(exchangeName, routingKey, {
            method,
            args
        }, {
            expiration: ackTimeout,
            replyTo: this.replyId,
            correlationId: id
        }).catch(err => {
            call.reject(err);
            call.timeouts.forEach(clearTimeout);
        });

        return call.settled;
    }

    async call(queueName, method, args = [], options = {}) {
        return await this.callEx('', queueName, method, args, options);
    }
}

module.exports = RPCClient;
