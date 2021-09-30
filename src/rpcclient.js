const uuid = require('uuid');
const EventEmitter = require('events');
const {pipeline, Writable} = require('stream');
const debug = require('debug')('simplemq:rpcclient');
const ChannelAssertions = require('./channel-assertions');
const {deserializeError} = require('serialize-error');

class RPCClient extends EventEmitter {
    constructor(mq, {rpcExchangeName = 'simplemq2.rpc', channelName, signal, recoveryRetries = Infinity, concurrency = 1, timeout = 1000*30, ackTimeout = 1000*5} = {}) {
        super();
        this.mq = mq;
        this.isClosed = false;

        this.timeout = timeout;
        this.ackTimeout = ackTimeout;

        const queueName = 'simplemq2.responses.' + uuid.v4();

        this.calls = new Map();
        this.replyId = queueName;
        this.rpcExchangeName = rpcExchangeName;

        const assertions = new ChannelAssertions({
            exchanges: [
                {
                    name: rpcExchangeName,
                    type: 'direct',
                    options: {
                        durable: false
                    }
                }
            ],
            queues: [
                {
                    name: queueName,
                    options: {
                        messageTtl: 1000*30,
                        expires: 1000*30,
                        durable: false,
                    }
                }
            ]
        });

        this.publisherStream = mq.createPublisherStream({assertions, signal, channelName, recoveryRetries, highWaterMark: concurrency});
        this.publisherStream.on('error', err => this._onError(err));

        this.consumerStream = mq.createConsumerStream({queueName, assertions, signal, channelName, recoveryRetries, concurrency});

        this.responseProcessorStream = new Writable({
            objectMode: true,
            write: async (msg, enc, cb) => {
                try {
                    msg.ack();
                    cb();

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
                                const err = deserializeError(body.error);
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

                } catch (err) {
                    console.log('response stream err:', err);
                }
            }
        });

        pipeline(
            this.consumerStream,
            this.responseProcessorStream,
            err => {
                if (err) {
                    if (err.code === 'ERR_STREAM_PREMATURE_CLOSE') {
                        // doesn't matter - this is a normal part of closing the consumer/request processor pipeline early
                    } else {
                        this._onError(err);
                    }
                }
                this.close(err);
            }
        );
    }

    _onError(err) {
        if (err.name === 'AbortError') {
            // don't emit abort errors
        } else {
            this.emit('error', err);
        }
        // always close on error
        this.close(err);
    }

    close(err) {
        if (!this.isClosed) {
            this.isClosed = true;
            this.responseProcessorStream.destroy();
            this.publisherStream.destroy();
            this.consumerStream.destroy();
            this.emit('close', err);
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
                    return self.call(routingKey, prop, args);
                };
            }
        });
    }

    bind(queueName) {
        return this.bindEx('', queueName);
    }

    async call(routingKey, method, args = [], options = {}) {
        if (!Array.isArray(args)) {
            throw new TypeError(`args must be an Array`);
        }

        // optional options overrides
        const ackTimeout = options.ackTimeout ?? 1000*5;
        const timeout = options.timeout ?? 1000*30;

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
            const timeoutErr = Error("RPC ACK Timeout");
            const ato = setTimeout(() => {
                call.timeouts.forEach(clearTimeout);
                call.reject(timeoutErr);
            }, ackTimeout);
            call.timeouts.push(ato);
            call.ack = () => {
                clearTimeout(ato);
            };
        }

        if (timeout) {
            // set reply timeout
            const timeoutErr = Error("RPC Response Timeout");
            const rto = setTimeout(() => {
                call.timeouts.forEach(clearTimeout);
                call.reject(timeoutErr);
            }, timeout);
            call.timeouts.push(rto);
        }

        this.publisherStream.write({
            exchangeName: this.rpcExchangeName,
            routingKey,
            content: {
                method,
                args
            },
            options: {
                expiration: ackTimeout,
                replyTo: this.replyId,
                correlationId: id
            }
        });

        return call.settled;
    }
}

module.exports = RPCClient;
