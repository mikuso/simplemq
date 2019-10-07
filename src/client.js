const uuid = require('uuid');
const Connection = require('./connection');
const debug = require('debug')('simplemq:rpcclient');

class RPCClient {
    constructor({url}) {
        this.url = url;
        this.calls = new Map();
    }

    async open() {
        return new Promise((resolve, reject) => {
            if (this.amqp) {
                return;
            }

            this.amqp = new Connection({
                url: this.url
            });

            this.amqp.on('open', async (channel) => {
                const replyQueue = await channel.assertQueue(null, { messageTtl: 1000*30, expires: 1000*30 });
                this.replyQueueName = replyQueue.queue;

                await channel.consume(replyQueue.queue, (msg) => {
                    if (!msg) {
                        return;
                    }
                    channel.ack(msg);
                    const call = this.calls.get(msg.properties.correlationId);
                    if (!call) {
                        debug(`Ignoring response for unknown call ${msg.properties.correlationId}`);
                        return;
                    }
                    this.calls.delete(msg.properties.correlationId);

                    try {
                        const body = JSON.parse(msg.content.toString('utf8'));
                        if (body.error) {
                            const err = Error(body.error.message);
                            err.stack = body.error.stack;
                            err.code = body.error.code;
                            err.details = body.error.details;
                            return call.reject(err);
                        } else {
                            return call.resolve(body.result);
                        }
                    } catch (err) {
                        return call.reject(err);
                    }
                });

                resolve();
            });
        });
    }

    close() {
        this.amqp && this.amqp.shutdown();
    }

    async call(queueName, method, args = [], options = {}) {
        const timeout = options.timeout || 1000*10;

        const id = uuid.v4();

        const call = {};
        call.done = new Promise((res, rej) => {
            call.resolve = res;
            call.reject = rej;
        });
        this.calls.set(id, call);

        if (timeout) {
            // set reply timeout
            setTimeout(() => {
                call.reject(Error("RPC Timeout"));
            }, timeout);
        }

        this.amqp.sendToQueue(queueName, Buffer.from(JSON.stringify({
            method,
            args
        })), {
            expiration: timeout,
            replyTo: this.replyQueueName,
            correlationId: id
        }).catch(()=>{});

        return call.done;
    }
}

module.exports = RPCClient;
