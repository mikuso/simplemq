const uuid = require('uuid');
const Connection = require('./connection');
const debug = require('debug')('simplemq:rpcclient');

class RPCClient {
    constructor({url}) {
        this.url = url;
        this.keepAlive = true;
        this.calls = new Map();

        let ready;
        this._isListening = new Promise(r => { ready = r; });

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

            ready();
        });

        this.amqp.on('close', async () => {
            this._isListening = new Promise(r => { ready = r; });

            if (this.keepAlive) {
                this.amqp.getChannel().catch((err) => {
                    debug(`Error reopening channel:`, err);
                });
            }
        });

        this.init().catch((err)=>{
            debug(`Failed to get init:`, err);
        });
    }

    async init() {
        await this.amqp.getChannel();
        await this._isListening;
    }

    close() {
        this.keepAlive = false;
        this.amqp.shutdown();
    }

    async call(queueName, method, args = [], options = {}) {
        if (!this.keepAlive) {
            throw Error(`Connection closed`);
        }
        // don't call until receive channel is set up
        await this._isListening;

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

        if (!this.keepAlive) {
            return call.reject(Error(`Connection closed`));
        }

        const channel = await this.amqp.getChannel();
        try {
            await channel.sendToQueue(queueName, Buffer.from(JSON.stringify({
                method,
                args
            })), {
                expiration: timeout,
                replyTo: this.replyQueueName,
                correlationId: id
            });
        } catch (err) {
            call.reject(err);
        }

        return call.done;
    }
}

module.exports = RPCClient;
