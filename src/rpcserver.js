const Connection = require('./connection');
const debug = require('debug')('simplemq:rpcserver');
const EventEmitter = require('eventemitter3');

class RPCServer extends EventEmitter {
    constructor(mq) {
        super();
        this.mq = mq;
        this.consumer = null;
    }

    async init({queueName, host, options}) {
        if (this.consumer) {
            throw Error(`Consumer already initiated`);
        }

        if (typeof queueName === 'string') {
            await this.mq.assertQueue(queueName, {
                messageTtl: options.queueMessageTtl || 1000*30,
                expires: options.queueExpires || 1000*30
            });
        }

        this.consumer = await this.mq.consume(queueName, async (msg) => {
            try {
                if (!msg) { return; }
                msg.ack();

                // acknowledge call
                this.mq.publish(
                    'simplemq.rpc',
                    msg.properties.replyTo,
                    Buffer.from(JSON.stringify({ack:true})),
                    {correlationId: msg.properties.correlationId}
                ).catch(()=>{}); // ignore failure sending acknowledgement

                const body = msg.json;
                const response = {};
                try {
                    this.emit('call', {
                        method: body.method,
                        args: body.args
                    });

                    response.result = await host[body.method](...body.args);
                } catch (err) {
                    response.error = {
                        message: err.message,
                        code: err.code,
                        stack: err.stack,
                        details: err.details
                    };
                }

                this.emit('response', {
                    method: body.method,
                    args: body.args,
                    response: response
                });

                // send result
                this.mq.publish(
                    'simplemq.rpc',
                    msg.properties.replyTo,
                    response,
                    {correlationId: msg.properties.correlationId}
                );
            } catch (err) {
                debug(`Error consuming RPC message:`, err.message);
            }
        });
    }

    close() {
        if (this.consumer) {
            this.consumer.cancel();
        }
        this.removeAllListeners();
    }
}

module.exports = RPCServer;
