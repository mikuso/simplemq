const Connection = require('./connection');
const debug = require('debug')('simplemq:rpcserver');
const EventEmitter = require('eventemitter3');

class RPCServer extends EventEmitter {
    constructor(pubsub) {
        super();
        this.pubsub = pubsub;
        this.consumer = null;
    }

    async init({queueName, host, options}) {
        if (this.consumer) {
            throw Error(`Consumer already initiated`);
        }

        await this.pubsub.assertQueue(queueName, {
            messageTtl: options.queueMessageTtl || 1000*30,
            expires: options.queueExpires || 1000*30
        });

        this.consumer = await this.pubsub.consume(queueName, async (msg) => {
            try {
                if (!msg) { return; }
                msg.ack();

                // acknowledge call
                this.pubsub.sendToQueue(
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
                this.pubsub.publish(
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
