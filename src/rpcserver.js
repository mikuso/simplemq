const Connection = require('./connection');
const debug = require('debug')('simplemq:rpcserver');

class RPCServer {
    constructor({url}) {
        this.wrapped = [];
        this.keepAlive = true;

        this.amqp = new Connection({url});

        this.amqp.on('open', async (channel) => {
            await this._startConsumers(channel).catch(err => {
                debug(`Error starting consumers:`, err.message);
            });
        });

        this.amqp.on('close', async () => {
            if (this.keepAlive) {
                this.amqp.getChannel().catch((err) => {
                    debug(`Error reopening channel:`, err);
                });
            }
        });

        this.amqp.getChannel().catch((err)=>{
            debug(`Failed to get channel:`, err);
        });
    }

    async wrap(queueName, host) {
        const wrapped = {queueName, host, consumerTag: null, cancel: null};
        this.wrapped.push(wrapped);
        const channel = await this.amqp.getChannel();
        await this._startConsumer(channel, wrapped).catch(err => {
            debug(`Error starting consumer:`, err.message);
        });
    }

    async unwrap(queueName, host) {
        const wrapped = this.wrapped.find(w => w.queueName === queueName && host === host);
        if (wrapped) {
            const idx = this.wrapped.indexOf(wrapped);
            if (idx !== -1) { this.wrapped.splice(idx, 1); }
            if (wrapped.cancel && wrapped.consumerTag) {
                await wrapped.cancel().catch(err => {
                    console.log(`Error cancelleing wrap`, err);
                });
            }
        }
    }

    async unwrapAll() {
        return Promise.all([...this.wrapped].map(wrapped => {
            return this.unwrap(wrapped.queueName, wrapped.host);
        }));
    }

    close() {
        this.unwrapAll();
        this.amqp.shutdown();
    }

    //
    // Private methods
    //

    async _startConsumers(channel) {
        return Promise.all(this.wrapped.map(wrapped => {
            return this._startConsumer(channel, wrapped);
        }));
    }

    async _startConsumer(channel, wrapped) {
        debug(`Starting consumer for ${wrapped.queueName}`);

        await channel.assertQueue(wrapped.queueName, { messageTtl: 1000*30, expires: 1000*30 });
        const consumer = await channel.consume(wrapped.queueName, async (msg) => {
            try {
                if (!msg) { return; }

                const body = JSON.parse(msg.content.toString('utf8'));
                const response = {};
                try {
                    response.result = await wrapped.host[body.method](...body.args);
                } catch (err) {
                    response.error = {
                        message: err.message,
                        code: err.code,
                        stack: err.stack,
                        details: err.details
                    };
                }

                channel.sendToQueue(
                    msg.properties.replyTo,
                    Buffer.from(JSON.stringify(response)),
                    {correlationId: msg.properties.correlationId}
                );

                await channel.ack(msg);
            } catch (err) {
                debug(`Error consuming message`, err.message);
            }
        });

        wrapped.consumerTag = consumer.consumerTag;

        wrapped.cancel = () => {
            return channel.cancel(wrapped.consumerTag).catch(err => {
                debug(`Error cancelling consumer:`, err.message);
            });
        };
    }
}

module.exports = RPCServer;
