const AMQPConnection = require('./connection');
const debug = require('debug')('simplemq:rpcserver');

class RPCServer {
    constructor({url}) {
        this.wrapped = [];

        this.amqp = new AMQPConnection({url});
        this.amqp.on('open', async (channel) => {
            this.channel = channel;
            await this._startConsumers().catch(err => {
                debug(`Error starting consumers:`, err.message);
            });
        });
    }

    async wrap(queueName, host) {
        const wrapped = {queueName, host, consumerTag: null, cancel: null};
        this.wrapped.push(wrapped);
        if (this.amqp && this.amqp.connected) {
            await this._startConsumer(wrapped).catch(err => {
                debug(`Error starting consumer:`, err.message);
            });
        }
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

    async _startConsumers() {
        return Promise.all(this.wrapped.map(wrapped => {
            return this._startConsumer(wrapped);
        }));
    }

    async _startConsumer(wrapped) {
        debug(`Starting consumer for ${wrapped.queueName}`);
        const channel = this.channel;

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

                await this.amqp.sendToQueue(
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
