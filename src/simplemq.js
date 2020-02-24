const Connection = require('./connection');
const debug = require('debug')('simplemq:mq');
const EventEmitter = require('eventemitter3');
const RPCServer = require('./rpcserver');
const RPCClient = require('./rpcclient');

class SimpleMQ extends EventEmitter {
    constructor({url} = {}) {
        super();
        this.url = url;
        this.keepAlive = true;
        this.consumers = [];

        this.connection = new Connection({
            url: this.url
        });

        this.connection.on('open', async () => {
            this._resumeConsumers();
        });

        this.connection.on('close', async () => {
            this.consumers.forEach(c=>{c.alive = false;});
            if (this.keepAlive) {
                this.connection.getChannel().catch((err) => {
                    debug(`Error reopening channel:`, err);
                });
            }
        });
    }

    close() {
        this.keepAlive = false;
        return this.connection.shutdown();
    }

    async consume(queueName, options, callback) {
        if (callback === undefined && options instanceof Function) {
            callback = options;
            options = {};
        }

        options = options || {};

        const consumer = {
            queueName,
            callback,
            alive: false,
            options
        };

        await this._resumeConsumer(consumer);
        this.consumers.push(consumer);
        return consumer;
    }

    async assertQueue(queue, options) {
        const channel = await this.connection.getChannel();
        return channel.assertQueue(queue, options);
    }

    async checkQueue(queue) {
        const channel = await this.connection.getChannel();
        return channel.checkQueue(queue);
    }

    async deleteQueue(queue, options) {
        const channel = await this.connection.getChannel();
        return channel.deleteQueue(queue, options);
    }

    async purgeQueue(queue) {
        const channel = await this.connection.getChannel();
        return channel.purgeQueue(queue);
    }

    async bindQueue(queue, source, pattern, args) {
        const channel = await this.connection.getChannel();
        return channel.bindQueue(queue, source, pattern, args);
    }

    async unbindQueue(queue, source, pattern, args) {
        const channel = await this.connection.getChannel();
        return channel.unbindQueue(queue, source, pattern, args);
    }

    async assertExchange(exchange, type, options) {
        const channel = await this.connection.getChannel();
        return await channel.assertExchange(exchange, type, options);
    }

    async checkExchange(exchange) {
        const channel = await this.connection.getChannel();
        return channel.checkExchange(exchange);
    }

    async deleteExchange(exchange, options) {
        const channel = await this.connection.getChannel();
        return channel.deleteExchange(exchange, options);
    }

    async bindExchange(destination, source, pattern, args) {
        const channel = await this.connection.getChannel();
        return channel.bindExchange(destination, source, pattern, args);
    }

    async unbindExchange(destination, source, pattern, args) {
        const channel = await this.connection.getChannel();
        return channel.unbindExchange(destination, source, pattern, args);
    }

    async publish(exchange, routingKey, content, options = {}) {
        const channel = await this.connection.getChannel();
        if (!(content instanceof Buffer)) {
            try {
                content = Buffer.from(JSON.stringify(content));
                options.contentType = 'application/json';
            } catch (err) {}
        }
        if (!options.timestamp) {
            options.timestamp = Date.now();
        }
        return channel.publish(exchange, routingKey, content, options);
    }

    async sendToQueue(queue, content, options) {
        return await this.publish('', queue, content, options);
    }

    async prefetch(count, global = false) {
        const channel = await this.getChannel();
        return channel.prefetch(count, global = false);
    }

    async recover() {
        const channel = await this.getChannel();
        return channel.recover();
    }

    async ackAll() {
        const channel = await this.getChannel();
        return channel.ackAll();
    }

    async nackAll(requeue = undefined) {
        const channel = await this.getChannel();
        return channel.nackAll(requeue);
    }

    async rpcServer(queueName, host, options = {}) {
        await this.assertExchange('simplemq.rpc', 'topic');
        const server = new RPCServer(this);
        await server.init({
            queueName,
            host,
            options
        });
        return server;
    }

    async rpcClient(options) {
        await this.assertExchange('simplemq.rpc', 'topic');
        const client = new RPCClient(this, options);
        await client.init();
        return client;
    }


    //
    // Private methods
    //

    async _resumeConsumer(consumer) {
        if (consumer.alive) {
            return;
        }
        consumer.alive = true;

        const channel = await this.connection.getChannel();

        if (consumer.options && consumer.options.prefetch) {
            channel.prefetch(consumer.options.prefetch);
        }

        let queueName = consumer.queueName;

        // If an object was provided for a queue name, it is assumed that a temporary queue is required.
        // This could be useful if a connection is interrupted for too long, as temporary queues could expire.
        // This feature will create a new temporary queue upon resuming the consumer, so we're not listening on nothing.
        // Admittedly, any messages held in any previous temporary queue will be lost.
        if (typeof queueName === 'object') {
            let {expires, exchange, routingKey} = consumer.queueName;
            if (expires === undefined) {
                expires = 1000*30;
            }
            if (routingKey === undefined) {
                routingKey = '#';
            }
            const queueOptions = {};
            if (expires) queueOptions.expires = expires;
            // Create a temporary queue and bind it to the exchange
            const queue = await channel.assertQueue(null, queueOptions);
            await channel.bindQueue(queue.queue, exchange, routingKey);
            queueName = queue.queue;
        }

        let cons, ctag;
        try {
            cons = await channel.consume(queueName, async (data) => {
                const msg = {};
                let resolved = false;
                msg.ack = (allUpTo = false) => {
                    resolved = true;
                    channel.ack(data, allUpTo);
                };
                msg.nack = (allUpTo = false, requeue = true) => {
                    resolved = true;
                    channel.nack(data, allUpTo, requeue);
                };
                msg.properties = data.properties;
                msg.fields = data.fields;
                msg.body = data.content;
                if (data.properties.contentType === 'application/json') {
                    try {
                        msg.json = JSON.parse(msg.body.toString('utf8'));
                    } catch (err) {}
                }
                try {
                    await consumer.callback(msg);
                } catch (err) {
                    if (!resolved) {
                        debug(`Message nack'd due to callback error:`, err);
                        msg.nack();
                    } else {
                        debug(`Swallowed error after ack/nack:`, err);
                    }
                }
            });

            ctag = cons.consumerTag;
        } catch (err) {
            // Error resuming consumer; perhaps the queue no longer exists?
            // Stop auto-subscribing the consumer and emit an error on the mq object.
            this._unregisterConsumer(consumer);
            this.emit('resumeError', err);
        }

        consumer.cancel = () => {
            this._unregisterConsumer(consumer);
            return channel.cancel(ctag).catch(()=>{});
        };
    }

    _unregisterConsumer(consumer) {
        const idx = this.consumers.indexOf(consumer);
        if (idx !== -1) { this.consumers.splice(idx, 1); }
    }

    async _resumeConsumers() {
        return Promise.all(this.consumers.map(c => this._resumeConsumer(c)));
    }

}

module.exports = SimpleMQ;
