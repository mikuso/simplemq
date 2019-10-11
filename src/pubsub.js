const Connection = require('./connection');
const debug = require('debug')('simplemq:pubsub');

class PubSub {
    constructor({url}) {
        this.url = url;
        this.keepAlive = true;
        this.consumers = [];

        this.amqp = new Connection({
            url: this.url
        });

        this.amqp.on('open', async () => {
            this._resumeConsumers();
        });

        this.amqp.on('close', async () => {
            this.consumers.forEach(c=>{c.alive = false;});
            if (this.keepAlive) {
                this.amqp.getChannel().catch((err) => {
                    debug(`Error reopening channel:`, err);
                });
            }
        });

        this.amqp.getChannel().catch((err)=>{
            debug(`Failed to get init:`, err);
        });
    }

    async close() {
        this.keepAlive = false;
        await Promise.all(this.consumers.slice(0).map(c => c.cancel()));
        this.amqp.shutdown();
    }

    async consume(queueName, callback) {
        const consumer = {
            queueName,
            callback,
            alive: false
        };

        this.consumers.push(consumer);
        await this._resumeConsumer(consumer);
        return consumer;
    }

    async assertQueue(queue, options) {
        const channel = await this.amqp.getChannel();
        return channel.assertQueue(queue, options);
    }

    async checkQueue(queue) {
        const channel = await this.amqp.getChannel();
        return channel.checkQueue(queue);
    }

    async deleteQueue(queue, options) {
        const channel = await this.amqp.getChannel();
        return channel.deleteQueue(queue, options);
    }

    async purgeQueue(queue) {
        const channel = await this.amqp.getChannel();
        return channel.purgeQueue(queue);
    }

    async bindQueue(queue, source, pattern, args) {
        const channel = await this.amqp.getChannel();
        return channel.bindQueue(queue, source, pattern, args);
    }

    async unbindQueue(queue, source, pattern, args) {
        const channel = await this.amqp.getChannel();
        return channel.unbindQueue(queue, source, pattern, args);
    }

    async assertExchange(exchange, type, options) {
        const channel = await this.amqp.getChannel();
        return await channel.assertExchange(exchange, type, options);
    }

    async checkExchange(exchange) {
        const channel = await this.amqp.getChannel();
        return channel.checkExchange(exchange);
    }

    async deleteExchange(exchange, options) {
        const channel = await this.amqp.getChannel();
        return channel.deleteExchange(exchange, options);
    }

    async bindExchange(destination, source, pattern, args) {
        const channel = await this.amqp.getChannel();
        return channel.bindExchange(destination, source, pattern, args);
    }

    async unbindExchange(destination, source, pattern, args) {
        const channel = await this.amqp.getChannel();
        return channel.unbindExchange(destination, source, pattern, args);
    }

    async publish(exchange, routingKey, content, options = {}) {
        const channel = await this.amqp.getChannel();
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
        return this.publish('', queue, content, options);
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



    //
    // Private methods
    //

    async _resumeConsumer(consumer) {
        if (consumer.alive) {
            return;
        }
        consumer.alive = true;

        const channel = await this.amqp.getChannel();

        const cons = await channel.consume(consumer.queueName, async (data) => {
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
        const ctag = cons.consumerTag;

        consumer.cancel = () => {
            const idx = this.consumers.indexOf(consumer);
            if (idx !== -1) { this.consumers.splice(idx, 1); }
            return channel.cancel(ctag).catch(()=>{});
        };
    }

    async _resumeConsumers() {
        return Promise.all(this.consumers.map(c => this._resumeConsumer(c)));
    }

}

module.exports = PubSub;
