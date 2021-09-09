const debug = require('debug')('simplemq:channel-assertions');

class ChannelAssertions {
    constructor(props = {}) {
        this.assertedTo = new WeakMap();

        this.queues = [];
        this.exchanges = [];
        this.queueBindings = [];
        this.exchangeBindings = [];

        if ('queues' in props) {
            props.queues.map(queue => this.addQueue(queue));
        }
        if ('exchanges' in props) {
            props.exchanges.map(exchange => this.addExchange(exchange));
        }
        if ('queueBindings' in props) {
            props.queueBindings.map(queueBinding => this.addQueueBinding(queueBinding));
        }
        if ('exchangeBindings' in props) {
            props.exchangeBindings.map(exchangeBinding => this.addExchangeBinding(exchangeBinding));
        }
    }

    addQueue(queue) {
        if (!queue.name) throw Error("Bad assertion: Missing queue.name");
        this.queues.push(queue);
    }

    addExchange(exchange) {
        if (!exchange.name) throw Error("Bad assertion: Missing exchange.name");
        if (!exchange.type) throw Error("Bad assertion: Missing exchange.type");
        this.exchanges.push(exchange);
    }

    addQueueBinding(binding) {
        if (!binding.queue) throw Error("Bad assertion: Missing binding.queue");
        if (!binding.source) throw Error("Bad assertion: Missing binding.source");
        if (!binding.pattern) throw Error("Bad assertion: Missing binding.pattern");
        this.queueBindings.push(binding);
    }

    addExchangeBinding(binding) {
        if (!binding.destination) throw Error("Bad assertion: Missing binding.destination");
        if (!binding.source) throw Error("Bad assertion: Missing binding.source");
        if (!binding.pattern) throw Error("Bad assertion: Missing binding.pattern");
        this.exchangeBindings.push(binding);
    }

    async performAssertions(channel) {
        for (const queue of this.queues) {
            debug('Asserting queue', queue);
            await channel.assertQueue(queue.name, queue.options);
        }
        for (const exchange of this.exchanges) {
            debug('Asserting exchange', exchange);
            await channel.assertExchange(exchange.name, exchange.type, exchange.options);
        }
        for (const binding of this.queueBindings) {
            debug('Asserting bind', binding);
            await channel.bindQueue(binding.queue, binding.source, binding.pattern, binding.args);
        }
        for (const binding of this.exchangeBindings) {
            debug('Asserting bind', binding);
            await channel.bindExchange(binding.destination, binding.source, binding.pattern, binding.args);
        }
    }

    async assertTo(channel) {
        if (!this.assertedTo.has(channel)) {
            const performing = this.performAssertions(channel).catch(err => {
                if (this.assertedTo.get(channel) === performing) {
                    this.assertedTo.delete(channel);
                }
                throw err;
            });
            this.assertedTo.set(channel, performing);
        }
        return await this.assertedTo.get(channel);
    }
}

module.exports = ChannelAssertions;
