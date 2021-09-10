const uuid = require('uuid');
const {setMaxListeners, EventEmitter} = require('events');
const {Writable, Readable} = require('stream');
const {setTimeout} = require('timers/promises');
const eventStapler = require('eventstapler');
const debug = require('debug')('simplemq:mq');
const RPCServer = require('./rpcserver');
const RPCClient = require('./rpcclient');
const ChannelAssertions = require('./channel-assertions');
const ConnectionManager = require('./connection-manager');
const ChannelManager = require('./channel-manager');
const HealthCheck = require('./health-check');

class SimpleMQ extends EventEmitter {
    constructor({url} = {}) {
        super();
        this.connections = new ConnectionManager(url);
        this.channels = new ChannelManager();
    }

    async withChannel(channelName, asyncFunc) {
        if (typeof channelName === 'function') {
            asyncFunc = channelName;
            channelName = undefined;
        }

        const stakeholder = Symbol("disposable");
        const connection = await this.connections.get({stakeholder});
        const channel = await this.channels.get({channelName, connection, stakeholder});
        let result;

        try {
            result = await asyncFunc(channel);
        } finally {
            this.channels.release({channelName, stakeholder});
            this.connections.release({stakeholder});
        }
        return result;
    }

    async assertQueue(queue, options) {
        return this.withChannel(channel => {
            debug('assertQueue(', queue, ',', options, ')');
            return channel.assertQueue(queue, options);
        });
    }

    async checkQueue(queue) {
        return this.withChannel(channel => {
            debug('checkQueue(', queue, ')');
            return channel.checkQueue(queue);
        });
    }

    async deleteQueue(queue, options) {
        return this.withChannel(channel => {
            debug('deleteQueue(', queue, ',', options, ')');
            return channel.deleteQueue(queue, options);
        });
    }

    async purgeQueue(queue) {
        return this.withChannel(channel => {
            debug('purgeQueue(', queue, ')');
            return channel.purgeQueue(queue);
        });
    }

    async bindQueue(queue, source, pattern, args) {
        return this.withChannel(channel => {
            debug('bindQueue(', queue, ',', source, ',', pattern, ',', args, ')');
            return channel.bindQueue(queue, source, pattern, args);
        });
    }

    async unbindQueue(queue, source, pattern, args) {
        return this.withChannel(channel => {
            debug('unbindQueue(', queue, ',', source, ',', pattern, ',', args, ')');
            return channel.unbindQueue(queue, source, pattern, args);
        });
    }

    async assertExchange(exchange, type, options) {
        return this.withChannel(channel => {
            debug('assertExchange(', exchange, ',', type, ',', options, ')');
            return channel.assertExchange(exchange, type, options);
        });
    }

    async checkExchange(exchange) {
        return this.withChannel(channel => {
            debug('checkExchange(', exchange, ')');
            return channel.checkExchange(exchange);
        });
    }

    async deleteExchange(exchange, options) {
        return this.withChannel(channel => {
            debug('deleteExchange(', exchange, ',', options, ')');
            return channel.deleteExchange(exchange, options);
        });
    }

    async bindExchange(destination, source, pattern, args) {
        return this.withChannel(channel => {
            debug('bindExchange(', destination, ',', source, ',', pattern, ',', args, ')');
            return channel.bindExchange(destination, source, pattern, args);
        });
    }

    async unbindExchange(destination, source, pattern, args) {
        return this.withChannel(channel => {
            debug('unbindExchange(', destination, ',', source, ',', pattern, ',', args, ')');
            return channel.unbindExchange(destination, source, pattern, args);
        });
    }

    consume(queueName, consumerOptions = {}, callback) {
        if (typeof consumerOptions === 'function') {
            callback = consumerOptions;
            consumerOptions = {};
        }
        const concurrency = consumerOptions.prefetch ?? consumerOptions.concurrency ?? 1;
        const recoveryRetries = consumerOptions.recoveryRetries ?? 2;
        const assertions = consumerOptions.assertions ?? {};

        if (typeof queueName !== 'string' && queueName.exchange) {
            const exchangeName = queueName.exchange;
            const routingKey = queueName.routingKey ?? '#';
            queueName = exchangeName + '.consumer.' + uuid.v4();

            assertions.queues ??= [];
            assertions.queues.push({
                name: queueName,
                options: {
                    durable: false,
                    expires: 1000*30,
                }
            });

            assertions.queueBindings ??= [];
            assertions.queueBindings.push({
                queue: queueName,
                source: exchangeName,
                pattern: routingKey,
            });
        }

        const stream = this.createConsumerStream({
            queueName,
            assertions,
            options: consumerOptions,
            recoveryRetries,
            concurrency
        });

        const ee = new EventEmitter();
        stream.on('close', () => ee.emit('close'));
        stream.on('error', (err) => ee.emit('error', err));
        stream.on('data', callback);
        ee.cancel = () => stream.destroy();
        return ee;
    }

    async publish(exchange, routingKey, content, options = {}) {
        if (!(content instanceof Buffer)) {
            try {
                content = Buffer.from(JSON.stringify(content));
                options.contentType = 'application/json';
            } catch (err) {}
        }
        if (!options.timestamp) {
            options.timestamp = Date.now();
        }

        return this.withChannel(channel => {
            debug('publish(', exchange, ',', routingKey, ',', '<- omitted ->', ',', options, ')');
            return channel.publish(exchange, routingKey, content, options);
        });
    }

    async sendToQueue(queue, content, options) {
        return await this.publish('', queue, content, options);
    }

    async prefetch(count, global = false) {
        return this.withChannel(channel => {
            debug('prefetch(', count, ',', global, ')');
            return channel.prefetch(count, global = false);
        });
    }

    async recover() {
        return this.withChannel(channel => {
            debug('recover()');
            return channel.recover();
        });
    }

    async ackAll() {
        return this.withChannel(channel => {
            debug('ackAll()');
            return channel.ackAll();
        });
    }

    async nackAll(requeue = undefined) {
        return this.withChannel(channel => {
            debug('nackAll(', requeue, ')');
            return channel.nackAll(requeue);
        });
    }

    createHealthCheck({signal} = {}) {
        return new HealthCheck(this, {signal});
    }

    rpcServer(identity, host, options = {}) {
        return new RPCServer(this, {
            identity,
            host,
            ...options,
        });
    }

    rpcClient(options) {
        return new RPCClient(this, options);
    }

    definePublisher({channelName, assertions, releaseTimeout = 1000*2} = {}) {
        if (!(assertions instanceof ChannelAssertions)) {
            assertions = new ChannelAssertions(assertions);
        }

        let connection;
        let channel;
        let isDisposed = false;

        const stakeholder = Symbol("publisher");
        const usableChannels = new WeakSet();

        const recover = async ({signal}) => {
            // recover connection
            connection = await this.connections.get({stakeholder}); // this could be an existing connection
            if ((signal && signal.aborted) || isDisposed) throw new AbortError("Aborted");

            // recover channel
            channel = await this.channels.get({connection, channelName, stakeholder}); // this could be an existing channel
            if ((signal && signal.aborted) || isDisposed) throw new AbortError("Aborted");

            if (!usableChannels.has(channel)) {
                usableChannels.add(channel);

                // perform assertions
                await assertions.assertTo(channel);
            }
        };

        return {
            async write({exchangeName, routingKey, content, options, signal}) {

                // ensure content is correctly formed
                if (!options) options = {};

                if (!(content instanceof Buffer)) {
                    try {
                        content = Buffer.from(JSON.stringify(content));
                        options.contentType = 'application/json';
                    } catch (err) {}
                }

                if (!options.timestamp) {
                    options.timestamp = Date.now();
                }

                // recover (if necessary)
                await recover({signal});

                const published = channel.publish(exchangeName, routingKey, content, options);

                if (!published) {
                    debug(`Congestion in publisher stream to: ${exchangeName}`);
                    await new Promise((resolve, reject) => {
                        eventStapler(channel)
                            .once('drain', resolve)
                            .once('close', () => reject(Error("Channel closed")))
                            .releaseAfter('drain')
                            .releaseAfter('close');
                    });
                }
            },
            dispose: async () => {
                isDisposed = true;
                // Connection/channel may or may not exist at this point.
                // (Don't worry - there's no consequence if we release when already released)
                this.channels.release({connection, channelName, stakeholder});
                this.connections.release({stakeholder});
            }
        };
    }

    defineConsumer({channelName, assertions, queueName, consumerOptions, concurrency, releaseTimeout = 1000*2} = {}) {
        if (!(assertions instanceof ChannelAssertions)) {
            assertions = new ChannelAssertions(assertions);
        }

        let connection;
        let channel;
        let consumer;

        const stakeholder = Symbol("consumer");
        const usableChannels = new WeakSet();

        const buff = {
            queue: [],
            events: new EventEmitter(),
        };

        const recover = async ({signal}) => {
            // recover connection
            connection = await this.connections.get({stakeholder}); // this could be an existing connection

            // recover channel
            channel = await this.channels.get({connection, channelName, stakeholder}); // this could be an existing channel
            if (signal && signal.aborted) throw new AbortError("Aborted");

            if (!usableChannels.has(channel)) {
                usableChannels.add(channel);

                const onClose = () => {
                    buff.events.emit('cancel', Error("Channel closed"));
                    channel = null;
                };

                try {
                    channel.once('close', onClose);

                    // perform assertions
                    await assertions.assertTo(channel);

                    // recover consumer
                    if (concurrency !== undefined) {
                        await channel.prefetch(concurrency);
                    }
                    consumer = await channel.consume(queueName, msg => {
                        if (!msg) {
                            // queue deleted. cancel consumer & remove channel from usableChannels?
                            // recovery should automatically re-run, along with creating consumer
                            usableChannels.delete(channel);
                            channel.cancel(consumer.consumerTag).catch(() => {});
                            buff.events.emit('cancel', Error("Queue deleted"));
                        } else {
                            buff.queue.push(msg);
                            buff.events.emit('readable', buff.queue.length);
                        }
                    }, consumerOptions);
                } catch (err) {
                    channel && channel.off('close', onClose);
                    usableChannels.delete(channel);
                    throw err;
                }
            }
        };

        return {
            async read({signal}) {
                // recover (if necessary)
                await recover({signal});

                let data;
                // pull the message from the front of the queue
                if (buff.queue.length) {
                    data = buff.queue.shift();
                } else {

                    // otherwise, wait for readable event
                    await new Promise((resolve, reject) => {
                        eventStapler(buff.events)
                            .once('readable', resolve)
                            .once('cancel', err => reject(err))
                            .releaseAfter('readable')
                            .releaseAfter('cancel');
                    });
                    data = buff.queue.shift();
                }


                const msg = {};
                msg.ack = (allUpTo = false) => {
                    channel.ack(data, allUpTo);
                };
                msg.nack = (allUpTo = false, requeue = true) => {
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

                return msg;
            },
            dispose: async () => {
                if (consumer && channel) {
                    await channel.cancel(consumer.consumerTag).catch(()=>{});
                }

                // Connection/channel may or may not exist at this point.
                // (Don't worry - there's no consequence if we release when already released)
                this.channels.release({connection, channelName, stakeholder});
                this.connections.release({stakeholder});
            }
        };
    }

    /**
     * Create a durable Writable stream which accepts objects suited for publish()
     * @param  {object} assertions   Assertions to perform after channel opened but before allowing writes
     * @param  {AbortSignal} signal  Signal to abort the stream
     * @param  {string|Symbol} channelName  Key for this channel in the cache. Using the same key as another stream will cause the stream to share the same channel
     * @param  {number} recoveryRetries [default=2] The number of times to try re-try recovering a dead channel after the channel fails. (0 = Will try recovery once, but no retries.)
     * @return {Writable}            Writable stream
     */
    createPublisherStream({assertions, signal, channelName, recoveryRetries = 2, highWaterMark = 16}) {
        const publisher = this.definePublisher({channelName, assertions});
        if (signal) {
            setMaxListeners(0, signal);
        }

        let isDestroyed = false;

        const stream = new Writable({
            signal,
            objectMode: true,
            highWaterMark,
            async write({exchangeName, routingKey, content, options}, encoding, callback) {
                try {

                    for (let attempt = 0;; attempt++) {
                        if (isDestroyed) break;
                        try {
                            await publisher.write({exchangeName, routingKey, content, options, signal}); // only returns after publish() returns true or drained
                            break;
                        } catch (err) {
                            if (err.name === 'AbortError' || attempt >= recoveryRetries) {
                                throw err;
                            }
                            await setTimeout(1000);
                        }
                    }

                    callback();
                } catch (err) {
                    callback(err);
                }
            },
            destroy(err, callback) {
                isDestroyed = true;
                publisher.dispose();
                callback(err);
            }
        });

        return stream;
    }

    /**
     * Create a Readable stream which consumes messages from a queue (messages must still be (n)acked manually)
     * @param  {string} queueName    Name of the queue to consume
     * @param  {object} assertions   Assertions to perform after channel opened but before allowing reads
     * @param  {object} options      Consumer options (http://www.squaremobius.net/amqp.node/channel_api.html#channel_consume)
     * @param  {AbortSignal} signal  Signal to abort the stream
     * @param  {string|Symbol} channelName  Key for this channel in the cache. Using the same key as another stream will cause the stream to share the same channel
     * @param  {number} recoveryRetries [default=2] The number of times to try re-try recovering a dead channel after the channel fails. (0 = Will try recovery once, but no retries.)
     * @param  {number} concurrency [default=1] Set the highWaterMark and channel prefetch limits for this stream.
     * @return {Readable}            Readable stream
     */
    createConsumerStream({queueName, assertions, options, signal, channelName, recoveryRetries = 2, concurrency = 1}) {
        const consumer = this.defineConsumer({channelName, assertions, queueName, consumerOptions: options, concurrency});
        if (signal) {
            setMaxListeners(0, signal);
        }

        let isReading = false;
        let isDestroyed = false;

        const stream = new Readable({
            signal,
            objectMode: true,
            highWaterMark: (concurrency > 0 ? concurrency : 1),
            async read(size) {
                if (isReading) {
                    return;
                }
                isReading = true;

                try {
                    let congested = false;
                    while (!congested) {
                        for (let attempt = 0;; attempt++) {
                            if (isDestroyed) return;
                            try {
                                const msg = await consumer.read({signal});
                                congested = !this.push(msg);
                                break;
                            } catch (err) {
                                if (err.name === 'AbortError' || attempt >= recoveryRetries) {
                                    throw err;
                                }
                                await setTimeout(1000);
                            }
                        }
                    }
                } catch (err) {
                    this.destroy(err);
                } finally {
                    isReading = false;
                }
            },
            destroy(err, callback) {
                isDestroyed = true;
                consumer.dispose();
                callback(err);
            }
        });

        return stream;
    }


}

module.exports = SimpleMQ;
