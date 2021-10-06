const uuid = require('uuid');
const {pipeline, Writable} = require('stream');
const EventEmitter = require('events');
const {serializeError} = require('serialize-error');
const ChannelAssertions = require('./channel-assertions');
const debug = require('debug')('simplemq:rpcserver');

class RPCServer extends EventEmitter {
    constructor(mq, {identity, host, rpcExchangeName = 'simplemq2.rpc', channelName, signal, recoveryRetries = Infinity, concurrency = 1} = {}) {
        super();
        this.mq = mq;
        this.isClosed = false;

        const queueName = 'simplmq2.requests.' + uuid.v4();

        const assertions = new ChannelAssertions({
            exchanges: [
                {
                    name: rpcExchangeName,
                    type: 'direct',
                    options: {
                        durable: false
                    }
                }
            ],
            queues: [
                {
                    name: queueName,
                    options: {
                        messageTtl: 1000*30,
                        expires: 1000*30,
                        durable: false
                    }
                }
            ],
            queueBindings: [
                {
                    queue: queueName,
                    source: rpcExchangeName,
                    pattern: identity,
                }
            ]
        });

        this.publisherStream = mq.createPublisherStream({assertions, signal, channelName, recoveryRetries, highWaterMark: concurrency});
        this.publisherStream.on('error', this._onError.bind(this));

        this.consumerStream = mq.createConsumerStream({queueName, assertions, signal, channelName, recoveryRetries, concurrency});

        this.requestProcessorStream = new Writable({
            objectMode: true,
            write: async (msg, enc, cb) => {
                try {
                    msg.ack();

                    // acknowledge call
                    this.publisherStream.write({
                        exchangeName: '',
                        routingKey: msg.properties.replyTo,
                        content: {ack:true},
                        options: {correlationId: msg.properties.correlationId},
                    });

                    const body = msg.json;
                    const response = {};

                    this.emit('call', {
                        method: body.method,
                        args: body.args
                    });

                    Promise.resolve(host[body.method](...body.args)).then(result => {
                        response.result = result;
                    }).catch(err => {
                        response.error = serializeError(err);
                    }).finally(() => {
                        this.emit('response', {
                            method: body.method,
                            args: body.args,
                            response: response
                        });

                        // send result
                        this.publisherStream.write({
                            exchangeName: '',
                            routingKey: msg.properties.replyTo,
                            content: response,
                            options: {correlationId: msg.properties.correlationId},
                        });
                    });

                    cb();
                } catch (err) {
                    cb(err);
                }
            }
        });

        pipeline(
            this.consumerStream,
            this.requestProcessorStream,
            err => {
                if (err) {
                    if (err.code === 'ERR_STREAM_PREMATURE_CLOSE') {
                        // doesn't matter - this is a normal part of closing the consumer/request processor pipeline early
                    } else {
                        this._onError(err);
                    }
                }
                this.close(err);
            }
        );
    }

    _onError(err) {
        if (err.name === 'AbortError') {
            // don't emit abort errors
        } else {
            this.emit('error', err);
        }
        // always close on error
        this.close(err);
    }

    close(err) {
        if (!this.isClosed) {
            this.isClosed = true;
            this.requestProcessorStream.destroy();
            this.publisherStream.destroy();
            this.consumerStream.destroy();
            this.emit('close', err);
        }
    }
}

module.exports = RPCServer;
