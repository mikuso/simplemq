const EventEmitter = require('eventemitter3');
const amqp = require('amqplib');
const backoff = require('backoff');
const debug = require('debug')('simplemq:connection');

class Connection extends EventEmitter {
    constructor({url}) {
        super();
        this.url = url;

        this.keepAlive = true;
        this.connecting = false;
        this.connected = false;
        this.sendToQueueBuffer = [];

        this.backoff = backoff.exponential({
			randomisationFactor: 0.2,
			initialDelay: 1000,
			maxDelay: 60*1000
		});

        this.backoff.on('backoff', (count, delay) => {
            debug(`Back-off #${count+1}. ${delay} ms`);
        });

        this.backoff.on('ready', () => this._reconnect());
        this._reconnect();
    }

    async shutdown() {
        this.keepAlive = false;
        if (this.connection) {
            console.log('closing connection');
            return this.connection.close().catch(err => {
                debug(`Error closing connection:`, err);
            });
        }
    }

    async sendToQueue(queueName, msg, options) {
        let done;
        const sent = new Promise(r => done = r);
        this.sendToQueueBuffer.push({queueName, msg, options, done});
        this._flushBuffer();
        return sent;
    }

    //
    // Private methods
    //

    async _reconnect() {
        if (this.connecting) {
            return;
        }
        this.connected = false;
        this.connecting = true;
        debug('Reconnecting...');

        try {
            // close any existing connection/channel
            if (this.connection) {
                await this.connection.close();
                this.connection = null;
            }

            this.connection = await amqp.connect(this.url);
            this.connection.on('error', this._onError.bind(this, 'connection'));
            this.connection.on('close', this._onConnectionClose.bind(this));
            this.channel = await this.connection.createChannel();
            this.channel.on('error', this._onError.bind(this, 'channel'));
            this.channel.on('close', this._onChannelClose.bind(this));

            this.connected = true;
            this.backoff.reset();
            this._flushBuffer();
            this.emit('open', this.channel);
            debug(`Connected`);
        } catch (err) {
            if (this.keepAlive) {
                this.backoff.backoff();
            }
        } finally {
            this.connecting = false;
        }
    }

    _onError(subject, err) {
        debug(`${subject} Error: ${err.message}`);
    }

    _onConnectionClose(err) {
        this.connection = null;
        if (this.channel) {
            this.channel.removeAllListeners('close');
            this.channel.close();
            this.channel = null;
        }
        if (this.keepAlive) {
            debug('Connection closed');
            this._reconnect();
        }
    }

    _onChannelClose(err) {
        this.channel = null;
        if (this.connection) {
            this.connection.removeAllListeners('close');
            this.connection.close();
            this.connection = null;
        }
        if (this.keepAlive) {
            debug('Channel closed');
            this._reconnect();
        }
    }

    _flushBuffer() {
        if (this.connected && this.channel) {
            while (this.sendToQueueBuffer.length > 0) {
                const {queueName, msg, options, done} = this.sendToQueueBuffer.shift();
                this.channel.sendToQueue(queueName, msg, options);
                done();
            }
        }
    }

}

module.exports = Connection;
