const EventEmitter = require('eventemitter3');
const amqp = require('amqplib');
const backoff = require('backoff');
const debug = require('debug')('simplemq:connection');

class Connection extends EventEmitter {
    constructor({url}) {
        super();
        this.url = url;
    }

    async getConnection({connect = true} = {}) {
        if (!this.connection && connect) {
            this.connection = new Promise((resolve, reject) => {
                debug(`Opening new connection`);

                const boff = backoff.exponential({
                    randomisationFactor: 0.2,
                    initialDelay: 1000,
                    maxDelay: 60*1000
                });

                const connect = async () => {
                    debug(`Attempting connection`);
                    try {
                        const conn = await amqp.connect(this.url);
                        conn.on('error', (err) => {
                            debug(`Connection error:`, err);
                        });
                        conn.once('close', () => {
                            debug(`Connection closed`);
                            this.connection = null;
                            this.channel = null;
                            this.emit('close');
                        });
                        debug(`Connection open`);
                        resolve(conn);
                    } catch (err) {
                        debug(`Connection attempt failed:`, err);
                        boff.backoff();
                    }
                };

                boff.on('backoff', (count, delay) => {
                    debug(`Back-off #${count+1}. ${delay} ms`);
                });

                boff.on('ready', connect);
                connect();

            });
        }

        return this.connection;
    }

    async getChannel() {
        if (!this.channel) {
            this.channel = new Promise(async (resolve, reject) => {
                debug(`Opening new channel`);
                try {
                    const conn = await this.getConnection();
                    const channel = await conn.createChannel();
                    channel.on('error', (err) => {
                        debug(`Channel error:`, err);
                    });
                    channel.once('close', async () => {
                        debug(`Channel closed`);
                        this.channel = null;
                        const conn = await this.getConnection({connect: false});
                        if (conn) {
                            conn.close();
                        }
                    });
                    debug(`Channel open`);
                    this.emit('open', channel);
                    resolve(channel);
                } catch (err) {
                    reject(err);
                }
            });
        }

        return this.channel;
    }

    async shutdown() {
        const conn = await this.getConnection({connect: false});
        if (conn) {
            debug('Closing connection');
            return conn.close().catch(err => {
                debug(`Error closing connection:`, err);
            });
        }
    }

}

module.exports = Connection;
