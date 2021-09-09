const amqp = require('amqplib');
const {setTimeout} = require('timers/promises');
const debug = require('debug')('simplemq:connection-manager');

const DEFAULT = Symbol('DEFAULT');

class ConnectionManager {
    constructor(url) {
        this._connections = new Map();
        this._url = url;
    }

    async get({connectionName, stakeholder, signal}) {
        const key = connectionName ?? DEFAULT;

        if (!this._connections.has(key)) {
            const obj = {
                name: key,
                promise: amqp.connect(this._url, {signal}),
                stakeholders: new Set(),
                dispose: null,
            };

            obj.dispose = (err) => {
                debug(`Connection disposed:`, key);
                if (this._connections.get(key) === obj) {
                    obj.stakeholders.clear();
                    debug(`Removed all stakeholders from connection:`, obj.name, `(${obj.stakeholders.size} stakeholder(s) total)`);
                    this._connections.delete(key);
                }
            };

            obj.promise.then(conn => {
                debug(`Connection established:`, key);
                conn.once('close', obj.dispose);
                conn.on('error', err => debug(`Connection error:`, err.message));
            }).catch(obj.dispose);
            this._connections.set(key, obj);
        }

        const obj = this._connections.get(key);
        if (stakeholder && !obj.stakeholders.has(stakeholder)) {
            obj.stakeholders.add(stakeholder);
            debug(`Added stakeholder to connection:`, obj.name, `(${obj.stakeholders.size} stakeholder(s) total)`);
        }
        return obj.promise;
    }

    release({connectionName, stakeholder}) {
        const key = connectionName ?? DEFAULT;

        const obj = this._connections.get(key);
        if (!obj) return;

        setTimeout(1000, obj.promise).then((connection) => {
            obj.stakeholders.delete(stakeholder);
            debug(`Removed stakeholder from connection:`, obj.name, `(${obj.stakeholders.size} stakeholder(s) total)`);
            if (!obj.stakeholders.size) {
                debug(`Closing redundant connection:`, obj.name);
                obj.dispose();
                return connection.close();
            }
        });
    }
}

module.exports = ConnectionManager;
