const {setTimeout} = require('timers/promises');
const debug = require('debug')('simplemq:channel-manager');

const DEFAULT = Symbol('DEFAULT');

class ChannelManager {
    constructor() {
        this._channels = new Map();
    }

    async get({channelName, connection, stakeholder}) {
        const key = channelName ?? DEFAULT;

        if (!this._channels.has(key)) {
            const obj = {
                name: key,
                promise: connection.createChannel(),
                stakeholders: new Set(),
                dispose: null,
            };

            obj.dispose = (err) => {
                debug(`Channel disposed:`, key);
                if (this._channels.get(key) === obj) {
                    obj.stakeholders.clear();
                    debug(`Removed all stakeholders from channel:`, obj.name, `(${obj.stakeholders.size} stakeholder(s) total)`);
                    this._channels.delete(key);
                }
            };

            obj.promise.then(chan => {
                debug(`Channel established:`, key);
                chan.setMaxListeners(0);
                chan.once('close', obj.dispose);
                chan.on('error', err => debug(`Channel error:`, err.message));
            }).catch(obj.dispose);
            this._channels.set(key, obj);
        }

        const obj = this._channels.get(key);
        if (stakeholder && !obj.stakeholders.has(stakeholder)) {
            obj.stakeholders.add(stakeholder);
            debug(`Added stakeholder to channel:`, obj.name, `(${obj.stakeholders.size} stakeholder(s) total)`);
        }
        return obj.promise;
    }

    release({channelName, stakeholder}) {
        const key = channelName ?? DEFAULT;

        const obj = this._channels.get(key);
        if (!obj) return;

        setTimeout(1000, obj.promise).then((channel) => {
            obj.stakeholders.delete(stakeholder);
            debug(`Removed stakeholder from channel:`, obj.name, `(${obj.stakeholders.size} stakeholder(s) total)`);
            if (!obj.stakeholders.size) {
                debug(`Closing redundant channel:`, obj.name);
                obj.dispose();
                channel.close().catch(()=>{});
            }
        });
    }
}

module.exports = ChannelManager;
