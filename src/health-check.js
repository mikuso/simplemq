const EventEmitter = require('events');
const uuid = require('uuid');

class HealthCheck extends EventEmitter {
    constructor(mq, {signal} = {}) {
        super();
        this.alive = true;

        this.identity = 'heathcheck.'+uuid.v4();
        this.host = {ping: (...args)=>args};

        this.cli = mq.rpcClient({signal});
        this.srv = mq.rpcServer(this.identity, this.host, {signal, timeout: 1000*2, ackTimeout: 1000*2});

        this.cli.on('error', err => this.emit('error', err));
        this.srv.on('error', err => this.emit('error', err));
        if (signal) {
            signal.addEventListener('abort', () => this.dispose(), {once: true});
        }
    }

    ping(...args) {
        if (!this.alive) return;
        return this.cli.call(this.identity, 'ping', args);
    }

    dispose() {
        this.alive = false;
        this.cli.close();
        this.srv.close();
        this.emit('close');
    }
}

module.exports = HealthCheck;
