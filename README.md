# Description

A library built on the back of amqplib, adding some powerful opinionated features.

Features:
- Reliable (self-repairing) Connections
- RPC Client/Server implementation
- Pub/Sub implementation
- Automatic JSON (de-)serialisation

Todo:
- Write API docs

# Usage

## Publish / Subscribe

```js
const simplemq = require('@flamescape/simplemq');
const url = 'amqp://user:pass@127.0.0.1/';

async function main() {
    const mq = simplemq({url});

    const consumer = await mq.consume('testQueue', {prefetch:1}, msg => {
        console.log(msg.json); // {hello: 'world'}
        msg.ack();
    });

    await mq.sendToQueue('testQueue', {hello:'world'});
}
main();
```

## RPC Server / Client

```js
class Adder {
    sum(a, b) { return a + b; }
}
const simplemq = require('@flamescape/simplemq');
const url = 'amqp://user:pass@127.0.0.1/';

async function main() {
    const mq = simplemq({url});

    const server = await mq.rpcServer('adder', new Adder());
    const client = await mq.rpcClient();

    // either:
    const result1 = await client.call('adder', 'sum', [1, 2]);
    console.log(result1); // 3

    // - or -
    const rpcAdder = client.bind('adder');
    const result2 = await rpcAdder.sum(4, 5);
    console.log(result2); // 9

    // cleanup...
    client.close();
    server.close();

    await mq.close();
}
main();
```

## RPC Fan-out

In standard usage, simplemq's RPC follows the pattern of competing consumers. That is, if you have more than one RPC server bound to the same queue, then they will balance the load between themselves automatically.

Another approach is to create an RPC server around an exchange rather than a queue. Under the hood, the RPC server will create an anonymous queue and bind it to the exchange for you, with all the usual connection keep-alive & recovery magic. If you have multiple RPC servers using the same exchange and routing key, they will each execute any instructions given (which may result in duplicate work being done). The response received by the client is from whichever server is first to complete the work.

To do this, create a `direct` exchange and then follow the example below.

Note: It's important to create a `direct` exchange rather than `fanout` or `topic` exchange.  `fanout` will send calls to all services on the same exchange. `topic` will add special meaning to some characters (`#`, `*`, `.`) in the routingKey which can confuse things.  `direct` will ensure a 1-to-1 mapping between client and server service with arbitrary service names.

```js
class HelloMachine {
    constructor(msg) { this.msg = msg; }
    hello() { return 'Hello ' + this.msg; }
}

const mq = simplemq({url});

// If you want to use this approach, you must create your own 'direct' exchange first.
await mq.assertExchange('rpc-fanout-test', 'direct');

await mq.rpcServer({
    exchange: 'rpc-fanout-test',
    routingKey: 'helloMachine'
}, new HelloMachine('Hello world'));

await mq.rpcServer({
    exchange: 'rpc-fanout-test',
    routingKey: 'helloMachine'
}, new HelloMachine('Hello universe'));

const cli = await mq.rpcClient();

// either ...
await cli.callEx('rpc-fanout-test', 'helloMachine', 'hello', []); // 'Hello World!'

// - or -
const helloMachine = cli.bindEx('rpc-fanout-test', 'helloMachine');
await helloMachine.hello(); // 'Hello World!'
```

## Error handling

simplemq will automatically reconnect to the amqp server after connection loss.  Unfortunately, some things can go wrong when this happens.

For example, create and consume a queue with an auto-expiration of 30 seconds. A 30 second network interruption would destroy your queue. After the connection is restored, simplemq will attempt to restore the consumer on the queue, but this will throw an error and kill the channel. You can respond to this type of error by listening for the `resumeError` event.

```js
const mq = simplemq({url});

mq.on('resumeError', err => {
    // something bad happened
    console.log('Error resuming after connection interruption:', err);
});

await mq.assertQueue('testQueue', {expires: 1000})
await mq.bindQueue('testQueue', 'myExchange', '#');
const consumer = await mq.consume('testQueue', msg => {
    msg.ack();
});
```

If this is likely to be a common problem in your project, simplemq provides an easy way to automatically assert, bind & consume an anonymous queue in a single call. When your connection is restored after a network interruption, simplemq will recreate this state and hopefully things will keep on moving.

```js
const mq = simplemq({url});

const consumer = await mq.consume({
    exchange: 'myExchange'
}, msg => {
    // Magic! a volatile anonymous queue is bound to the myExchange exchange.
    //
    // If the queue expires during a network interruption, all will be recreated
    // when the connection is restored and this callback will continue to work.
    //
    // Keep in mind, messages added to your old auto-expiring queue will be lost
    // - but you would have had that problem anyway.
    msg.ack();
});
```

## Cleaning up

After you're finished using a consumer, call `.cancel()` on it to stop consuming and prevent auto-recovery.

You can close down RPC servers & clients with the `.close()` method.  This will stop them from listening on their call & reply queues respectively.

simplemq can sometimes have an idle amqp connection in the background after recently being used. You can force-close the amqp connection and prevent any further usage on the simplemq instance by calling `.close()`.  Attempting to use the simplemq instance after closing will throw an error (asynchronously, of course).

Closing a simplemq instance without first closing any existing consumers/RPC is bad practice and could result in unexpected behaviour.


# API

`// TODO: write API docs`
