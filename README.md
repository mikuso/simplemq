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

    const server = mq.rpcServer('adder', new Adder());
    const client = mq.rpcClient();

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
}
main();
```

## Recovery

simplemq will automatically reconnect to the amqp server after connection loss.  Unfortunately, some things can go wrong when this happens.

For example, create and consume a queue with an auto-expiration of 30 seconds. A 30 second network interruption would destroy your queue. After the connection is restored, simplemq will attempt to restore the consumer on the queue, but this will throw an error and kill the channel. You can listen for these errors on the consumer's `error` event.

```js
const mq = simplemq({url});

await mq.assertQueue('testQueue', {expires: 1000})
const consumer = mq.consume('testQueue', msg => {
    msg.ack();
});
consumer.on('error', err => {
    console.error(err);
});
```

If this is likely to be a common problem in your project, simplemq provides an easy way to automatically assert, bind & consume an anonymous queue in a single call. When your connection is restored after a network interruption, simplemq will recreate this state and hopefully things will keep on moving.

```js
const mq = simplemq({url});

const consumer = mq.consume({
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

## Streams

You can create publisher and consumer streams...

```js
mq.createConsumerStream({
    queueName, // name of queue to consume
    assertions, // (optional) ChannelAssertions object
    options, // (optional) consumer options
    signal, // (optional) AbortSignal
    channelName, // (optional) a name to identify the channel this consumer will run on (if not specified, will use the default channel)
    recoveryRetries: 2, // (optional) how many times to re-try recovery upon service failure
    concurrency: 1 // (optional) a.k.a. "prefetch" - the number of unacknowledged messages the consumer will process at once
});

mq.createPublisherStream({
    assertions, // (optional) ChannelAssertions object
    signal, // (optional) AbortSignal
    channelName, // (optional) a name to identify the channel this consumer will run on (if not specified, will use the default channel)
    recoveryRetries: 2, // (optional) how many times to re-try recovery upon service failure
    highWaterMark: 16, // (optional) the number of messages this stream will buffer while waiting for amqplib's `publish()` buffer to drain
    reassertOnReturn: false // (optional) causes assertions to be automatically re-performed if messages are not routed to any queue (also adds `mandatory` flag to every published message)
});
```

## Cleaning up

After you're finished using a consumer, call `.cancel()` on it to stop consuming and prevent auto-recovery.

You can close down RPC servers & clients with the `.close()` method.  This will stop them from listening on their call & reply queues respectively.

simplemq will automatically discard any unused channels and/or connections after they are no longer in use.

# Other considerations

* It's tempting to set `recoveryRetries` to a high number (or Infinity), but this could be a mistake. If a queue/exchange assertion conflicts with an existing one, this will count as a soft fail and will retry indefinitely, but will likely never succeed.
* `channelName`s segregate activities into different channels. If an activity (such as assertion) throws an error, this usually topples to channel too. If you only use a single channel for all activities, then one single error can topple everything. On the other hand, creating a channel for each activity and having high channel churn is considered bad practice. Use sparingly.
* Providing `assertions` to a stream/consumer/publisher will ensure that these are carried out every time a channel or connection fails. Assertions will also occur if a queue is deleted while being consumed.

# API

`// TODO: write API docs`
