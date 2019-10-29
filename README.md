# Description

A library to simplify using amqplib.

Features:
- Reliable (self-repairing) Connections
- RPC Client/Server implementation
- Pub/Sub implementation
- Automatic JSON (de-)serialisation

Todo:
- Write API docs

# Usage

## RPC Server / Client

```js
class Adder {
    sum(a, b) { return a + b; }
}
const {RPCServer, RPCClient} = require('@flamescape/simplemq');
const url = 'amqp://user:pass@127.0.0.1/';

async function main() {
    const server = new RPCServer({url});
    server.wrap('rpcQueueName', new Adder());

    const client = new RPCClient({url})
    const result = await client.call('rpcQueueName', 'sum', [1, 2]);
    console.log(result); // 3

    // - or -
    const adder = client.bind('rpcQueueName');
    const result2 = await adder.sum(1, 2);
    console.log(result2); // 3

    client.close();
    server.close();
}
main();
```

## PubSub

```js
const {PubSub} = require('@flamescape/simplemq');
const url = 'amqp://user:pass@127.0.0.1/';

async function main() {
    const pubsub = new PubSub({url});

    const consumer = await pubsub.consume('testQueue', msg => {
        console.log(msg.json); // {hello: 'world'}
        msg.ack();
    });

    await pubsub.publish('', 'testQueue', {hello:'world'});
}
main();
```

# API

- [Class: simplemq.RPCServer](#class-rpcserver)
  - [new simplemq.RPCServer(options)](#new-simplemqrpcserveroptions)
  - [server.wrap(queueName, host)](#serverwrap)
  - [server.unwrap(queueName, host)](#serverunwrap)
  - [server.unwrapAll()](#serverunwrapAll)
  - [server.close()](#serverclose)
- [Class: simplemq.RPCClient](#class-rpcclient)
  - [new simplemq.RPCClient(options)](#new-simplemqrpcclientoptions)
  - [client.call(queueName, method[, args][, options])](#clientcall)
  - [client.bind(queueName)](#clientbind)
  - [client.close()](#clientclose)
- [Class: simplemq.PubSub](#class-pubsub)
  - [new simplemq.PubSub(options)](#new-simplemqpubsuboptions)
  - [pubsub.consume(queueName, callback)](#pubsubconsume)
  - [pubsub.publish(exchange, routingKey, content[, options])](#pubsubpublish)
  - [pubsub.close()](#[pubsubclose)

### Class: RPCServer

### Class: RPCClient

### Class: PubSub
