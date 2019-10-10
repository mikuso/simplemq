# Description

A library to simplify using amqplib.

Features:
- Reliable (self-repairing) Connections
- RPC Client/Server implementation

Todo:
- Pub/Sub implementation
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

    client.close();
    server.close();
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
  - [client.close()](#clientclose)

### Class: RPCServer

### Class: RPCClient
