const simplemq = require('.');
const url = 'amqp://';

class Adder {
    work(a, b) {
        console.log(`Adder returning ${a}+${b}=${a+b}`);
        return a+b;
    }
}
class Multer {
    work(a, b) {
        console.log(`Multer returning ${a}*${b}=${a*b}`);
        return a*b;
    }
}

async function main() {
    const mq = simplemq({url});

    mq.on('resumeError', async (err) => {
        console.log('mq err:', err);
        await mq.close();
    });


    await mq.assertExchange('m3.rpc', 'topic');

    const s1 = await mq.rpcServer({
        exchange: 'm3.rpc',
        routingKey: 'testrpc'
    }, new Adder());

    const s2 = await mq.rpcServer({
        exchange: 'm3.rpc',
        routingKey: 'testrpc'
    }, new Multer());


    const cli = await mq.rpcClient();
    // const testrpc = cli.bindExchange('m3.rpc', 'testrpc');

    console.log(new Date(), await cli.callExchange('m3.rpc', 'testrpc', 'work', [5,6]));
    console.log(new Date(), await cli.callExchange('m3.rpc', 'testrpc', 'work', [5,6]));
    // console.log(new Date(), await testrpc.work(5,5));
    // cli.close();
    // s1.close();
    // s2.close();
    mq.close();

    // cli.call('testrpc', 'work', [2,2]).catch(err => {
    //     console.log('caught', err);
    // })
    // console.log(new Date(), await cli.call('testrpc', 'work', [2,2]));
    // await testrpc.work(1,2);

}
main().catch(err => {
    console.log('MAIN ERR:', err);
});
