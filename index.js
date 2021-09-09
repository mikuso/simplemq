const SimpleMQ = require('./src/simplemq');
const ChannelAssertions = require('./src/channel-assertions');

const factory = function(opts) {
    return new SimpleMQ(opts);
};

factory.ChannelAssertions = ChannelAssertions;

module.exports = factory;
