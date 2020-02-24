const PubSub = require('./src/pubsub');

module.exports = function(opts) {
    return new PubSub(opts);
};
