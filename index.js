const SimpleMQ = require('./src/simplemq');

module.exports = function(opts) {
    return new SimpleMQ(opts);
};
