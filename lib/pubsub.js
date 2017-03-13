/**
 * Created by yeanzhi on 17/3/12.
 */
'use strict';
const PubSub = require('sharedb').PubSub;
const r = require('rethinkdbdash');

module.exports = RethinDbPubSub;
function RethinDbPubSub(options) {
    if (!(this instanceof MemoryPubSub)) return new MemoryPubSub(options);
    options || (options = {});
    PubSub.call(this, options);
    this.r = r(options.rethinkdb);
    this.db = this.r.db(options.db);
}

RethinDbPubSub.prototype = Object.create(PubSub.prototype);

RethinDbPubSub.prototype.close = function(callback) {
    if (!callback) {
        callback = function(err) {
            if (err) throw err;
        };
    }
    var pubsub = this;
    PubSub.prototype.close.call(this, function(err) {
        if (err) return callback(err);
        // pubsub.client.quit(function(err) {
        //     if (err) return callback(err);
        //     pubsub.observer.quit(callback);
        // });
    });
};

RethinDbPubSub.prototype._subscribe = function(channel, callback) {
    if (callback) process.nextTick(callback);
};

RethinDbPubSub.prototype._unsubscribe = function(channel, callback) {
    if (callback) process.nextTick(callback);
};

RethinDbPubSub.prototype._publish = function(channels, data, callback) {
    var pubsub = this;
    process.nextTick(function() {
        for (var i = 0; i < channels.length; i++) {
            var channel = channels[i];
            if (pubsub.subscribed[channel]) {
                pubsub._emit(channel, data);
            }
        }
        if (callback) callback();
    });
};
