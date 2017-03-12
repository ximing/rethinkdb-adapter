/**
 * Created by yeanzhi on 17/3/12.
 */
'use strict';
const PubSub = require('sharedb').PubSub;
const r = require('rethinkdbdash');

module.exports = RethinDbPubSub;
function RethinDbPubSub(options) {
    if (!(this instanceof MemoryPubSub)) return new MemoryPubSub(options);
    PubSub.call(this, options);
}
module.exports = RethinDbPubSub;
