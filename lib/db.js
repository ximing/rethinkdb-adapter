/**
 * Created by yeanzhi on 17/3/12.
 */
'use strict';
const async = require('async');
const r = require('rethinkdbdash');
const DB = require('sharedb').DB;
var ShareDBError = require('sharedb').Error;

module.exports = ShareDbRethinkDb;

function ShareDbRethinkDb(options) {
    if (!(this instanceof ShareDbRethinkDb)) return new ShareDbRethinkDb(options);
    DB.call(this, options);
    this.r = r(options.rethinkdb);
    this.db = this.r.db(options.db);
}
ShareDbRethinkDb.prototype = Object.create(DB.prototype);

ShareDbRethinkDb.prototype.debug = true;
ShareDbRethinkDb.prototype.logger = function (...args) {
    if (this.debug) {
        console.log(...args)
    }
};

ShareDbRethinkDb.prototype.projectsSnapshots = true;
ShareDbRethinkDb.prototype.getCollection = function (collectionName) {
    return this.db.table(collectionName);
};
ShareDbRethinkDb.prototype.getOpCollection = function (collectionName) {
    return this.db.table(this.getOplogCollectionName(collectionName));
};

//TODO imp
ShareDbRethinkDb.prototype.close = function (callback) {
    if (!callback) {
        callback = function (err) {
            if (err) throw err;
        };
    }
    return callback();
};

// 提交snapshot 和 op
ShareDbRethinkDb.prototype.commit = function (collectionName, id, op, snapshot, options, callback) {
    const self = this;
    this.logger(collectionName, id, op, snapshot, 'ss')
    this._writeOp(collectionName, id, op, snapshot, function (err, optId) {
        if (err) return callback(err);
        var opId = optId;
        self._writeSnapshot(collectionName, id, snapshot, opId, function (err, succeeded) {
            if (succeeded) return callback(err, succeeded);
            // Cleanup unsuccessful op if snapshot write failed. This is not
            // neccessary for data correctness, but it gets rid of clutter
            self._deleteOp(collectionName, opId, function (removeErr) {
                callback(err || removeErr, succeeded);
            });
        });
    });
};
ShareDbRethinkDb.prototype._writeOp = function (collectionName, id, op, snapshot, callback) {
    if (typeof op.v !== 'number') {
        var err = ShareDbRethinkDb.invalidOpVersionError(collectionName, id, op.v);
        return callback(err);
    }
    var doc = shallowClone(op);
    doc._o = snapshot._o;
    doc.d = id;
    this.db.table(this.getOplogCollectionName(collectionName)).insert(doc).run().then(res => {
        callback(null, res.generated_keys);
    }).catch(err => callback(err));
};

ShareDbRethinkDb.prototype._deleteOp = function (collectionName, opId, callback) {
    this.db.table(this.getOplogCollectionName(collectionName))
        .get(opId).delete().then().catch(err => callback(err));
};

ShareDbRethinkDb.prototype._writeSnapshot = function (collectionName, id, snapshot, opLink, callback) {
    var doc = shallowClone(snapshot);
    doc._o = opLink;

    if (doc.v === 1) {
        this.db.table(collectionName).insert(doc).run().then(res => {
            callback(null, res.generated_keys);
        }).catch(err => callback(err));
    } else {
        this.db.table(collectionName).get(doc.id).replace(snapshot).then(res => {
            if (res || (Array.isArray(res) && res.length === 0)) {
                var succeeded = !!res.replaced;
                callback(null, succeeded);
            }
        }).catch(err => callback(err));
    }
};
// **** Snapshot methods
ShareDbRethinkDb.prototype.getSnapshot = function (collectionName, id, fields, options, callback) {
    this.logger(collectionName, id, fields, options, 'getSnapshot')
    this.db.table(collectionName).get(id).then(res => {
        console.log(collectionName, id, res);
        if (!res) {
            callback(null, new RethinkDBSnapshot(id, 0, null, undefined));
        } else {
            callback(null, res);
        }
    }).catch(callback);
};

ShareDbRethinkDb.prototype.getSnapshotBulk = function (collectionName, ids, fields, options, callback) {
    this.db.table(collectionName).getAll(ids).then(res => {
        let snapshotMap = res || {};
        for (let i = 0; i < ids.length; i++) {
            let id = ids[i];
            if (snapshotMap[id]) continue;
            snapshotMap[id] = {id, v: 0};
        }
        callback(null, snapshotMap);
    }).catch(callback);
};


ShareDbRethinkDb.prototype.getOps = function (collectionName, id, from, to, options, callback) {
    this.logger(collectionName, id, from, to, options, 'getOps')
    if (Number.isInteger(from) && Number.isInteger(to)) {
        this.db.table(this.getOplogCollectionName(collectionName)).between(from, to, {index: 'v'}).filter({d: id}).run().then(res => {
            console.log('getOps', res);
            callback(null, res)
        }).catch(callback);
    } else if (Number.isInteger(from)) {
        this.db.table(this.getOplogCollectionName(collectionName)).filter(this.r.and(this.r("d").eq(id),this.r("v").ge(from))).run().then(res => {
            console.log('getOps', res);
            callback(null, res)
        }).catch(callback);
    } else if (Number.isInteger(to)) {
        this.db.table(this.getOplogCollectionName(collectionName)).filter(this.r.and(this.r("d").eq(id),this.r("v").lt(to))).run().then(res => {
            console.log('getOps', res);
            callback(null, res)
        }).catch(callback);
    } else {
        this.db.table(this.getOplogCollectionName(collectionName)).filter({d: id}).run().then(res => {
            console.log('getOps', res);
            callback(null, res)
        }).catch(callback);
    }
};

ShareDbRethinkDb.prototype.getOpsBulk = function (collectionName, fromMap, toMap, options, callback) {
    var results = {};
    var db = this;
    async.forEachOf(fromMap, function (from, id, eachCb) {
        var to = toMap && toMap[id];
        db.getOps(collectionName, id, from, to, options, function (err, ops) {
            if (err) return eachCb(err);
            results[id] = ops;
            eachCb();
        });
    }, function (err) {
        if (err) return callback(err);
        callback(null, results);
    });
};

ShareDbRethinkDb.prototype.getOpsToSnapshot = function (collectionName, id, from, snapshot, options, callback) {
    if (snapshot._o == null) {
        let err = ShareDbRethinkDb.missingLastOperationError(collectionName, id);
        return callback(err);
    }
    let to = snapshot.v;
    this.getOps(collectionName, id, from, to, options, callback);
};

ShareDbRethinkDb.prototype.getCommittedOpVersion = function (collection, id, snapshot, op, options, callback) {
    this.getOpsToSnapshot(collection, id, 0, snapshot, options, function (err, ops) {
        if (err) return callback(err);
        for (var i = ops.length; i--;) {
            var item = ops[i];
            if (op.src === item.src && op.seq === item.seq) {
                return callback(null, item.v);
            }
        }
        callback();
    });
};


// query

ShareDbRethinkDb.prototype.query = function (collection, query, fields, options, callback) {
    callback(new ShareDBError(4022, 'query DB method unimplemented'));
};

ShareDbRethinkDb.prototype.queryPoll = function (collection, query, options, callback) {
    var fields = {};
    this.query(collection, query, fields, options, function (err, snapshots, extra) {
        if (err) return callback(err);
        var ids = [];
        for (var i = 0; i < snapshots.length; i++) {
            ids.push(snapshots[i].id);
        }
        callback(null, ids, extra);
    });
};

ShareDbRethinkDb.prototype.queryPollDoc = function (collection, id, query, options, callback) {
    callback(new ShareDBError(5014, 'queryPollDoc DB method unimplemented'));
};

ShareDbRethinkDb.prototype.canPollDoc = function () {
    return false;
};

ShareDbRethinkDb.prototype.skipPoll = function () {
    return false;
};


// **** Oplog methods

// Overwrite me if you want to change this behaviour.
ShareDbRethinkDb.prototype.getOplogCollectionName = function (collectionName) {
    return 'o_' + collectionName;
};

ShareDbRethinkDb.prototype.validateCollectionName = function (collectionName) {
    if (
        collectionName === 'system' || (
            collectionName[0] === 'o' &&
            collectionName[1] === '_'
        )
    ) {
        return ShareDbRethinkDb.invalidCollectionError(collectionName);
    }
};

function isObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function shallowClone(object) {
    var out = {};
    for (var key in object) {
        out[key] = object[key];
    }
    return out;
}

function isPlainObject(value) {
    return (
        typeof value === 'object' && (
            Object.getPrototypeOf(value) === Object.prototype ||
            Object.getPrototypeOf(value) === null
        )
    );
}

function RethinkDBSnapshot(id, version, type, data, meta, opLink) {
    this.id = id;
    this.v = version;
    this.type = type;
    this.data = data;
    this.u = 0;
    if (meta) this.m = meta;
    if (opLink) this._o = opLink;
}


// Bad request errors
ShareDbRethinkDb.invalidOpVersionError = function (collectionName, id, v) {
    return {
        code: 4101,
        message: 'Invalid op version ' + collectionName + '.' + id + ' ' + v
    };
};
ShareDbRethinkDb.invalidCollectionError = function (collectionName) {
    return {code: 4102, message: 'Invalid collection name ' + collectionName};
};
ShareDbRethinkDb.$whereDisabledError = function () {
    return {code: 4103, message: '$where queries disabled'};
};
ShareDbRethinkDb.$mapReduceDisabledError = function () {
    return {code: 4104, message: '$mapReduce queries disabled'};
};
ShareDbRethinkDb.$aggregateDisabledError = function () {
    return {code: 4105, message: '$aggregate queries disabled'};
};
ShareDbRethinkDb.$queryDeprecatedError = function () {
    return {code: 4106, message: '$query property deprecated in queries'};
};
ShareDbRethinkDb.malformedQueryOperatorError = function (operator) {
    return {code: 4107, message: 'Malformed query operator: ' + operator};
};
ShareDbRethinkDb.onlyOneCollectionOperationError = function (operation1, operation2) {
    return {
        code: 4108,
        message: 'Only one collection operation allowed. ' +
        'Found ' + operation1 + ' and ' + operation2
    };
};
ShareDbRethinkDb.onlyOneCursorOperationError = function (operation1, operation2) {
    return {
        code: 4109,
        message: 'Only one cursor operation allowed. ' +
        'Found ' + operation1 + ' and ' + operation2
    };
};
ShareDbRethinkDb.cursorAndCollectionMethodError = function (collectionOperation) {
    return {
        code: 4110,
        message: 'Cursor methods can\'t run after collection method ' +
        collectionOperation
    };
};

// Internal errors
ShareDbRethinkDb.alreadyClosedError = function () {
    return {code: 5101, message: 'Already closed'};
};
ShareDbRethinkDb.missingLastOperationError = function (collectionName, id) {
    return {
        code: 5102,
        message: 'Snapshot missing last operation field "_o" ' + collectionName + '.' + id
    };
};
ShareDbRethinkDb.missingOpsError = function (collectionName, id, from) {
    return {
        code: 5103,
        message: 'Missing ops from requested version ' + collectionName + '.' + id + ' ' + from
    };
};
// Modifies 'err' argument
ShareDbRethinkDb.parseQueryError = function (err) {
    err.code = 5104;
    return err;
};
