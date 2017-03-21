/**
 * Created by yeanzhi on 17/3/12.
 */
'use strict';
const async = require('async');
const r = require('rethinkdbdash');
const DB = require('sharedb').DB;
let ShareDBError = require('sharedb').Error;

module.exports = ShareDbRethinkDb;

function ShareDbRethinkDb(options) {
    if (!(this instanceof ShareDbRethinkDb)) return new ShareDbRethinkDb(options);
    this.r = r(options.rethinkdb);
    this.db = options.db;
}
ShareDbRethinkDb.prototype = Object.create(DB.prototype);

ShareDbRethinkDb.prototype.debug = true;
ShareDbRethinkDb.prototype.logger = function (...args) {
    if (this.debug) {
        console.log(...args);
    }
};

ShareDbRethinkDb.prototype.projectsSnapshots = true;
ShareDbRethinkDb.prototype.getCollection = function (collectionName, callback) {
    callback(null, this.r.db(this.db).table(collectionName));
};
ShareDbRethinkDb.prototype.getOpCollection = function (collectionName, callback) {
    callback(null, this.r.db(this.db).table(this.getOplogCollectionName(collectionName)));
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
    this.logger(collectionName, id, op.v, snapshot.v, 'commit');
    this._writeOp(collectionName, id, op, snapshot, function (err, optId) {
        if (err) return callback(err);
        self.logger('_writeOp callback', optId);
        let opId = optId;
        self._writeSnapshot(collectionName, id, snapshot, opId, function (err, succeeded) {
            self.logger('_writeSnapshot callback', err, succeeded);
            if (!!succeeded) return callback(err, !!succeeded);
            // Cleanup unsuccessful op if snapshot write failed. This is not
            // neccessary for data correctness, but it gets rid of clutter
            self._deleteOp(collectionName, opId, function (removeErr) {
                callback(err || removeErr, succeeded);
            });
        });
    });
};
ShareDbRethinkDb.prototype._writeOp = function (collectionName, id, op, snapshot, callback) {
    this.logger(collectionName, id, '_writeOp');
    const self = this;
    if (typeof op.v !== 'number') {
        let err = ShareDbRethinkDb.invalidOpVersionError(collectionName, id, op.v);
        return callback(err);
    }
    let doc = shallowClone(op);
    doc._o = snapshot._o;
    doc.d = id;
    this.logger(collectionName, id, '_writeOp outer');
    this.getOpCollection(collectionName, function (err, collection) {
        collection.insert(doc).run().then(res => {
            self.logger(collectionName, id, '_writeOp after', res, res.generated_keys[0]);
            callback(null, res.generated_keys[0]);
        }).catch(err => callback(err));
    });
};

ShareDbRethinkDb.prototype._deleteOp = function (collectionName, opId, callback) {
    this.getOpCollection(collectionName, function (err, collection) {
        collection.get(opId).delete().then(res => callback(null, res)).catch(err => callback(err));
    });
};

ShareDbRethinkDb.prototype._writeSnapshot = function (collectionName, id, snapshot, opLink, callback) {
    this.logger('_writeSnapshot', id, opLink, snapshot.v);
    const self = this;
    let doc = shallowClone(snapshot);
    doc._o = opLink;

    if (doc.v === 1) {
        this.getCollection(collectionName, function (err, collection) {
            collection.insert(doc).run().then(res => {
                self.logger(collectionName, id, '_writeSnapshot create after', res);
                let succeeded = !!res.replaced || !!res.inserted;
                callback(null, succeeded);
            }).catch(err => callback(err));
        });
    } else {
        this.getCollection(collectionName, function (err, collection) {
            collection.get(doc.id).replace(snapshot).then(res => {
                self.logger(collectionName, id, '_writeSnapshot replace after', res);
                let succeeded = !!res.replaced || !!res.inserted;
                self.logger('------>', succeeded, res);
                callback(null, succeeded);
            }).catch(err => callback(err));
        });
    }
};
// **** Snapshot methods
ShareDbRethinkDb.prototype.getSnapshot = function (collectionName, id, fields, options, callback) {
    this.logger(collectionName, id, fields, options, 'getSnapshot******');
    const self = this;
    this.getCollection(collectionName, function (res, collection) {
        collection.get(id).then(res => {
            self.logger(collectionName, id, res);
            if (!res) {
                callback(null, new RethinkDBSnapshot(id, 0, null, undefined));
            } else {
                callback(null, res);
            }
        }).catch(res => callback(res));
    });
};

ShareDbRethinkDb.prototype.getSnapshotBulk = function (collectionName, ids, fields, options, callback) {
    this.logger('getSnapshotBulk', ids, options);
    this.getCollection(collectionName, function (res, collection) {
        collection.getAll(...ids).then(res => {
            let snapshotMap = res || {};
            for (let i = 0; i < ids.length; i++) {
                let id = ids[i];
                if (snapshotMap[id]) continue;
                snapshotMap[id] = {id, v: 0};
            }
            callback(null, snapshotMap);
        }).catch(res => callback(res));
    });
};


ShareDbRethinkDb.prototype.getOps = function (collectionName, id, from, to, options, callback) {
    this.logger(collectionName, id, from, to, options, 'getOps-------');

    let self = this;
    this._getSnapshotOpLink(collectionName, id, function(err, doc) {
        if (err) return callback(err);
        if (doc) {
            if (isCurrentVersion(doc, from)) {
                return callback(null, []);
            }
            let err = doc && checkDocHasOp(collectionName, id, doc);
            if (err) return callback(err);
        }
        self._getOps(collectionName, id, from, options, function(err, ops) {
            if (err) return callback(err);
            let filtered = filterOps(ops, doc, to);
            err = checkOpsFrom(collectionName, id, filtered, from);
            if (err) return callback(err);
            callback(null, filtered);
        });
    });
};

//get Ops Bulk
ShareDbRethinkDb.prototype.getOpsBulk = function (collectionName, fromMap, toMap, options, callback) {
    this.logger('getOpsBulk', fromMap, toMap);
    let self = this;
    let ids = Object.keys(fromMap);
    this._getSnapshotOpLinkBulk(collectionName, ids, function(err, docs) {
        if (err) return callback(err);
        let docMap = getDocMap(docs);
        // Add empty array for snapshot versions that are up to date and create
        // the query conditions for ops that we need to get
        let conditions = [];
        let conditionObj = [];
        let opsMap = {};
        for (let i = 0; i < ids.length; i++) {
            let id = ids[i];
            let doc = docMap[id];
            let from = fromMap[id];
            if (doc) {
                if (isCurrentVersion(doc, from)) {
                    opsMap[id] = [];
                    continue;
                }
                let err = checkDocHasOp(collectionName, id, doc);
                if (err) return callback(err);
            }
            let condition = getOpsQuery(id, from);
            conditions.push(condition);
        }
        // Return right away if none of the snapshot versions are newer than the
        // requested versions
        if (!conditions.length) return callback(null, opsMap);
        // Otherwise, get all of the ops that are newer
        self._getOpsBulk(collectionName, conditions, options, function(err, opsBulk) {
            if (err) return callback(err);
            for (let i = 0; i < conditions.length; i++) {
                let id = conditions[i].d;
                let ops = opsBulk[id];
                let doc = docMap[id];
                let from = fromMap[id];
                let to = toMap && toMap[id];
                let filtered = filterOps(ops, doc, to);
                let err = checkOpsFrom(collectionName, id, filtered, from);
                if (err) return callback(err);
                opsMap[id] = filtered;
            }
            callback(null, opsMap);
        });
    });
};

ShareDbRethinkDb.prototype.getOpsToSnapshot = function (collectionName, id, from, snapshot, options, callback) {
    this.logger('getOpsToSnapshot', id, from);

    if (snapshot._o == null) {
        let err = ShareDbRethinkDb.missingLastOperationError(collectionName, id);
        return callback(err);
    }
    this._getOps(collectionName, id, from, options, function (err, ops) {
        if (err) return callback(err);
        let filtered = getLinkedOps(ops, null, snapshot._opLink);
        err = checkOpsFrom(collectionName, id, filtered, from);
        if (err) return callback(err);
        callback(null, filtered);
    });
};

ShareDbRethinkDb.prototype.getCommittedOpVersion = function (collectionName, id, snapshot, op, options, callback) {
    this.logger('getCommittedOpVersion', id, op, options);
    const self = this;
    this.getOpCollection(collectionName, function(err, opCollection) {
        if (err) return callback(err);
        let query = {
            src: op.src,
            seq: op.seq
        };
        // Find the earliest version at which the op may have been committed.
        // Since ops are optimistically written prior to writing the snapshot, the
        // op could end up being written multiple times or have been written but
        // not count as committed if not backreferenced from the snapshot
        opCollection.orderBy('v').filter(query).limit(1).run().then(function(err, doc) {
            if (err) return callback(err);
            // If we find no op with the same src and seq, we definitely don't have
            // any match. This should prevent us from accidentally querying a huge
            // history of ops
            if (!doc) return callback();
            // If we do find an op with the same src and seq, we still have to get
            // the ops from the snapshot to figure out if the op was actually
            // committed already, and at what version in case of multiple matches
            let from = doc.v;
            self.getOpsToSnapshot(collectionName, id, from, snapshot, options, function(err, ops) {
                if (err) return callback(err);
                for (let i = ops.length; i--;) {
                    let item = ops[i];
                    if (op.src === item.src && op.seq === item.seq) {
                        return callback(null, item.v);
                    }
                }
                callback();
            });
        });
    });
};


ShareDbRethinkDb.prototype._getOps = function (collectionName, id, from, options, callback) {
    const self = this;
    this.getOpCollection(collectionName, function (err, opCollection) {
        if (err) return callback(err);
        if(!from)from=0;
        opCollection.getAll(id,{index:'d'}).orderBy('v').filter(self.r('v').ge(from)).run().then(res=>{callback(null,res);}).catch(err=>callback(err));
    });
};

//@param conditions = [{id, from}]
ShareDbRethinkDb.prototype._getOpsBulk = function (collectionName, conditions, options, callback) {
    let self = this;
    this.getOpCollection(collectionName, function (err, opCollection) {
        if (err) return callback(err);
        opCollection.filter((function (conditions) {
            let condition;
            for (let item in conditions) {
                let conditionForThisKey = self.r.and(self.r.row('id').eq(item.id),self.r.row('v').ge(item.from));
                if(!condition){
                    condition = conditionForThisKey;
                }else{
                    condition = condition.or(conditionForThisKey);
                }
            }
            return condition;
        })(conditions)).groupBy('d').run().then(res=>callback(null,res)).catch(err=>{callback(err);});
    });
};

ShareDbRethinkDb.prototype._getSnapshotOpLink = function(collectionName, id, callback) {
    this.getCollection(collectionName, function(err, collection) {
        if (err) return callback(err);
        collection.get(id).pluck('_v','_o').run().
        then(res=>callback(null,res)).catch(err=>callback(err));
    });
};

ShareDbRethinkDb.prototype._getSnapshotOpLinkBulk = function(collectionName, ids, callback) {
    this.getCollection(collectionName, function(err, collection) {
        if (err) return callback(err);
        collection.getAll(...ids).pluck('_v','_o').run().
        then(res=>callback(null,res)).catch(err=>callback(err));
    });
};


// query

ShareDbRethinkDb.prototype.query = function (collection, query, fields, options, callback) {
    callback(new ShareDBError(4022, 'query DB method unimplemented'));
};

ShareDbRethinkDb.prototype.queryPoll = function (collection, query, options, callback) {
    let fields = {};
    this.query(collection, query, fields, options, function (err, snapshots, extra) {
        if (err) return callback(err);
        let ids = [];
        for (let i = 0; i < snapshots.length; i++) {
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
function getOpsQuery(id, from) {
    return (from == null) ?
        {d: id,v:0} :
        {d: id, v: from};
}
function checkOpsFrom(collectionName, id, ops, from) {
    if (ops.length === 0) return;
    if (ops[0] && ops[0].v === from) return;
    if (from == null) return;
    return ShareDbRethinkDb.missingOpsError(collectionName, id, from);
}

function checkDocHasOp(collectionName, id, doc) {
    if (doc._o) return;
    return ShareDbRethinkDb.missingLastOperationError(collectionName, id);
}

function isCurrentVersion(doc, version) {
    return doc._v === version;
}

function filterOps(ops, doc, to) {
    // Always return in the case of no ops found whether or not consistent with
    // the snapshot
    if (!ops) return [];
    if (!ops.length) return ops;
    if (!doc) {
        // There is no snapshot currently. We already returned if there are no
        // ops, so this could happen if:
        //   1. The doc was deleted
        //   2. The doc create op is written but not the doc snapshot
        //   3. Same as 3 for a recreate
        //   4. We are in an inconsistent state because of an error
        //
        // We treat the snapshot as the canonical version, so if the snapshot
        // doesn't exist, the doc should be considered deleted. Thus, a delete op
        // should be in the last version if no commits are inflight or second to
        // last version if commit(s) are inflight. Rather than trying to detect
        // ops inconsistent with a deleted state, we are simply returning ops from
        // the last delete. Inconsistent states will ultimately cause write
        // failures on attempt to commit.
        //
        // Different delete ops must be identical and must link back to the same
        // prior version in order to be inserted, so if there are multiple delete
        // ops at the same version, we can grab any of them for this method.
        // However, the _id of the delete op might not ultimately match the delete
        // op that gets maintained if two are written as a result of two
        // simultanous delete commits. Thus, the _id of the op should *not* be
        // assumed to be consistent in the future.
        let deleteOp = getLatestDeleteOp(ops);
        // Don't return any ops if we don't find a delete operation, which is the
        // correct thing to do if the doc was just created and the op has been
        // written but not the snapshot. Note that this will simply return no ops
        // if there are ops but the snapshot doesn't exist.
        if (!deleteOp) return [];
        return getLinkedOps(ops, to, deleteOp._id);
    }
    return getLinkedOps(ops, to, doc._o);
}

function getLatestDeleteOp(ops) {
    for (let i = ops.length; i--;) {
        let op = ops[i];
        if (op.del) return op;
    }
}

function getLinkedOps(ops, to, link) {
    let linkedOps = [];
    for (let i = ops.length; i-- && link;) {
        let op = ops[i];
        if (link.equals ? !link.equals(op._id) : link !== op._id) continue;
        link = op.o;
        if (to == null || op.v < to) {
            delete op._id;
            delete op.o;
            linkedOps.unshift(op);
        }
    }
    return linkedOps;
}

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
    let out = {};
    for (let key in object) {
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

function getDocMap(docs) {
    let docMap = {};
    for (let i = 0; i < docs.length; i++) {
        let doc = docs[i];
        docMap[doc._id] = doc;
    }
    return docMap;
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
