/**
 * Created by yeanzhi on 17/3/12.
 */
'use strict';
const async = require('async');
const r = require('rethinkdbdash');
const DB = require('sharedb').DB;
module.exports = ShareDbRethinkDb;

function ShareDbRethinkDb(options) {
    this.r = r(options.rethinkdb);
    this.db = this.r.db(options.db);
}
ShareDbRethinkDb.prototype = Object.create(DB.prototype);
ShareDbRethinkDb.prototype.projectsSnapshots = true;
ShareDbRethinkDb.prototype.getCollection = function(collectionName,callback) {
    const err = this.validateCollectionName(collectionName);
    if (err) return callback(err);
    return this.db.table(collectionName);
}
//TODO imp
ShareDbRethinkDb.prototype.close = function(callback) {
    if (callback) callback();
};
ShareDbRethinkDb.prototype.getOpsCollectionName = function (collectionName) {
    return collectionName;
}
ShareDbRethinkDb.prototype.getSnapshotCollectionName = function (collectionName) {
    return `${collectionName}_ops`;
}
// 提交snapshot 和 op
ShareDbRethinkDb.prototype.commit = function(collectionName, id, op, snapshot, options, callback) {
    const self = this;
    this._writeOp(collectionName, id, op, snapshot, function(err, optId) {
        if (err) return callback(err);
        var opId = optId;
        self._writeSnapshot(collectionName, id, snapshot, opId, function(err, succeeded) {
            if (succeeded) return callback(err, succeeded);
            // Cleanup unsuccessful op if snapshot write failed. This is not
            // neccessary for data correctness, but it gets rid of clutter
            self._deleteOp(collectionName, opId, function(removeErr) {
                callback(err || removeErr, succeeded);
            });
        });
    })
};
ShareDbRethinkDb.prototype._writeOp = function(collectionName, id, op, snapshot, callback) {
    if (typeof op.v !== 'number') {
        var err = ShareDbRethinkDb.invalidOpVersionError(collectionName, id, op.v);
        return callback(err);
    }
    var doc = shallowClone(op);
    doc._o = snapshot._o;
    this.db.table(this.getOpsCollectionName(collectionName)).insert(doc).run().then(res=>{
        callback(null,res.generated_keys);
    }).catch(err=>callback(err))
};

ShareDbRethinkDb.prototype._deleteOp = function(collectionName, opId, callback) {
    this.db.table(this.getOpsCollectionName(collectionName))
        .get(opId).delete().then().catch(err=>callback(err));
};

ShareDbRethinkDb.prototype._writeSnapshot = function(collectionName, id, snapshot, opLink, callback) {
    var doc = shallowClone(snapshot);
    doc._o = opLink;

    if(doc.v === 1){
        this.db.table(this.getSnapshotCollectionName(collectionName)).insert(doc).run().then(res=>{
            callback(null, res.generated_keys);
        }).catch(err=>callback(err))
    }else{
        this.db.table(this.getSnapshotCollectionName(collectionName)).get(doc.id).replace(snapshot).then(res=>{
            if(res || (Array.isArray(res)&&res.length===0)){
                var succeeded = !!result.replaced;
                callback(null, succeeded);
            }
        }).catch(err=>callback(err))
    }
};
// **** Snapshot methods
ShareDbRethinkDb.prototype.getSnapshot = function(collectionName, id, fields, options, callback) {
    this.db.table(this.getSnapshotCollectionName(collectionName)).get(id).then(res=>{
        if(res){
            callback(null,{id,v:0});
        }else{
            callback(null,res);
        }
    }).catch(callback);
};

ShareDbRethinkDb.prototype.getSnapshotBulk = function(collectionName, ids, fields, options, callback) {
    this.db.table(this.getSnapshotCollectionName(collectionName)).getAll(ids).then(res=>{
        let snapshotMap = res;
        for (let i = 0; i < ids.length; i++) {
            let id = ids[i];
            if (snapshotMap[id]) continue;
            snapshotMap[id] = {id,v:0};
        }
        callback(null, snapshotMap);
    }).catch(callback);
};


ShareDbRethinkDb.prototype.getOps = function(collectionName, id, from, to, options, callback) {
    this.db.table(this.getOpsCollectionName(collectionName)).filter(function (opt) {
        opt("docId").eq(id).and(opt("v").ge(from)).and(opt("v").lt(to))
    }).run().then(res=>callback(null,res)).catch(callback);
};

ShareDbRethinkDb.prototype.getOpsBulk = function(collectionName, fromMap, toMap, options, callback) {
    var results = {};
    var db = this;
    async.forEachOf(fromMap, function(from, id, eachCb) {
        var to = toMap && toMap[id];
        db.getOps(collectionName, id, from, to, options, function(err, ops) {
            if (err) return eachCb(err);
            results[id] = ops;
            eachCb();
        });
    }, function(err) {
        if (err) return callback(err);
        callback(null, results);
    });
};

ShareDbRethinkDb.prototype.getOpsToSnapshot = function(collectionName, id, from, snapshot, options, callback) {
    if (snapshot._o == null) {
        let err = ShareDbRethinkDb.missingLastOperationError(collectionName, id);
        return callback(err);
    }
    let to = snapshot.v;
    this.getOps(collectionName, id, from, to, options, callback);
};

ShareDbRethinkDb.prototype.getCommittedOpVersion = function(collection, id, snapshot, op, options, callback) {
    this.getOpsToSnapshot(collection, id, 0, snapshot, options, function(err, ops) {
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

ShareDbRethinkDb.prototype.query = function(collection, query, fields, options, callback) {
    callback(new ShareDBError(4022, 'query DB method unimplemented'));
};

ShareDbRethinkDb.prototype.queryPoll = function(collection, query, options, callback) {
    var fields = {};
    this.query(collection, query, fields, options, function(err, snapshots, extra) {
        if (err) return callback(err);
        var ids = [];
        for (var i = 0; i < snapshots.length; i++) {
            ids.push(snapshots[i].id);
        }
        callback(null, ids, extra);
    });
};

ShareDbRethinkDb.prototype.queryPollDoc = function(collection, id, query, options, callback) {
    callback(new ShareDBError(5014, 'queryPollDoc DB method unimplemented'));
};

ShareDbRethinkDb.prototype.canPollDoc = function() {
    return false;
};

ShareDbRethinkDb.prototype.skipPoll = function() {
    return false;
};


// **** Oplog methods

// Overwrite me if you want to change this behaviour.
ShareDbRethinkDb.prototype.getOplogCollectionName = function(collectionName) {
    return 'xm_' + collectionName;
};

ShareDbRethinkDb.prototype.validateCollectionName = function(collectionName) {
    if (
        collectionName === 'system' || (
            collectionName[0] === 'x' &&
            collectionName[1] === 'm' &&
            collectionName[2] === '_'
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

// Convert a simple map of fields that we want into a mongo projection. This
// depends on the data being stored at the top level of the document. It will
// only work properly for json documents--which are the only types for which
// we really want projections.
function getProjection(fields, options) {
    // When there is no projection specified, still exclude returning the
    // metadata that is added to a doc for querying or auditing
    if (!fields) {
        return (options && options.metadata) ? {_o: 0} : {_m: 0, _o: 0};
    }
    // Do not project when called by ShareDB submit
    if (fields.$submit) return;

    var projection = {};
    for (var key in fields) {
        projection[key] = 1;
    }
    projection._type = 1;
    projection._v = 1;
    if (options && options.metadata) projection._m = 1;
    return projection;
}

var collectionOperationsMap = {
    '$distinct': function(collection, query, value, cb) {
        collection.distinct(value.field, query, cb);
    },
    '$aggregate': function(collection, query, value, cb) {
        collection.aggregate(value, cb);
    },
    '$mapReduce': function(collection, query, value, cb) {
        if (typeof value !== 'object') {
            var err = ShareDbMongo.malformedQueryOperatorError('$mapReduce');
            return cb(err);
        }
        var mapReduceOptions = {
            query: query,
            out: {inline: 1},
            scope: value.scope || {}
        };
        collection.mapReduce(
            value.map, value.reduce, mapReduceOptions, cb);
    }
};

var cursorOperationsMap = {
    '$count': function(cursor, value, cb) {
        cursor.count(cb);
    },
    '$explain': function(cursor, verbosity, cb) {
        cursor.explain(verbosity, cb);
    },
    '$map': function(cursor, fn, cb) {
        cursor.map(fn, cb);
    }
};

var cursorTransformsMap = {
    '$batchSize': function(cursor, size) { return cursor.batchSize(size); },
    '$comment': function(cursor, text) { return cursor.comment(text); },
    '$hint': function(cursor, index) { return cursor.hint(index); },
    '$max': function(cursor, value) { return cursor.max(value); },
    '$maxScan': function(cursor, value) { return cursor.maxScan(value); },
    '$maxTimeMS': function(cursor, milliseconds) {
        return cursor.maxTimeMS(milliseconds);
    },
    '$min': function(cursor, value) { return cursor.min(value); },
    '$noCursorTimeout': function(cursor) {
        // no argument to cursor method
        return cursor.noCursorTimeout();
    },
    '$orderby': function(cursor, value) {
        console.warn('Deprecated: $orderby; Use $sort.');
        return cursor.sort(value);
    },
    '$readConcern': function(cursor, level) {
        return cursor.readConcern(level);
    },
    '$readPref': function(cursor, value) {
        // The Mongo driver cursor method takes two argments. Our queries
        // have a single value for the '$readPref' property. Interpret as
        // an object with {mode, tagSet}.
        if (typeof value !== 'object') return null;
        return cursor.readPref(value.mode, value.tagSet);
    },
    '$returnKey': function(cursor) {
        // no argument to cursor method
        return cursor.returnKey();
    },
    '$snapshot': function(cursor) {
        // no argument to cursor method
        return cursor.snapshot();
    },
    '$sort': function(cursor, value) { return cursor.sort(value); },
    '$skip': function(cursor, value) { return cursor.skip(value); },
    '$limit': function(cursor, value) { return cursor.limit(value); },
    '$showDiskLoc': function(cursor, value) {
        console.warn('Deprecated: $showDiskLoc; Use $showRecordId.');
        return cursor.showRecordId(value);
    },
    '$showRecordId': function(cursor) {
        // no argument to cursor method
        return cursor.showRecordId();
    }
};

// Bad request errors
ShareDbRethinkDb.invalidOpVersionError = function(collectionName, id, v) {
    return {
        code: 4101,
        message: 'Invalid op version ' + collectionName + '.' + id + ' ' + op.v
    };
};
ShareDbRethinkDb.invalidCollectionError = function(collectionName) {
    return {code: 4102, message: 'Invalid collection name ' + collectionName};
};
ShareDbRethinkDb.$whereDisabledError = function() {
    return {code: 4103, message: '$where queries disabled'};
};
ShareDbRethinkDb.$mapReduceDisabledError = function() {
    return {code: 4104, message: '$mapReduce queries disabled'};
};
ShareDbRethinkDb.$aggregateDisabledError = function() {
    return {code: 4105, message: '$aggregate queries disabled'};
};
ShareDbRethinkDb.$queryDeprecatedError = function() {
    return {code: 4106, message: '$query property deprecated in queries'};
};
ShareDbRethinkDb.malformedQueryOperatorError = function(operator) {
    return {code: 4107, message: "Malformed query operator: " + operator};
};
ShareDbRethinkDb.onlyOneCollectionOperationError = function(operation1, operation2) {
    return {
        code: 4108,
        message: 'Only one collection operation allowed. ' +
        'Found ' + operation1 + ' and ' + operation2
    };
};
ShareDbRethinkDb.onlyOneCursorOperationError = function(operation1, operation2) {
    return {
        code: 4109,
        message: 'Only one cursor operation allowed. ' +
        'Found ' + operation1 + ' and ' + operation2
    };
};
ShareDbRethinkDb.cursorAndCollectionMethodError = function(collectionOperation) {
    return {
        code: 4110,
        message: 'Cursor methods can\'t run after collection method ' +
        collectionOperation
    };
};

// Internal errors
ShareDbRethinkDb.alreadyClosedError = function() {
    return {code: 5101, message: 'Already closed'};
};
ShareDbRethinkDb.missingLastOperationError = function(collectionName, id) {
    return {
        code: 5102,
        message: 'Snapshot missing last operation field "_o" ' + collectionName + '.' + id
    };
};
ShareDbRethinkDb.missingOpsError = function(collectionName, id, from) {
    return {
        code: 5103,
        message: 'Missing ops from requested version ' + collectionName + '.' + id + ' ' + from
    };
};
// Modifies 'err' argument
ShareDbRethinkDb.parseQueryError = function(err) {
    err.code = 5104;
    return err;
};
