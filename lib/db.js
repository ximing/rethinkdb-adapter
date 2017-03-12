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

// 提交snapshot 和 op
ShareDbRethinkDb.prototype.commit = function(collectionName, id, op, snapshot, options, callback) {
    const self = this;
    this.db.table(collectionName)
};
ShareDbRethinkDb.prototype._writeOp = function(collectionName, id, op, snapshot, callback) {
    if (typeof op.v !== 'number') {
        var err = ShareDbRethinkDb.invalidOpVersionError(collectionName, id, op.v);
        return callback(err);
    }
    this.getOpCollection(collectionName, function(err, opCollection) {
        if (err) return callback(err);
        var doc = shallowClone(op);
        doc.d = id;
        doc.o = snapshot._opLink;
        opCollection.insertOne(doc, callback);
    });
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
