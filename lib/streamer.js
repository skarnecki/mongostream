var program  = require('commander')
    , Db = require('mongodb').Db
    , Connection = require('mongodb').Connection
    , Server = require('mongodb').Server;

function Streamer(event, onUpdate, options, undefined) {
    this.event = event;
    this.event.on(event.EVENT_OUTSTREAM_UPDATE, onUpdate);
    this.options = options;
    this.OPLOG_DB_NAME = 'local';
    this.OPLOG_COLLECTION_NAME = 'oplog.rs';
}

Streamer.prototype.startFinder = function(mongodbHost, mongodbPort, mongodbWatchedDb, mongodbWatchedCollection, queryExtend) {
    var event = this.event;
    var transformatorKeyMutator = this.options.transformatorKeyMutator;
    var transformatorValueMutator = this.options.transformatorValueMutator;
    var query = queryExtend || {};

    var targetDb = new Db(
        mongodbWatchedDb,
        new Server(mongodbHost, mongodbPort, {auto_reconnect: true}),
        {w:0}
    );

    targetDb.open(function(error, targetDb){
        event.on(event.EVENT_OPLOG_INSERT, function (collectionName, id) {
            query._id = id;
            targetDb.collection(collectionName).findOne(query, function(err, data){
                event.emit(event.EVENT_OUTSTREAM_MUTATE, collectionName, data)
            });
        });

        event.on(event.EVENT_OPLOG_UPDATE, function (collectionName, id) {
            query._id = id;
            targetDb.collection(collectionName).findOne(query, function(err, data){
                event.emit(event.EVENT_OUTSTREAM_MUTATE, collectionName, data);
            });
        });

        event.on(event.EVENT_OUTSTREAM_MUTATE, function(collectionName, data) {
            event.emit(
                event.EVENT_OUTSTREAM_UPDATE,
                transformatorKeyMutator(collectionName, data),
                transformatorValueMutator(collectionName, data)
            );
        });
    });
};

Streamer.prototype.startTailer = function(mongodbHost, mongodbPort, mongodbWatchedDb, mongodbWatchedCollection) {

    var event = this.event;
    var collectionName = this.OPLOG_COLLECTION_NAME;
    var oplogDb = new Db(
        this.OPLOG_DB_NAME,
        new Server(mongodbHost, mongodbPort, {auto_reconnect: true}),
        {w:0}
    );

    oplogDb.open(function (err, oplogDb) {
        console.log('connected')
        var query = {'ns':mongodbWatchedDb + '.' + mongodbWatchedCollection};
        var queryOptions = {tailable: true, awaitdata: true, numberOfRetries: -1};

        oplogDb.collection(collectionName).find(
            query,
            queryOptions,
            function(err, cursor) {
                event.emit(event.EVENT_OPLOG_STARTED, cursor);

                cursor.each(function(error, doc){
//                    console.log(doc)
                    if (doc == null) {
                        event.emit(event.EVENT_OPLOG_EMPTY);
                    } else if (doc.op == 'i') {
                        event.emit(
                            event.EVENT_OPLOG_INSERT,
                            doc.ns.substr(0, doc.ns.indexOf('.')),
                            doc.o._id
                        );
                    } else if (doc.op == 'u') {
                        event.emit(
                            event.EVENT_OPLOG_UPDATE,
                            doc.ns.substr(0, doc.ns.indexOf('.')),
                            doc.o2._id
                        );
                    }
                    // TODO delete
                });
            }
        );
    });
};

Streamer.prototype.populateOnStart = function(mongodbHost, mongodbPort, mongodbWatchedDb, mongodbWatchedCollection, queryExtend) {
    var transformatorKeyMutator = this.options.transformatorKeyMutator;
    var transformatorValueMutator = this.options.transformatorValueMutator;
    var event = this.event;
    var query = queryExtend || {};

    var targetDb = new Db(
        mongodbWatchedDb,
        new Server(mongodbHost, mongodbPort, {auto_reconnect: true}),
        {w:0}
    );

    targetDb.open(function(error, targetDb){
        targetDb.collection(mongodbWatchedCollection).find(query, function(err, cursor) {
            cursor.each(function (err, document) {
                if (document != null) {
                    event.emit(
                        event.EVENT_OUTSTREAM_UPDATE,
                        transformatorKeyMutator(mongodbWatchedCollection, document),
                        transformatorValueMutator(mongodbWatchedCollection, document)
                    );
                }
            })
        });
    });
}

module.exports = Streamer;
