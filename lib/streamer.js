var program  = require('commander')
    , Db = require('mongodb').Db
    , Connection = require('mongodb').Connection
    , Server = require('mongodb').Server;

function Streamer(event, onUpdate, options, undefined) {
    this.event = event;
    this.event.on('outstream:update', onUpdate);

    this.options = options;

    if (options.transformatorKeyMutator != undefined) {
        this.transformatorKeyMutator = function(collectionName, data) {
            return '#' + collectionName + '/' + data._id;
        };
    }

    if (options.transformatorValueMutator != undefined) {
        this.transformatorValueMutator = function(collectionName, data) {
            return JSON.stringify(data);
        };
    }
}

Streamer.prototype.startFinder = function(mongodbHost, mongodbPort, mongodbWatchedCollection) {
    // Open mongodb targer collection
    var targetDb = new Db(
        mongodbWatchedCollection,
        new Server(mongodbHost, mongodbPort, {auto_reconnect: true}),
        {w:0}
    );

    var event = this.event;
    var transformatorKeyMutator = this.transformatorKeyMutator;
    var transformatorValueMutator = this.transformatorValueMutator;

    targetDb.open(function(error, targetDb){
        event.on('oplog:cursor:insert', function (collectionName, id) {
            targetDb.collection(collectionName).findOne({_id: id}, function(err, data){
                event.emit('outstream:mutate', collectionName, data)
            });
        });

        event.on('oplog:cursor:update', function (collectionName, id) {
            targetDb.collection(collectionName).findOne({_id: id}, function(err, data){
                event.emit('outstream:mutate', collectionName, data);
            });
        });

        event.on('outstream:mutate', function(collectionName, data) {
            event.emit(
                'outstream:update',
                transformatorKeyMutator(collectionName, data),
                transformatorValueMutator(collectionName, data)
            );
        });
    });
};

Streamer.prototype.startTailer = function(mongodbHost, mongodbPort, mongodbWatchedDb, mongodbWatchedCollection, mongodbLocalDb) {
    var oplogDb = new Db(
        mongodbLocalDb,
        new Server(mongodbHost, mongodbPort, {auto_reconnect: true}),
        {w:0}
    );

    var event = this.event;

    oplogDb.open(function (err, oplogDb) {
        console.log('connected')
        var query = {'ns':mongodbWatchedDb + '.' + mongodbWatchedCollection};
        var queryOptions = {tailable: true, awaitdata: true, numberOfRetries: -1};

        oplogDb.collection('oplog.rs').find(
            query,
            queryOptions,
            function(err, cursor) {
                event.emit('oplog:cursorStarted', cursor);

                cursor.each(function(error, doc){
//                    console.log(doc)
                    if (doc == null) {
                        event.emit('oplog:cursor:empty');
                    } else if (doc.op == 'i') {
                        event.emit(
                            'oplog:cursor:insert',
                            doc.ns.substr(0, doc.ns.indexOf('.')),
                            doc.o._id
                        );
                    } else if (doc.op == 'u') {
                        event.emit(
                            'oplog:cursor:update',
                            doc.ns.substr(0, doc.ns.indexOf('.')),
                            doc.o2._id
                        );
                    }
                });
            });
    });
};

module.exports = Streamer;
