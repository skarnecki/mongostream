var program  = require('commander')
    , Db = require('mongodb').Db
    , Connection = require('mongodb').Connection
    , EventEmitter = require('events').EventEmitter
    , BSON = require('mongodb').BSONPure
    , Server = require('mongodb').Server;

event = new EventEmitter;

Optlog = function () {
};

Optlog.prototype.open = function (host, port, callback) {
    this.db = new Db('local', new Server(host, port, {auto_reconnect: true}), {w:0});
    this.targetDb = new Db('test', new Server(host, port, {auto_reconnect: true}), {w:0});

    event.on('oplog:connected', function(){console.log('connected')});
    event.on('oplog:disconnected', function(){console.log('disconnected')});
    event.on('oplog:cursorStarted', function(){console.log('cursor started')});
    event.on('oplog:cursorFoundDoc', function(doc){console.log('found doc')});
    event.on('oplog:cursor:empty', function(){console.log('no data')})
    this.db.open(function(){event.emit('oplog:connected');});
    callback()
}

Optlog.prototype.stop = function (callback) {
    this.db.close(function(){event.emit('oplog:disconnected');});
    callback()
}

Optlog.prototype.simpleFind = function (namespace, id, callback) {
    collectionName = namespace.substr(0, namespace.indexOf('.'))
    this.targetDb.collection(collectionName).findOne({_id: id}, function(err, data){
        event.emit('outstream:update', data)
    });
}

Optlog.prototype.tailCollection = function (opts) {
    var query = {};
    if (opts.collection) {
        query = {'ns':opts.collection};
    }

    this.db.collection('oplog.rs').find(
        query,
        {tailable:true, awaitdata: true},
        function(err, cursor) {
            event.emit('oplog:cursorStarted', cursor);
            cursor.each(function(error, doc){

                if (doc == typeof(undefined)) {
                    event.emit('oplog:cursor:empty');
                } else if (doc.op == 'i') {
                    event.emit('oplog:cursor:insert', doc.ns, doc.o._id);
                } else if (doc.op == 'u') {
                    event.emit('oplog:cursor:update', doc.ns, doc.o2._id);
                }
        });
    });
}

var log = (new Optlog);

log.open("192.168.1.99", 27502, function(){
    log.tailCollection({collection: 'test.test'});
});

event.on('oplog:cursor:insert', function (namespace, id){
    log.simpleFind(namespace, id, function(error, data) { console.log(data);});
})
event.on('oplog:cursor:update', function (namespace, id){
    log.simpleFind(namespace, id, function(error, data) { console.log(data);});
});

event.on('outstream:update', function(data) {
    console.log(data);
})



//setTimeout(function() {
//    console.log('searching')
//    // Peform a simple find and return all the documents
//    //tailable:true, awaitdata: true
//}, 1000)
