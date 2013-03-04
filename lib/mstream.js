var program  = require('commander')
    , EventEmitter = require('events').EventEmitter
    , redis = require('redis')
    , mc = require('mc')
    , Streamer = require('./streamer');


options = {useRedis:false, useMemcache:true};

var mongodbHost = '192.168.1.99';
var mongodbPort = 27502;
var mongodbWatchedDb = 'test';
var mongodbWatchedCollection = 'test';
var mongodbLocalDb = 'local';
var redisHost = '192.168.1.99';
var redisPort = 6379;

if (options.useRedis) {
    // Open connection to redis
    var redisConnection = redis.createClient(redisPort, redisHost)

    redisConnection.on("error", function (err) {
        console.log("Error " + err);
    });

    var cacheSetCallback = function(key, value) {
        redisConnection.set(key, value);
    };

} else if (options.useMemcache) {
    var memcacheConnection = new mc.Client(['192.168.1.99']);
    memcacheConnection.connect(function(){console.log('memconnected')});

    var cacheSetCallback = function(key, value) {
        memcacheConnection.set(key, value, { flags: 0, exptime: 0}, function(err, status) {
//            console.log('status' + status);
//            console.log('error' + err.type);
//            console.log('error' + err.description);
        });
    };
}

var streamer = new Streamer(new EventEmitter,
    cacheSetCallback,
    {
    transformatorKeyMutator: function(collectionName, data) {
        return '#' + collectionName + '/' + data._id;
    },
    transformatorValueMutator: function(collectionName, data) {
        return JSON.stringify(data);
    }

});

streamer.startTailer(mongodbHost, mongodbPort, mongodbWatchedDb, mongodbWatchedCollection, mongodbLocalDb);
streamer.startFinder(mongodbHost, mongodbPort, mongodbWatchedCollection);

