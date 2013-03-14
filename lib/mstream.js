var program  = require('commander')
    , EventEmitter = require('events').EventEmitter
    , redis = require('redis')
    , mc = require('mc')
    , config = require('config')
    , _ = require('underscore')
    , Streamer = require('./streamer');


if (config.options.useRedis) {
    // Open connection to redis
    var redisConnection = redis.createClient(config.redis.port, config.redis.host)

    // Handle redis errors
    redisConnection.on("error", function (err) {
        console.log("Error " + err);
    });

    // Set cache callback function
    var cacheSetCallback = function(key, value) {
        redisConnection.set(key, value);
    };
} else if (config.options.useMemcache) {
    // Open connection to memcache
    var memcacheConnection = new mc.Client([config.memcache.host]);
    memcacheConnection.connect(function(){
        console.log('Connected to memcache!')
    });

    // Set cache callback function
    var cacheSetCallback = function(key, value) {
        memcacheConnection.add(
            key,
            value,
            {flags: config.memcache.flags, exptime: config.memcache.expirationTime},
            function(err, status, undefined) {
                if (err != undefined) {
                    console.log('Memcache error: ' + err.description);
                }
            }
        );
    };
}

var keyTemplate = _.template(config.format.key);
if (config.format.value) {
    var valueTemplate = _.template(config.format.value);
}

var streamEvent = new EventEmitter;
streamEvent['EVENT_OPLOG_INSERT'] = "oplog:cursor:insert";
streamEvent['EVENT_OPLOG_UPDATE'] = "oplog:cursor:update";
streamEvent['EVENT_OUTSTREAM_UPDATE'] = "outstream:update";
streamEvent['EVENT_OUTSTREAM_MUTATE'] = "outstream:mutate";
streamEvent['EVENT_OPLOG_STARTED'] = "oplog:cursor:started";
streamEvent['EVENT_OPLOG_EMPTY'] = "oplog:cursor:empty";

// Set streamer callbacks and data transformers
var streamer = new Streamer(
    streamEvent,
    cacheSetCallback,
    {
    transformatorKeyMutator: function(collectionName, data) {
        return keyTemplate(
            {collectionName: collectionName, data: data}
        );
    },
    transformatorValueMutator: function(collectionName, data) {
        if (config.format.value) {
            return valueTemplate({collectionName: collectionName, data: data})
        }
        return JSON.stringify(data);
    }
});

// Populate cache with all collection data
if (config.options.populateOnStart) {
    streamer.populateOnStart(
        config.target.host,
        config.target.port,
        config.target.dbname,
        config.target.collection,
        config.target.query
    );
};

streamer.startTailer(
    config.oplog.host,
    config.oplog.port,
    config.target.dbname,
    config.target.collection
);

streamer.startFinder(
    config.target.host,
    config.target.port,
    config.target.dbname,
    config.target.collection,
    config.target.query
);

