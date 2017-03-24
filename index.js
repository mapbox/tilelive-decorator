'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var redis = require('redis');
var zlib = require('zlib');
var TileDecorator = require('tile-decorator');
var qs = require('querystring');
var url = require('url');
var tilelive = require('tilelive');
var LRU = require('lru-cache');

module.exports = Decorator;
util.inherits(Decorator, EventEmitter);

module.exports.loadAttributes = loadAttributes;

/**
 * @constructor
 * @param {object} uri
 * @param {function} callback
 */
function Decorator(uri, callback) {
    if (typeof uri === 'string') uri = url.parse(uri, true);
    if (typeof uri.query === 'string') uri.query = qs.parse(uri.query);
    uri.query = uri.query || {};

    this.key = uri.key || uri.query.key;
    this.keepKeys = (uri.keepKeys || uri.query.keepKeys).split(',');
    this.keepKeysRedis = uri.keepKeysRedis || uri.query.keepKeysRedis;
    if (this.keepKeysRedis) this.keepKeysRedis = this.keepKeysRedis.split(',');
    this.requiredKeys = uri.requiredKeys || uri.query.requiredKeys;
    if (this.requiredKeys) this.requiredKeys = this.requiredKeys.split(',');
    this.requiredKeysRedis = uri.requiredKeysRedis || uri.query.requiredKeysRedis;
    if (this.requiredKeysRedis) this.requiredKeysRedis = this.requiredKeysRedis.split(',');
    this.client = redis.createClient(uri.redis || uri.query.redis);
    this.hashes = (uri.hashes || uri.query.hashes) === 'true';
    this.cache = new LRU({max: 10000});

    // Source is loaded and provided explicitly.
    if (uri.source) {
        this._fromSource = uri.source;
        callback(null, this);
    } else {
        uri.protocol = uri.protocol.replace(/^decorator\+/, '');
        tilelive.auto(uri);
        tilelive.load(uri, function(err, fromSource) {
            if (err) return callback(err);
            this._fromSource = fromSource;
            callback(null, this);
        }.bind(this));
    }
}

Decorator.prototype.getInfo = function(callback) {
    this._fromSource.getInfo(callback);
};

// Fetch a tile from S3 and extend its features' properties with data stored in Redis.
Decorator.prototype.getTile = function(z, x, y, callback) {
    var source = this;
    var client = this.client;
    var cache = this.cache;
    var useHashes = this.hashes;

    this._fromSource.getTile(z, x, y, function(err, buffer) {
        if (err) return callback(err);
        zlib.gunzip(buffer, function(err, buffer) {
            if (err) return callback(err);

            var tile = TileDecorator.read(buffer);
            var layer = tile.layers[0];
            if (!layer) return callback(new Error('No layers found'));

            var keysToGet = TileDecorator.getLayerValues(layer, source.key);

            loadAttributes(useHashes, source.keepKeysRedis, source.requiredKeysRedis, keysToGet, client, cache, function(err, replies) {
                if (err) return callback(err);

                TileDecorator.decorateLayer(layer, source.keepKeys, replies, source.requiredKeys, source.propertyTransform);
                TileDecorator.mergeLayer(layer);
                zlib.gzip(new Buffer(TileDecorator.write(tile)), callback);
            });
        });
    });
};

function loadAttributes(useHashes, keepKeysRedis, requiredKeysRedis, keys, client, cache, callback) {
    // Grab cached values from LRU, leave
    // remaining for retrieval from redis.
    var replies = [];
    var loadKeys = [];
    var loadPos = [];
    var multi = client.multi();

    for (var i = 0; i < keys.length; i++) {
        var cached = cache.get(keys[i]);

        if (cached) {
            replies[i] = cached;
        } else {
            if (useHashes) {
                if (keepKeysRedis) {
                    multi.hmget(keys[i], keepKeysRedis.slice());
                } else {
                    multi.hgetall(keys[i]);
                }
            } else {
                multi.get(keys[i]);
            }
            loadKeys.push(keys[i]);
            loadPos.push(i);
        }
    }

    // Nothing left to hit redis for.
    if (!loadKeys.length) return callback(null, replies, 0);

    multi.exec(function(err, loaded) {
        if (err) return callback(err);

        function setInCache(val, i) {
            replies[loadPos[i]] = val;
            cache.set(loadKeys[i], val);
        }

        for (var i = 0; i < loaded.length; i++) {
            if (!useHashes) loaded[i] = JSON.parse(loaded[i]);

            if (typeof loaded[i] !== 'object') {
                return callback(new Error('Invalid attribute data: ' + loaded[i]));
            }
        }
        // Insert redis-loaded values into the right positions and set in LRU cache.
        loaded.forEach(function(val, i) {
            // skip other checks if we know val is null
            if (val === null) return setInCache(val, i);
            if (keepKeysRedis && useHashes) {
                // If we're using hashes and there are keepKeysRedis, we used HMGET
                // instead of HGETALL, which means the response is an array that
                // we need to fix into an object
                var newval = {};
                for (var k = 0; k < keepKeysRedis.length; k++) { // eslint-disable-line no-redeclare
                    newval[keepKeysRedis[k]] = val[k];
                }
                val = newval;
            }


            if (requiredKeysRedis) {
                for (var k = 0; k < requiredKeysRedis.length; k++) { // eslint-disable-line no-redeclare
                    var required = requiredKeysRedis[k];
                    // If it doesn't have a required key, bail out
                    if (!val.hasOwnProperty(required) || val[required] === null) {
                        return setInCache(null, i);
                    }
                }
            }
            if (keepKeysRedis) {
                var keep = {};
                for (var k = 0; k < keepKeysRedis.length; k++) { // eslint-disable-line no-redeclare
                    var key = keepKeysRedis[k];
                    if (val.hasOwnProperty(key)) keep[key] = val[key];
                }
                val = keep;
            }

            setInCache(val, i);
        });

        return callback(null, replies, loaded.length);
    });
}

Decorator.prototype.close = function(callback) {
    this.client.unref();
    callback();
};

Decorator.registerProtocols = function(tilelive) {
    tilelive.protocols['decorator:'] = Decorator;
    tilelive.protocols['decorator+s3:'] = Decorator;
    tilelive.protocols['decorator+mbtiles:'] = Decorator;
};
