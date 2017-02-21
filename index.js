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
    this.requiredKeys = uri.requiredKeys || uri.query.requiredKeys;
    if (this.requiredKeys) this.requiredKeys = this.requiredKeys.split(',');
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

            loadAttributes(keysToGet, client, cache, function(err, replies) {
                if (err) callback(err);
                if (!useHashes) replies = replies.map(JSON.parse);

                for (var i = 0; i < replies.length; i++) {
                    if (typeof replies[i] !== 'object')
                        return callback(new Error('Invalid attribute data: ' + replies[i]));
                }
                TileDecorator.decorateLayer(layer, source.keepKeys, replies, source.requiredKeys);
                TileDecorator.mergeLayer(layer);
                zlib.gzip(new Buffer(TileDecorator.write(tile)), callback);
            });
        });
    });
};

function loadAttributes(useHashes, keys, client, cache, callback) {
    // Grab cached values from LRU, leave
    // remaining for retrieval from redis.
    var replies = [];
    var loadKeys = [];
    var loadPos = [];
    var queryCt = 0;
    var multi = client.multi();

    for (var i = 0; i < keys.length; i++) {
        var cached = cache.get(keys[i]);

        if (cached) {
            replies[i] = cached;
        } else {
            queryCt++;
            if (useHashes) multi.hgetall(keys[i]);
            else multi.get(keys[i])
            // loadKeys.push(keys[i]);
            loadPos.push(i);
        }
    }

    // Nothing left to hit redis for.
    if (queryCt === 0) return callback(null, replies, 0);

    multi.exec(function(err, loaded) {
        if (err) return callback(err);

        // Insert redis-loaded values into the right positions and set in LRU cache.
        for (var i = 0; i < loaded.length; i++) {
            replies[loadPos[i]] = loaded[i];
            cache.set(loadKeys[i], loaded[i]);
        }

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
