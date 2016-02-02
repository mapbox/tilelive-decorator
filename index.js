'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var redis = require('redis');
var Protobuf = require('pbf');
var zlib = require('zlib');
var VectorTile = require('./lib/vector-tile').Tile;
var qs = require('querystring');
var url = require('url');
var tilelive = require('tilelive');
var LRU = require('lru-cache');

module.exports = Decorator;
util.inherits(Decorator, EventEmitter);

module.exports.getDecoratorKeys = getDecoratorKeys;
module.exports.decorateLayer = decorateLayer;
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
    this.client = redis.createClient(uri.redis || uri.query.redis);
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

/**
 * Fetch a tile from S3 and extend its features' properties with data stored in
 * Redis.
 */
Decorator.prototype.getTile = function(z, x, y, callback) {
    var source = this;
    var client = this.client;
    var cache = this.cache;
    this._fromSource.getTile(z, x, y, function(err, buffer) {
        if (err) return callback(err);
        zlib.gunzip(buffer, function(err, buffer) {
            if (err) return callback(err);

            var tile = VectorTile.read(new Protobuf(buffer));
            var layer = tile.layers[0];
            if (!layer) return callback(new Error('No layers found'));

            var keysToGet = getDecoratorKeys(layer, source.key);

            loadAttributes(keysToGet, client, cache, function(err, replies) {
                if (err) throw err;

                try {
                    decorateLayer(layer, replies, source.key);
                } catch (err) {
                    return callback(err);
                }

                var pbf = new Protobuf();
                VectorTile.write(tile, pbf);
                zlib.gzip(pbf.finish(), callback);
            });
        });
    });
};

function loadAttributes(keys, client, cache, callback) {
    // Grab cached values from LRU, leave
    // remaining for retrieval from redis.
    var replies = [];
    var loadKeys = [];
    var loadPos = [];
    for (var i = 0; i < keys.length; i++) {
        var cached = cache.get(keys[i]);
        if (cached) {
            replies[i] = cached;
        } else {
            loadKeys.push(keys[i]);
            loadPos.push(i);
        }
    }

    // Nothing left to hit redis for.
    if (!loadKeys.length) return callback(null, replies, 0);

    client.mget(loadKeys, function(err, loaded) {
        if (err) return callback(err);

        // Insert redis-loaded values into the right positions
        // and set in LRU cache.
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

function decorateLayer(layer, replies) {
    var keyLookup = {};
    var keyIndex = 0;
    for (var k in layer.keys) {
        keyLookup[layer.keys[k]] = keyIndex;
        keyIndex++;
    }

    var valLookup = {};
    var valIndex = 0;
    for (k in layer.values) {
        valLookup[typedKey(layer.values[k])] = valIndex;
        valIndex++;
    }

    for (var i in replies) {
        if (replies[i]) {
            var feature = layer.features[i];
            var attrs = JSON.parse(replies[i]);

            if (typeof attrs !== 'object') {
                throw new Error('Invalid attribute data: ' + attrs);
            }

            for (k in attrs) {
                var keyTag = keyLookup[k];
                if (keyTag === undefined) {
                    keyTag = keyIndex;
                    keyLookup[k] = keyIndex;
                    layer.keys.push(k);
                    keyIndex++;
                }

                var valTypedKey = typedKey(attrs[k]);
                var valTag = valLookup[valTypedKey];
                if (valTag === undefined) {
                    valTag = valIndex;
                    valLookup[valTypedKey] = valIndex;
                    layer.values.push(typed(attrs[k]));
                    valIndex++;
                }

                feature.tags.push(keyTag);
                feature.tags.push(valTag);
            }
        }
    }
}

function getDecoratorKeys(layer, key) {
    var keysToGet = [];
    var keyIndex = layer.keys.indexOf(key);
    for (var i in layer.features) {
        var feature = layer.features[i];
        var valIndex = feature.tags[feature.tags.indexOf(keyIndex) + 1];
        for (var j in layer.values[valIndex]) {
            keysToGet.push(layer.values[valIndex][j]);
            break;
        }
    }
    return keysToGet;
}

function typedKey(value) {
    if (typeof value === 'string') return 's:' + value;
    if (typeof value === 'boolean') return 'b:' + value;
    if (typeof value === 'number') return 'n:' + value;
    if (typeof value === 'object') return 'o:' + JSON.stringify(value);
    return '?:' + value.toString();
}

function typed(value) {
    if (typeof value === 'string') return {string_value: value};
    if (typeof value === 'boolean') return {bool_value: value};
    if (typeof value === 'number') return {float_value: value};
    if (typeof value === 'object') return {string_value: JSON.stringify(value)};
    return {string_value: value.toString()};
}

Decorator.registerProtocols = function(tilelive) {
    tilelive.protocols['decorator:'] = Decorator;
    tilelive.protocols['decorator+s3:'] = Decorator;
    tilelive.protocols['decorator+mbtiles:'] = Decorator;
};
