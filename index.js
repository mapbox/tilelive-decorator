'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var redis = require('redis');
var zlib = require('zlib');
var TileDecorator = require('@mapbox/tile-decorator');
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
    var query = uri.query || uri;

    this.key = query.key;
    this.client = redis.createClient(query.redis);
    this.hashes = query.hashes === 'true';
    this.cache = new LRU({max: 10000});

    /*
        Each `props` supports `keep` and `required`.

        If a feature / record does not have all `required` properties at
        the given stage of the decoration cycle, it is rejected.

        `keep` specifies which columns should be retained at that stage
            - sourceProps.keep pulls only the named properties before decoration
            - redisProps.keep controls which properties will be queried from redis
            - outputProps.keep pulls only the named properties after decoration
    */
    this.sourceProps = parsePropertiesOption(query.sourceProps);
    this.redisProps = parsePropertiesOption(query.redisProps);
    this.outputProps = parsePropertiesOption(query.outputProps);

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

// Fetch a tile from the source and extend its features' properties with data stored in Redis.
Decorator.prototype.getTile = function(z, x, y, callback) {
    var self = this;
    var client = this.client;
    var cache = this.cache;
    var useHashes = this.hashes;

    self._fromSource.getTile(z, x, y, function(err, buffer) {
        if (err) return callback(err);
        zlib.gunzip(buffer, function(err, buffer) {
            if (err) return callback(err);

            var tile = TileDecorator.read(buffer);
            var layer = tile.layers[0];
            if (!layer) return callback(new Error('No layers found'));
            if (self.sourceProps.required) TileDecorator.filterLayerByKeys(layer, self.sourceProps.required);

            var keysToGet = TileDecorator.getLayerValues(layer, self.key);

            loadAttributes(useHashes, keysToGet, client, cache, function(err, replies) {
                if (err) callback(err);
                if (!useHashes) replies = replies.map(JSON.parse);

                for (var i = 0; i < replies.length; i++) {
                    if (typeof replies[i] !== 'object') {
                        return callback(new Error('Invalid attribute data: ' + replies[i]));
                    }

                    if (replies[i] === null) continue; // skip checking

                    if (self.redisProps.required) {
                        for (var k = 0; k < self.redisProps.required.length; k++) {
                            if (!replies[i].hasOwnProperty(self.redisProps.required[k])) {
                                replies[i] = null; // empty this reply
                                break;
                            }
                        }
                    }
                }

                if (self.redisProps.keep) {
                    replies = replies.map(function(reply) {
                        if (reply === null) return reply;

                        var keep = {};
                        for (var k = 0; k < self.redisProps.keep.length; k++) {
                            var key = self.redisProps.keep[k];
                            if (reply.hasOwnProperty(key)) keep[key] = reply[key];
                        }
                        return keep;
                    });
                }

                if (self.sourceProps.keep) TileDecorator.selectLayerKeys(layer, self.sourceProps.keep);
                TileDecorator.updateLayerProperties(layer, replies);
                if (self.outputProps.required) TileDecorator.filterLayerByKeys(layer, self.outputProps.required);
                if (self.outputProps.keep) TileDecorator.selectLayerKeys(layer, self.outputProps.keep);

                TileDecorator.mergeLayer(layer);

                zlib.gzip(new Buffer(TileDecorator.write(tile)), callback);
            });
        });
    });
};

function parsePropertiesOption(option) {
    if (!option) return {};
    if (typeof option === 'string') option = JSON.parse(option);
    for (var key in option) {
        option[key] = parseListOption(option[key]);
    }
    return option;
}

function parseListOption(option) {
    if (typeof option === 'string') return option.split(',');
    return option;
}

function loadAttributes(useHashes, keys, client, cache, callback) {
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
                multi.hgetall(keys[i]);
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
