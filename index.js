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
    // this.client = redis.createClient(query.redis);
    // this.hashes = query.hashes === 'true';
    // this.cache = new LRU({max: 10000});

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
    // var client = this.client;

    self._fromSource.getTile(z, x, y, function(err, buffer) {
        if (err) return callback(err);
        zlib.gunzip(buffer, function(err, buffer) {
            if (err) return callback(err);

            var tile = TileDecorator.read(buffer);
            var layer = tile.layers[0];
            if (!layer) return callback(new Error('No layers found'));
            if (self.sourceProps.required) TileDecorator.filterLayerByKeys(layer, self.sourceProps.required);

            var keysToGet = TileDecorator.getLayerValues(layer, self.key);

            loadAttributes(keysToGet, self.map, function(err, replies) {
                if (err) callback(err);

                for (var i = 0; i < replies.length; i++) {
                    if (!replies[i]) continue; // skip checking

                    if (typeof replies[i] !== 'object') {
                        return callback(new Error('Invalid attribute data: ' + replies[i]));
                    }

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

function loadAttributes(keys, map, callback) {
    var replies = keys.map(function (key) {
        return map.get(key)
    });

    return callback(null, replies, replies.length)
}

Decorator.prototype.close = function(callback) {
    // this.client.unref();
    callback();
};

Decorator.registerProtocols = function(tilelive) {
    tilelive.protocols['decorator:'] = Decorator;
    tilelive.protocols['decorator+s3:'] = Decorator;
    tilelive.protocols['decorator+mbtiles:'] = Decorator;
};
