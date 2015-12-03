var TileliveDecorator = require('..');
var TileliveS3 = require('tilelive-s3');
var tape = require('tape');
var url = require('url');
var redis = require('redis');
var client = redis.createClient();
client.unref();
var zlib = require('zlib');
var VectorTile = require('../lib/vector-tile').Tile;
var Protobuf = require('pbf');

var undecorated;

tape('setup', function(assert) {
    zlib.gunzip(require('fs').readFileSync(__dirname + '/14-4831-6159-undecorated.vector.pbfz'), function(err, buffer) {
        assert.ifError(err);
        undecorated = buffer;
        assert.end();
    });
});

tape('bench getDecoratorKeys', function(assert) {
    var tile = VectorTile.read(new Protobuf(undecorated));
    var layer = tile.layers[0];

    var start = +new Date;
    for (var i = 0; i < 10; i++) {
        TileliveDecorator.getDecoratorKeys(layer, 'productId');
    }
    assert.ok(true, ((+new Date - start)/10) + ' ms/run (x10 runs)');
    assert.end();
});

tape('bench vt parse + decorateLayer', function(assert) {
    var tile = VectorTile.read(new Protobuf(undecorated));
    var layer = tile.layers[0];
    var replies = [];
    TileliveDecorator.getDecoratorKeys(layer, 'productId').forEach(function(id, i) {
        replies.push("{\"shoeSize\":10}");
    });

    var start = +new Date;
    for (var i = 0; i < 10; i++) {
        tile = VectorTile.read(new Protobuf(undecorated));
        layer = tile.layers[0];
        TileliveDecorator.decorateLayer(layer, replies);
    }
    assert.ok(true, ((+new Date - start)/10) + ' ms/run (x10 runs)');
    assert.end();
});

