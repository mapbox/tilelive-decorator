'use strict';

var TileliveDecorator = require('..');
var TileliveS3 = require('tilelive-s3');
var tape = require('tape');
var redis = require('redis');
var zlib = require('zlib');
var VectorTile = require('vector-tile').VectorTile;
var Protobuf = require('pbf');
var fs = require('fs');
var path = require('path');
var tilelive = require('tilelive');
var LRU = require('lru-cache');

function TestSource(uri, callback) {
    return callback(null, this);
}
TestSource.prototype.getTile = function(z, x, y, callback) {
    var key = [z, x, y].join('-');
    fs.readFile(path.join(__dirname, key + '-undecorated.vector.pbfz'), function(err, zdata) {
        if (err && err.code === 'ENOENT') return callback(new Error('Tile does not exist'));
        if (err) return callback(err);
        callback(null, zdata, {});
    });
};

var client = redis.createClient();

tape('setup', function(assert) {
    client.set('4', JSON.stringify({foo: 3, bar: 'baz'}), redis.print);
    client.unref();
    assert.end();
});

tape('load with decorator+s3 uri', function(assert) {
    TileliveDecorator.registerProtocols(tilelive);
    TileliveS3.registerProtocols(tilelive);
    tilelive.load('decorator+s3://test/{z}/{x}/{y}?key=BoroCode&redis=redis://localhost:6379', function(err, source) {
        assert.ifError(err);
        assert.equal(source.key, 'BoroCode');
        assert.equal(source.client.address, 'localhost:6379');
        assert.equal(source._fromSource instanceof TileliveS3, true);
        source.client.unref();
        assert.end();
    });
});

tape('setup source directly', function(assert) {
    new TestSource(null, function(err, testSource) {
        assert.ifError(err);
        new TileliveDecorator({key: 'BoroCode', source: testSource}, function(err, source) {
            assert.ifError(err);
            source.getTile(14, 4831, 6159, function(err, tile) {
                assert.ifError(err);
                assert.equal(tile.length > 600 && tile.length < 700, true, 'buffer size check');
                zlib.gunzip(tile, function(err, buffer) {
                    assert.ifError(err);
                    var tile = new VectorTile(new Protobuf(buffer));
                    var decorated = tile.layers.nycneighborhoods.feature(tile.layers.nycneighborhoods.length - 1);

                    assert.deepEqual(decorated.properties, {
                        BoroCode: 4,
                        BoroName: 'Queens',
                        CountyFIPS: '081',
                        NTACode: 'QN99',
                        NTAName: 'park-cemetery-etc-Queens',
                        Shape_Area: 316377640.656,
                        Shape_Leng: 454280.564015,
                        bar: 'baz',
                        foo: 3
                    });

                    assert.deepEqual(decorated.loadGeometry(), [
                        [{x: 1363, y: -128},
                         {x: 1609, y: 150},
                         {x: 1821, y: 443},
                         {x: 1909, y: 598},
                         {x: 2057, y: 917},
                         {x: 2188, y: 1241},
                         {x: 2530, y: 2026},
                         {x: 2635, y: 2197},
                         {x: 2743, y: 2432},
                         {x: 2811, y: 2726},
                         {x: 2836, y: 2781},
                         {x: 2894, y: 2849},
                         {x: 3097, y: 3019},
                         {x: 3784, y: 3700},
                         {x: 3731, y: 3796},
                         {x: 3652, y: 3874},
                         {x: 3475, y: 3949},
                         {x: 3554, y: 4002},
                         {x: 3754, y: 3942},
                         {x: 4025, y: 3828},
                         {x: 4224, y: 3759},
                         {x: 4224, y: 1426},
                         {x: 4202, y: 1389},
                         {x: 4110, y: 1300},
                         {x: 3996, y: 1237},
                         {x: 3870, y: 1199},
                         {x: 3784, y: 1159},
                         {x: 3706, y: 1104},
                         {x: 3638, y: 1036},
                         {x: 3585, y: 959},
                         {x: 3429, y: 632},
                         {x: 3284, y: 410},
                         {x: 3174, y: 170},
                         {x: 3026, y: 14},
                         {x: 3537, y: -128},
                         {x: 1363, y: -128}
                    ]]);

                    source.client.unref();
                    assert.end();
                });
            });
        });
    });
});

tape('redis config', function(assert) {
    new TestSource(null, function(err, testSource) {
        assert.ifError(err);
        new TileliveDecorator({key: 'BoroCode', source: testSource, redis: 'redis://foo'}, function(err, source) {
            assert.ifError(err);
            source.client.once('error', function(err) {
                assert.equal(err.message.indexOf('Redis connection to foo:6379 failed - getaddrinfo'), 0);
                source.client.end();
                source.client.unref();
                assert.end();
            });
        });
    });
});

tape('setup', function(assert) {
    var client = redis.createClient();
    client.set('4', '"bad data"', redis.print);
    client.unref();
    assert.end();
});

tape('fail on bad redis data', function(assert) {
    new TestSource(null, function(err, testSource) {
        assert.ifError(err);
        new TileliveDecorator({key: 'BoroCode', source: testSource}, function(err, source) {
            assert.ifError(err);
            source.getTile(14, 4831, 6159, function(err) {
                assert.ok(err, 'expected error');
                if (!err) return assert.end();

                assert.equal(err.message, 'Invalid attribute data: bad data', 'expected error message');
                source.close(function() {
                    assert.end();
                });
            });
        });
    });
});

var cache = new LRU({max: 1000});

tape('lru setup', function(assert) {
    client.mset(
        '1', JSON.stringify({foo: 1}),
        '2', JSON.stringify({foo: 2}),
        '3', JSON.stringify({foo: 3}),
        '4', JSON.stringify({foo: 4}),
        assert.end
    );
});

tape('loadAttributes (cache miss)', function(assert) {
    TileliveDecorator.loadAttributes(['1', '2'], client, cache, function(err, replies, loaded) {
        assert.ifError(err);
        assert.deepEqual(replies, ['{"foo":1}', '{"foo":2}'], 'loads');
        assert.equal(cache.get('1'), '{"foo":1}', 'sets item 1 in cache');
        assert.equal(cache.get('2'), '{"foo":2}', 'sets item 2 in cache');
        assert.equal(loaded, 2, '2 items loaded from redis');
        assert.end();
    });
});

tape('loadAttributes (cache hit)', function(assert) {
    TileliveDecorator.loadAttributes(['1', '2'], client, cache, function(err, replies, loaded) {
        assert.ifError(err);
        assert.deepEqual(replies, ['{"foo":1}', '{"foo":2}'], 'loads');
        assert.equal(loaded, 0, '0 items loaded from redis');
        assert.end();
    });
});

tape('loadAttributes (cache mixed)', function(assert) {
    TileliveDecorator.loadAttributes(['1', '3', '2', '4'], client, cache, function(err, replies, loaded) {
        assert.ifError(err);
        assert.deepEqual(replies, ['{"foo":1}', '{"foo":3}', '{"foo":2}', '{"foo":4}'], 'loads');
        assert.equal(cache.get('1'), '{"foo":1}', 'sets item 1 in cache');
        assert.equal(cache.get('2'), '{"foo":2}', 'sets item 2 in cache');
        assert.equal(cache.get('3'), '{"foo":3}', 'sets item 3 in cache');
        assert.equal(cache.get('4'), '{"foo":4}', 'sets item 4 in cache');
        assert.equal(loaded, 2, '2 items loaded from redis');
        assert.end();
    });
});

tape('lru teardown', function(assert) {
    client.unref();
    assert.end();
});
