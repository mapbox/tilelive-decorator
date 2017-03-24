var fs = require('fs'),
  path = require('path'),
  zlib = require('zlib'),
  redis = require('redis'),
  Decorator = require('./index'),
  Benchmark = require('benchmark'),
  TileDecorator = require('tile-decorator');


var tilePath = path.join(__dirname, 'test/benchmark-tile.vector.pbf.gz'),
  benchOptions =  {defer: true, delay: 0.25, minSamples: 20},
  tile = TileDecorator.read(zlib.gunzipSync(fs.readFileSync(tilePath))),
  ids = TileDecorator.getLayerValues(tile.layers[0], '@id'),
  client = redis.createClient();

var source = {
  getTile: function (z, x, y, callback) {
    fs.readFile(tilePath, callback);
  }
};

ids.forEach(function (id, i) {
  var props = {
    foo: Math.round(Math.random() * 100),
    bar: Math.round(Math.random() * 100),
  };
  if (i % 2 === 0) props.baz = Math.round(Math.random() * 100);

  client.set(id, JSON.stringify(props));
});

client.quit();
client.on('end', function () {
  console.log('starting benchmarking');

  var suite = new Benchmark.Suite('tilelive-decorator');
  suite
    .add('decorator',function (deferred) {
      new Decorator({source: source, key: '@id', keepKeys: '@id,highway'}, function (err, dec) {
        dec.getTile(1, 1, 1, function (err, data) {
          dec.close(function () {
            deferred.resolve();
          });
        });
      });
    }, benchOptions)
    .add('decorator#requiredKeysRedis',function (deferred) {
      new Decorator({source: source, key: '@id', keepKeys: '@id,highway', requiredKeysRedis: 'baz'}, function (err, dec) {
        dec.getTile(1, 1, 1, function (err, data) {
          dec.close(function () {
            deferred.resolve();
          });
        });
      });
    }, benchOptions)
    .add('decorator#requiredKeys',function (deferred) {
      new Decorator({source: source, key: '@id', keepKeys: '@id,highway', requiredKeys: 'railway'}, function (err, dec) {
        dec.getTile(1, 1, 1, function (err, data) {
          dec.close(function () {
            deferred.resolve();
          });
        });
      });
    }, benchOptions)
    .on('cycle', function (event) {
      console.log(String(event.target));
    })
    .on('complete', function () { })
    .run();
});
