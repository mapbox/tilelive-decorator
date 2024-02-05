# @mapbox/tilelive-decorator

[![Circle CI](https://circleci.com/gh/mapbox/tilelive-decorator.svg?style=svg&circle-token=c22fed6001fd3757877eba8c55f119dd19f66702)](https://circleci.com/gh/mapbox/tilelive-decorator)


Load vector tiles from a tilelive source and decorate them with properties from redis. So if you use
tilelive-s3 it can load tiles from s3, and add properties to features from redis.

**NOTE**: Starting from version v4.0.2, this package has moved from `tilelive-decorator` to `@mapbox/tilelive-decorator`.

## usage

#### with tilelive

Tilelive decorator registers several tilelive protocols:

- `decorator+s3:` for reading tiles from S3
- `decorator+mbtiles:` for reading tiles from an mbtiles

```js
var tilelive = require('tilelive');
var TileliveDecorator = require('tilelive-decorator');
var TileliveS3 = require('tilelive-s3');

TileliveDecorator.registerProtocols(tilelive);
TileliveS3.registerProtocols(tilelive);

tilelive.load('decorator+s3://test/{z}/{x}/{y}?key=id&sourceProps={"keep":["id","name"]}&redis=redis://localhost:6379', function(err, source) {
  // source.getTile(z, x, y, callback);
});
```



#### manually

```js
var TileliveDecorator = require('tilelive-decorator');

var uri = 'decorator+s3://test/{z}/{x}/{y}?key=id&sourceProps={"keep":["id","name"]}&redis=redis://localhost:6379'
new TileliveDecorator(uri, function (err, source) {
  // source.getTile(z, x, y, callback);
});
```



#### options

**key** (required) - specifies what property in the source tiles will be matched to keys in redis.

**sourceProps** - a json object, specifying properties to `keep` from the source tile, and properties that are `required` to exist on features in the source tile. example: `{"keep": ["id", "class"], "required": ["rating"]}`. If all `required` properties don't exist on a feature, that feature is filtered out.

**redisProps** - a json object, specifying properties to `keep` from redis records, and properties that are `required` to exist on redis records. example: `{"keep": ["congestion"], "required": ["speed"]}`. If all `required` properties don't exist on a record, that record is filtered out and no new properties will be applied to features that match the record key.

**outputProps** - a json object, specifying properties to `keep` in the output tile after decoration, and properties that are `required` to exist on features in the output tile. example: `{"keep": ["class", "congestion"], "required": ["congestion"]}`. If all `required` properties don't exist on a feature, that feature is filtered out.

**redis** - a redis connection string, e.g. `redis://localhost:6379`.

**hashes** - If `hashes=true` is included, redis keys are treated as hash types as opposed to stringified JSON data in string type keys. In this case `hget` is used instead of the default `get` commands.


## development

#### setup

Tests and benchmarks require a local redis server

```
brew install redis
git clone https://github.com/mapbox/tilelive-decorator
cd tilelive-decorator
npm install
redis-server --save "" &
```


#### benchmarks

```js
redis-cli flushall && node benchmark.js
```

#### tests

```js
redis-cli flushall && npm test
```
