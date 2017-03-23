# tilelive-decorator

[![Circle CI](https://circleci.com/gh/mapbox/tilelive-decorator.svg?style=svg&circle-token=c22fed6001fd3757877eba8c55f119dd19f66702)](https://circleci.com/gh/mapbox/tilelive-decorator)

Load vector tiles from a tilelive source and decorate them with properties from redis. So if you use
tilelive-s3 it can load tiles from s3, and add properties to features from redis.

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

tilelive.load('decorator+s3://test/{z}/{x}/{y}?key=id&keepKeys=id,name&redis=redis://localhost:6379', function(err, source) {
  // source.getTile(z, x, y, callback);
});
```



#### manually

```js
var TileliveDecorator = require('tilelive-decorator');

var uri = 'decorator+s3://test/{z}/{x}/{y}?key=id&keepKeys=id,name&redis=redis://localhost:6379'
new TileliveDecorator(uri, function (err, source) {
  // source.getTile(z, x, y, callback); 
});
```



#### options

**key** (required) - specifies what property in the source tiles will be matched to keys in redis.

**keepKeys** (required) - a comma separated list of columns to be copied from source tiles to decorated tiles.

**keepKeysRedis** - a comma separated list of columns to be copied from redis to decorated tiles. By default, all keys are copied.

**requiredKeys** - a comma separated list of columns which must exist on features in the source tiles. If a feature does not have all required keys, that feature is excluded from the decorated tiles.

**requiredKeysRedis** - a comma separated list of columns which must exist in the  redis record for a key. If a key's record does not have all required keys, none of that key's data is merged into the decorated tiles.

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
