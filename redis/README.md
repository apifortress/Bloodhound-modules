# Bloodhound - Redis module

The module implements a number of functions requiring interaction with the Redis data store.

## Proxy

### UpstreamRedisActor

Upstream connecting to a Redis database.

**class:** `com.apifortress.afthem.modules.redis.actors.proxy.UpstreamRedisActor`

**sidecars**: yes

**config:**
* `uri`: the Redis URI

**headers:**
* `x-op`: options are `get|set|zadd|sadd|spop|hset|hgethgetall|expire`

**Multi-flow**: no

#### Request bodies

* **get**
```json
{
  "key": "key"
}
```

* **set**
```json
{
  "key": "key",
  "value": "value"
}
```

* **zadd**
```json
{
  "key": "key",
  "value": "value",
  "score": 1
}
```

* **sadd**
```json
{
  "key": "key",
  "value": "value"
}
```

* **spop**
```json
{
  "key": "key"
}
```

* **smembers**
```json
{
  "key": "key"
}
```


* **hset**
```json
{
  "key": "key",
  "field": "field",
  "value": "value"
}
```

* **hget**
```json
{
  "key": "key",
  "field": "field"
}
```

* **hgetall**
```json
{
  "key": "key"
}
```

* **expire**
```json
{
  "key": "key",
  "value": 15
}
```