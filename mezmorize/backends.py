import pickle

from itertools import chain
from werkzeug.contrib.cache import (
    BaseCache, NullCache, SimpleCache, MemcachedCache, GAEMemcachedCache,
    FileSystemCache, RedisCache)

from ._compat import range_type


class SASLMemcachedCache(MemcachedCache):
    def __init__(self, **kwargs):
        import pylibmc

        servers = kwargs.pop('servers', ['127.0.0.1:11211'])
        default_timeout = kwargs.pop('default_timeout', 300)
        key_prefix = kwargs.pop('key_prefix', None)
        BaseCache.__init__(self, default_timeout)

        self._client = pylibmc.Client(servers, binary=True, **kwargs)
        self.key_prefix = key_prefix


def null(config, *args, **kwargs):
    return NullCache()


def simple(config, *args, **kwargs):
    kwargs.update({'threshold': config['CACHE_THRESHOLD']})
    return SimpleCache(*args, **kwargs)


def memcached(config, *args, **kwargs):
    kwargs.update(
        {
            'servers': config['CACHE_MEMCACHED_SERVERS'],
            'key_prefix': config['CACHE_KEY_PREFIX']})

    return MemcachedCache(*args, **kwargs)


def saslmemcached(config, **kwargs):
    kwargs.update(
        {
            'servers': config['CACHE_MEMCACHED_SERVERS'],
            'username': config['CACHE_MEMCACHED_USERNAME'],
            'password': config['CACHE_MEMCACHED_PASSWORD'],
            'key_prefix': config['CACHE_KEY_PREFIX']})

    return SASLMemcachedCache(**kwargs)


def gaememcached(config, *args, **kwargs):
    kwargs.update({'key_prefix': config['CACHE_KEY_PREFIX']})
    return GAEMemcachedCache(*args, **kwargs)


def filesystem(config, *args, **kwargs):
    args = chain([config['CACHE_DIR']], args)
    kwargs.update({'threshold': config['CACHE_THRESHOLD']})
    return FileSystemCache(*args, **kwargs)


def redis(config, *args, **kwargs):
    from redis import from_url

    kwargs.update(
        {
            'host': config.get('CACHE_REDIS_HOST', 'localhost'),
            'port': config.get('CACHE_REDIS_PORT', 6379),
            'password': config.get('CACHE_REDIS_PASSWORD'),
            'key_prefix': config.get('CACHE_KEY_PREFIX'),
            'db': config.get('CACHE_REDIS_DB')})

    redis_url = config.get('CACHE_REDIS_URL')

    if redis_url:
        kwargs['host'] = from_url(redis_url, db=kwargs.pop('db', None))

    return RedisCache(*args, **kwargs)


class SpreadSASLMemcachedCache(SASLMemcachedCache):
    """
    Simple Subclass of SASLMemcached client that spread value across multiple
    key is they are bigger than a given threshhold.

    Spreading require using pickle to store the value, which can significantly
    impact the performances.
    """
    def __init__(self, *args, **kwargs):
        """
        chunksize : (int) max size in bytes of chunk stored in memcached
        """
        self.chunksize = kwargs.get('chunksize', 950000)
        self.maxchunk = kwargs.get('maxchunk', 32)
        super(SpreadSASLMemcachedCache, self).__init__(*args, **kwargs)

    def delete(self, key):
        for skey in self._genkeys(key):
            super(SpreadSASLMemcachedCache, self).delete(skey)

    def set(self, key, value, timeout=None, chunk=True):
        """set a value in cache, potentially spreding it across multiple key.

        chunk : (Bool) if set to false, does not try to spread across multiple
                key. this can be faster, but will fail if value is bigger than
                chunks, and require you to get back the object by specifying
                that it is not spread.

        """
        if chunk:
            return self._set(key, value, timeout=timeout)
        else:
            return super(SpreadSASLMemcachedCache, self).set(
                key, value, timeout=timeout)

    def _set(self, key, value, timeout=None):
        # TODO: fix this issues since <werkzeug.requests> is now removed
        # pickling/unpickling adds an overhead,
        # I didn't found a good way to avoid pickling/unpickling if
        # key is smaller than chunksize, because in case of <werkzeug.requests>
        # getting the length consume the data iterator.
        serialized = pickle.dumps(value, 2)
        values = {}
        len_ser = len(serialized)
        chunks = range_type(0, len_ser, self.chunksize)

        if len(chunks) > self.maxchunk:
            raise ValueError(
                'Cannot store value in less than %s keys' % (self.maxchunk))

        for i in chunks:
            values['%s.%s' % (key, i // self.chunksize)] = serialized[
                i:i + self.chunksize]

        super(SpreadSASLMemcachedCache, self).set_many(values, timeout)

    def get(self, key, chunk=True):
        """get a value in cache, potentially spread it across multiple keys.

        chunk : (Bool) if set to false, get a value set with
            set(..., chunk=False)
        """
        if chunk:
            return self._get(key)
        else:
            return super(SpreadSASLMemcachedCache, self).get(key)

    def _genkeys(self, key):
        return ['%s.%s' % (key, i) for i in range_type(self.maxchunk)]

    def _get(self, key):
        to_get = ['%s.%s' % (key, i) for i in range_type(self.maxchunk)]
        result = super(SpreadSASLMemcachedCache, self).get_many(*to_get)
        serialized = ''.join(v for v in result if v is not None)

        if not serialized:
            return None

        return pickle.loads(serialized)


def spreadsaslmemcachedcache(config, *args, **kwargs):
    kwargs.update(
        {
            'servers': config['CACHE_MEMCACHED_SERVERS'],
            'username': config.get('CACHE_MEMCACHED_USERNAME'),
            'password': config.get('CACHE_MEMCACHED_PASSWORD'),
            'key_prefix': config.get('CACHE_KEY_PREFIX')})

    return SpreadSASLMemcachedCache(*args, **kwargs)
