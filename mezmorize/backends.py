#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab
"""
    mezmorize.backends
    ~~~~~~~~~~~~~~~~~~

    Provides mezmorize storage mechanisms
"""
import pickle

from itertools import chain
from werkzeug.contrib.cache import (
    BaseCache, NullCache, SimpleCache, MemcachedCache, FileSystemCache,
    RedisCache)

try:
    import pylibmc
except ImportError:
    TooBig = pylibmc = None
else:
    from pylibmc import TooBig

try:
    from redis import from_url
except ImportError:
    from_url = None


class SASLMemcachedCache(MemcachedCache):
    def __init__(self, **kwargs):
        servers = kwargs.pop('servers', None) or ['127.0.0.1:11211']
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


def filesystem(config, *args, **kwargs):
    args = chain([config['CACHE_DIR']], args)
    kwargs.update({'threshold': config['CACHE_THRESHOLD']})
    return FileSystemCache(*args, **kwargs)


def redis(config, *args, **kwargs):
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
    key is they are bigger than a given threshold.

    Spreading require using pickle to store the value, which can significantly
    impact the performances.
    """
    def __init__(self, *args, **kwargs):
        """
        Kwargs:
            chunksize (int): max length of a pickled object that can fit in
                memcached (memcache has an upper limit of 1MB for values,
                default: 1024000)
        """
        self.CHUNKSIZE = kwargs.get('chunksize', 1024000)
        self.MARKER = 'SpreadSASLMemcachedCache.SpreadedValue'
        self.maxchunks = kwargs.get('maxchunks', 32)
        super(SpreadSASLMemcachedCache, self).__init__(*args, **kwargs)
        self.super = super(SpreadSASLMemcachedCache, self)

    def _genkeys(self, key):
        return ('{}.{}'.format(key, i) for i in range(self.maxchunks))

    def _gen_kv(self, key, pickled):
        chunks = range(0, len(pickled), self.CHUNKSIZE)

        if len(chunks) > self.maxchunks:
            msg = 'Value exceed maximum number of keys ({})'
            raise ValueError(msg.format(self.maxchunks))

        for i in chunks:
            _key = '{}.{}'.format(key, i // self.CHUNKSIZE)
            _value = pickled[i:i + self.CHUNKSIZE]
            yield _key, _value

    def delete(self, key):
        value = self.super.get(key)
        self.super.delete(key)

        if value == self.MARKER:
            for skey in self._genkeys(key):
                self.super.delete(skey)

    def set(self, key, value, timeout=None):
        """set a value in cache, potentially spreading it across multiple key.
        """
        try:
            value = self.super.set(key, value, timeout=timeout)
        except TooBig:
            self.super.set(key, self.MARKER, timeout=timeout)
            pickled = pickle.dumps(value, 2)
            values = dict(self._gen_kv(key, pickled))
            value = self.super.set_many(values, timeout)

        return value

    def get(self, key):
        """get a value in cache, potentially from multiple keys.
        """
        value = self.super.get(key)

        if value == self.MARKER:
            keys = self._genkeys(key)
            result = self.super.get_many(*keys)
            serialized = ''.join(v for v in result if v is not None)
            value = pickle.loads(serialized) if serialized else None

        return value


def spreadsaslmemcachedcache(config, *args, **kwargs):
    kwargs.update(
        {
            'servers': config['CACHE_MEMCACHED_SERVERS'],
            'username': config.get('CACHE_MEMCACHED_USERNAME'),
            'password': config.get('CACHE_MEMCACHED_PASSWORD'),
            'key_prefix': config.get('CACHE_KEY_PREFIX')})

    return SpreadSASLMemcachedCache(*args, **kwargs)
