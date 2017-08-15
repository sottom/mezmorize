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
    NullCache, SimpleCache, MemcachedCache as _MemcachedCache, FileSystemCache,
    RedisCache)

from .utils import DEF_SERVERS, IS_PY3, HAS_MEMCACHE

try:
    from redis import from_url
except ImportError:
    from_url = None

CONFIG_LOOKUP = {
    'servers': 'CACHE_MEMCACHED_SERVERS',
    'username': 'CACHE_MEMCACHED_USERNAME',
    'password': 'CACHE_MEMCACHED_PASSWORD',
    'key_prefix': 'CACHE_KEY_PREFIX'}


def gen_config_items(*keys, **config):
    for key in keys:
        yield (key, config[CONFIG_LOOKUP[key]])


def get_mc_client(servers=(DEF_SERVERS,), **kwargs):
    from pylibmc import Client
    timeout = kwargs.pop('timeout', None)

    try:
        from pylibmc import TooBig
    except ImportError:
        from pylibmc import Error, ServerError
        TooBig = (Error, ServerError)

    binary = kwargs.pop('binary', True)

    if timeout:
        kwargs['behaviors'] = {'connect_timeout': timeout}

    client = Client(servers, binary=binary, **kwargs)
    client.TooBig = TooBig
    return client


class MemcachedCache(_MemcachedCache):
    def __init__(self, *args, **kwargs):
        default_timeout = kwargs.pop('default_timeout', 300)
        key_prefix = kwargs.pop('key_prefix', None)

        if not HAS_MEMCACHE:
            raise RuntimeError('No memcache module found.')

        client = get_mc_client(**kwargs)
        skwargs = {'default_timeout': default_timeout, 'key_prefix': key_prefix}
        super(MemcachedCache, self).__init__(servers=client, **skwargs)
        self.TooBig = client.TooBig


class SASLMemcachedCache(MemcachedCache):
    def __init__(self, *args, **kwargs):
        super(SASLMemcachedCache, self).__init__(*args, **kwargs)


def null(config, *args, **kwargs):
    return NullCache()


def simple(config, *args, **kwargs):
    kwargs.update({'threshold': config['CACHE_THRESHOLD']})
    return SimpleCache(*args, **kwargs)


def memcached(config, *args, **kwargs):
    config_items = gen_config_items('servers', 'key_prefix', **config)
    kwargs.update(dict(config_items))
    return MemcachedCache(*args, **kwargs)


def saslmemcached(config, **kwargs):
    keys = ('servers', 'username', 'password', 'key_prefix')
    config_items = gen_config_items(*keys, **config)
    kwargs.update(dict(config_items))
    return SASLMemcachedCache(**kwargs)


def filesystem(config, *args, **kwargs):
    args = chain([config['CACHE_DIR']], args)
    kwargs.update({'threshold': config['CACHE_THRESHOLD']})
    return FileSystemCache(*args, **kwargs)


def redis(config, *args, **kwargs):
    kwargs.setdefault('host', config.get('CACHE_REDIS_HOST', 'localhost'))
    kwargs.setdefault('port', config.get('CACHE_REDIS_PORT', 6379))
    kwargs.setdefault('password', config.get('CACHE_REDIS_PASSWORD'))
    kwargs.setdefault('key_prefix', config.get('CACHE_KEY_PREFIX'))
    kwargs.setdefault('db', config.get('CACHE_REDIS_DB'))
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
                default: 1048448)
        """
        self.CHUNKSIZE = kwargs.get('chunksize', 1048448)
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
        except self.TooBig:
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
            filtered = (v for v in result if v is not None)

            if IS_PY3:
                serialized = b''.join(filtered)
            else:
                serialized = ''.join(filtered)

            value = pickle.loads(serialized) if serialized else None

        return value


def spreadsaslmemcached(config, *args, **kwargs):
    keys = ('servers', 'username', 'password', 'key_prefix')
    config_items = gen_config_items(*keys, **config)
    kwargs.update(dict(config_items))
    return SpreadSASLMemcachedCache(*args, **kwargs)
