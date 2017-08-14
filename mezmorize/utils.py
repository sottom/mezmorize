#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab
"""
    mezmorize.utils
    ~~~~~~~~~~~~~~~

    Provides mezmorize utility functions
"""
import sys

from os import getenv
from subprocess import call
from copy import copy

try:
    import pylibmc
except ImportError:
    pylibmc = None

try:
    import pymemcache
except ImportError:
    pymemcache = None

try:
    import bmemcached
except ImportError:
    bmemcached = None

try:
    import redis
except ImportError:
    redis = None

IS_PY3 = sys.version_info.major == 3

ALL_MEMCACHES = (
    ('pylibmc', pylibmc), ('pymemcache', pymemcache),
    ('bmemcached', bmemcached))

DEF_SERVERS = '127.0.0.1:11211'
MEMOIZE_DEFAULTS = {'CACHE_THRESHOLD': 2048, 'CACHE_DEFAULT_TIMEOUT': 3600}
MC_SERVERS = getenv('MEMCACHIER_SERVERS') or getenv('MEMCACHEDCLOUD_SERVERS')
MC_SERVERS = MC_SERVERS or getenv('MEMCACHE_SERVERS') or DEF_SERVERS
MC_USERNAME = getenv('MEMCACHIER_USERNAME') or getenv('MEMCACHEDCLOUD_USERNAME')
MC_PASSWORD = getenv('MEMCACHIER_PASSWORD') or getenv('MEMCACHEDCLOUD_PASSWORD')

REDIS_HOST = getenv('REDIS_PORT_6379_TCP_ADDR', 'localhost')
DEF_REDIS_URL = 'redis://{}:6379'.format(REDIS_HOST)
REDIS_URL = getenv('REDIS_URL') or getenv('REDISTOGO_URL') or DEF_REDIS_URL

CACHE_CONFIGS = {
    'simple': {'CACHE_TYPE': 'simple'},
    'filesystem': {
        'CACHE_TYPE': 'filesystem',
        'CACHE_DIR': getenv('CACHE_DIR')
    },
    'memcached': {
        'CACHE_TYPE': 'memcached',
        'CACHE_MEMCACHED_SERVERS': [MC_SERVERS]
    },
    'saslmemcached': {
        'CACHE_TYPE': 'saslmemcached',
        'CACHE_MEMCACHED_SERVERS': [MC_SERVERS],
        'CACHE_MEMCACHED_USERNAME': MC_USERNAME,
        'CACHE_MEMCACHED_PASSWORD': MC_PASSWORD
    },
    'spreadsaslmemcached': {
        'CACHE_TYPE': 'spreadsaslmemcached',
        'CACHE_MEMCACHED_SERVERS': [MC_SERVERS],
        'CACHE_MEMCACHED_USERNAME': MC_USERNAME,
        'CACHE_MEMCACHED_PASSWORD': MC_PASSWORD
    },
    'redis': {'CACHE_TYPE': 'redis', 'CACHE_REDIS_URL': REDIS_URL}
}


HEROKU_PROCESSES = {
    'postgres': ['DATABASE_URL'],
    'redis': ['REDIS_URL', 'REDISTOGO_URL'],
    'memcache': ['MEMCACHIER_SERVERS', 'MEMCACHE_SERVERS'],
}


def pgrep(process):
    envs = HEROKU_PROCESSES.get(process, [])
    any_env = any(map(getenv, envs))
    return any_env or call(['pgrep', process]) == 0


HAS_MEMCACHE = (pylibmc or pymemcache or bmemcached) and pgrep('memcache')
AVAIL_MEMCACHES = {k for k, v in ALL_MEMCACHES if HAS_MEMCACHE and v}
HAS_REDIS = redis and pgrep('redis')


def get_cache_type(spread=False):
    if HAS_MEMCACHE and MC_USERNAME and spread:
        cache_type = 'spreadsaslmemcached'
    elif HAS_MEMCACHE and MC_USERNAME:
        cache_type = 'saslmemcached'
    elif HAS_MEMCACHE:
        cache_type = 'memcached'
    elif HAS_REDIS:
        cache_type = 'redis'
    elif getenv('CACHE_DIR'):
        cache_type = 'filesystem'
    else:
        cache_type = 'simple'

    return cache_type


def get_cache_config(cache_type, db=None, **kwargs):
    config = copy(CACHE_CONFIGS[cache_type])
    redis_url = config.get('CACHE_REDIS_URL')

    if db and redis_url:
        config['CACHE_REDIS_URL'] = '{}/{}'.format(redis_url, db)

    [kwargs.setdefault(k, v) for k, v in MEMOIZE_DEFAULTS.items()]
    config.update(kwargs)
    return config


def get_pylibmc_client(servers, timeout=None, binary=True, **kwargs):
    from pylibmc import Client

    try:
        from pylibmc import TooBig
    except ImportError:
        from pylibmc import Error, ServerError
        TooBig = (Error, ServerError)

    if timeout:
        kwargs['behaviors'] = {'connect_timeout': timeout}

    client = Client(servers, binary=binary, **kwargs)
    client.TooBig = TooBig
    return client


def get_pymemcache_client(servers, timeout=None, **kwargs):
    from pymemcache.client.hash import HashClient

    from pymemcache.serde import (
        python_memcache_serializer, python_memcache_deserializer)

    kwargs.setdefault('serializer', python_memcache_serializer)
    kwargs.setdefault('deserializer', python_memcache_deserializer)

    if timeout:
        kwargs['timeout'] = timeout

    split = [s.split(':') for s in servers]
    _servers = [(host, int(port)) for host, port in split]
    client = HashClient(_servers, **kwargs)

    try:
        client.TooBig = ConnectionResetError
    except NameError:
        import socket
        client.TooBig = socket.error

    return client


def get_bmemcached_client(servers, timeout=None, **kwargs):
    from bmemcached import Client

    if timeout:
        kwargs['socket_timeout'] = timeout

    client = Client(servers, **kwargs)
    client.TooBig = None
    return client
