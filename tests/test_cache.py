# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab
"""
    tests.test_cache
    ~~~~~~~~~~~~~~~~~~~

    Provides unit tests.
"""
from __future__ import (
    absolute_import, division, print_function, unicode_literals)

import sys
import os
import time
import random

from subprocess import call

import nose.tools as nt

from mezmorize import Cache, function_namespace
from mezmorize.backends import (
    from_url, pylibmc, SimpleCache, FileSystemCache, RedisCache,
    MemcachedCache, SASLMemcachedCache, SpreadSASLMemcachedCache, TooBig)

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

pgrep = lambda process: call(['pgrep', process]) == 0
has_redis = lambda: from_url and pgrep('redis')
has_mc = lambda: pylibmc and pgrep('memcache')


class CacheTestCase(unittest.TestCase):
    def _get_config(self):
        return {'CACHE_TYPE': 'simple'}

    def test_dict_config(self):
        nt.assert_equal(self.cache.config['CACHE_TYPE'], 'simple')
        nt.assert_is_instance(self.cache.cache, SimpleCache)

    def setUp(self):
        self.config = self._get_config()
        self.cache = Cache(**self.config)
        self.func = None

    def tearDown(self):
        if self.func:
            self.cache.delete_memoized(self.func)

        self.cache = {}

    def test_000_set(self):
        self.cache.set('hi', 'hello')
        nt.assert_equal(self.cache.get('hi'), 'hello')

    def test_add(self):
        self.cache.add('hi', 'hello')
        nt.assert_equal(self.cache.get('hi'), 'hello')

        self.cache.add('hi', 'foobar')
        nt.assert_equal(self.cache.get('hi'), 'hello')

    def test_delete(self):
        self.cache.set('hi', 'hello')
        self.cache.delete('hi')
        nt.assert_is_none(self.cache.get('hi'))

    def test_memoize(self):
        @self.cache.memoize(5)
        def func(a, b):
            return a + b + random.randrange(0, 100000)

        self.func = func
        result = func(5, 2)
        time.sleep(1)
        nt.assert_equal(func(5, 2), result)

        result2 = func(5, 3)
        nt.assert_not_equal(result2, result)

        time.sleep(6)
        nt.assert_not_equal(func(5, 2), result)

        time.sleep(1)
        nt.assert_not_equal(func(5, 3), result2)

    def test_timeout(self):
        self.config['CACHE_DEFAULT_TIMEOUT'] = 1
        self.cache = Cache(**self.config)

        @self.cache.memoize(50)
        def func(a, b):
            return a + b + random.randrange(0, 100000)

        self.func = func
        result = func(5, 2)
        time.sleep(2)
        nt.assert_equal(func(5, 2), result)

    def test_delete_timeout(self):
        @self.cache.memoize(5)
        def func(a, b):
            return a + b + random.randrange(0, 100000)

        self.func = func
        result = func(5, 2)
        result2 = func(5, 3)
        time.sleep(1)

        nt.assert_equal(func(5, 2), result)
        nt.assert_equal(func(5, 2), result)
        nt.assert_not_equal(func(5, 3), result)
        nt.assert_equal(func(5, 3), result2)

        self.cache.delete_memoized(func)
        nt.assert_not_equal(func(5, 2), result)
        nt.assert_not_equal(func(5, 3), result2)

    def test_delete_verhash(self):
        @self.cache.memoize(5)
        def func(a, b):
            return a + b + random.randrange(0, 100000)

        self.func = func
        result = func(5, 2)
        result2 = func(5, 3)
        time.sleep(1)

        nt.assert_equal(func(5, 2), result)
        nt.assert_equal(func(5, 2), result)
        nt.assert_not_equal(func(5, 3), result)
        nt.assert_equal(func(5, 3), result2)

        fname = function_namespace(func)[0]
        version_key = self.cache._memvname(fname)
        nt.assert_is_not_none(self.cache.get(version_key))

        self.cache.delete_memoized_verhash(func)
        nt.assert_is_none(self.cache.get(version_key))
        nt.assert_not_equal(func(5, 2), result)
        nt.assert_not_equal(func(5, 3), result2)
        nt.assert_is_not_none(self.cache.get(version_key))

    def test_delete_rand(self):
        @self.cache.memoize()
        def func(a, b):
            return a + b + random.randrange(0, 100000)

        self.func = func
        result_a = func(5, 1)
        result_b = func(5, 2)
        nt.assert_equal(func(5, 1), result_a)
        nt.assert_equal(func(5, 2), result_b)

        self.cache.delete_memoized(func, 5, 2)
        nt.assert_equal(func(5, 1), result_a)
        nt.assert_not_equal(func(5, 2), result_b)

    def test_args(self):
        @self.cache.memoize()
        def func(a, b):
            return sum(a) + sum(b) + random.randrange(0, 100000)

        self.func = func
        result_a = func([5, 3, 2], [1])
        result_b = func([3, 3], [3, 1])
        nt.assert_equal(func([5, 3, 2], [1]), result_a)
        nt.assert_equal(func([3, 3], [3, 1]), result_b)

        self.cache.delete_memoized(func, [5, 3, 2], [1])
        nt.assert_not_equal(func([5, 3, 2], [1]), result_a)
        nt.assert_equal(func([3, 3], [3, 1]), result_b)

    def test_kwargs(self):
        @self.cache.memoize()
        def func(a, b=None):
            return a + sum(b.values()) + random.randrange(0, 100000)

        self.func = func
        result_a = func(1, {'one': 1, 'two': 2})
        result_b = func(5, {'three': 3, 'four': 4})
        nt.assert_equal(func(1, {'one': 1, 'two': 2}), result_a)
        nt.assert_equal(func(5, {'three': 3, 'four': 4}), result_b)

        self.cache.delete_memoized(func, 1, {'one': 1, 'two': 2})
        nt.assert_not_equal(func(1, {'one': 1, 'two': 2}), result_a)
        nt.assert_equal(func(5, {'three': 3, 'four': 4}), result_b)

    def test_kwargonly(self):
        @self.cache.memoize()
        def func(a=None):
            if a is None:
                a = 0
            return a + random.random()

        self.func = func
        result_a = func()
        result_b = func(5)

        nt.assert_equal(func(), result_a)
        nt.assert_less(func(), 1)
        nt.assert_equal(func(5), result_b)
        nt.assert_greater_equal(func(5), 5)
        nt.assert_less(func(5), 6)

    def test_arg_kwarg(self):
        @self.cache.memoize()
        def func(a, b, c=1):
            return a + b + c + random.randrange(0, 100000)

        self.func = func
        nt.assert_equal(func(1, 2), func(1, 2, c=1))
        nt.assert_equal(func(1, 2), func(1, 2, 1))
        nt.assert_equal(func(1, 2), func(1, 2))
        nt.assert_not_equal(func(1, 2, 3), func(1, 2))

        with nt.assert_raises(TypeError):
            func(1)

    def test_classarg(self):
        @self.cache.memoize()
        def func(a):
            return a.value + random.random()

        class Adder(object):
            def __init__(self, value):
                self.value = value

        self.func = func
        adder = Adder(15)
        adder2 = Adder(20)

        y = func(adder)
        z = func(adder2)
        nt.assert_not_equal(y, z)
        nt.assert_equal(func(adder), y)
        nt.assert_not_equal(func(adder), z)

        adder.value = 14
        nt.assert_equal(func(adder), y)
        nt.assert_not_equal(func(adder), z)
        nt.assert_not_equal(func(adder), func(adder2))
        nt.assert_equal(func(adder2), z)

    def test_classfunc(self):
        class Adder(object):
            def __init__(self, initial):
                self.initial = initial

            @self.cache.memoize()
            def add(self, b):
                return self.initial + b

        self.func = Adder.add
        adder1 = Adder(1)
        adder2 = Adder(2)

        x = adder1.add(3)
        nt.assert_equal(adder1.add(3), x)
        nt.assert_not_equal(adder1.add(4), x)
        nt.assert_not_equal(adder1.add(3), adder2.add(3))

    def test_delete_classfunc(self):
        class Adder(object):
            def __init__(self, initial):
                self.initial = initial

            @self.cache.memoize()
            def add(self, b):
                return self.initial + b + random.random()

        self.func = Adder.add
        adder1 = Adder(1)
        adder2 = Adder(2)

        a1 = adder1.add(3)
        a2 = adder2.add(3)
        nt.assert_not_equal(a1, a2)
        nt.assert_equal(adder1.add(3), a1)
        nt.assert_equal(adder2.add(3), a2)

        self.cache.delete_memoized(adder1.add)
        a3 = adder1.add(3)
        a4 = adder2.add(3)

        nt.assert_not_equal(a1, a3)
        nt.assert_not_equal(a1, a3)
        nt.assert_equal(a2, a4)

        self.cache.delete_memoized(Adder.add)
        a5 = adder1.add(3)
        a6 = adder2.add(3)

        nt.assert_not_equal(a5, a6)
        nt.assert_not_equal(a3, a5)
        nt.assert_not_equal(a4, a6)

    def test_delete_classmethod(self):
        class Mock(object):
            @classmethod
            @self.cache.memoize(5)
            def func(cls, a, b):
                return a + b + random.randrange(0, 100000)

        self.func = Mock.func
        result = Mock.func(5, 2)
        result2 = Mock.func(5, 3)
        time.sleep(1)

        nt.assert_equal(Mock.func(5, 2), result)
        nt.assert_equal(Mock.func(5, 2), result)
        nt.assert_not_equal(Mock.func(5, 3), result)
        nt.assert_equal(Mock.func(5, 3), result2)

        self.cache.delete_memoized(Mock.func)
        nt.assert_not_equal(Mock.func(5, 2), result)
        nt.assert_not_equal(Mock.func(5, 3), result2)

    def test_multiple_arg_kwarg_calls(self):
        @self.cache.memoize()
        def func(a, b, c=[1, 1], d=[1, 1]):
            rand = random.randrange(0, 100000)
            return sum(a) + sum(b) + sum(c) + sum(d) + rand

        self.func = func
        expected = func([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        nt.assert_equal(func([5, 3, 2], [1], d=[3, 3], c=[3, 3]), expected)

        result = func(b=[1], a=[5, 3, 2], c=[3, 3], d=[3, 3])
        nt.assert_equal(result, expected)
        nt.assert_equal(func([5, 3, 2], [1], [3, 3], [3, 3]), expected)

    def test_delete_multiple_arg_kwarg(self):
        @self.cache.memoize()
        def func(a, b, c=[1, 1], d=[1, 1]):
            rand = random.randrange(0, 100000)
            return sum(a) + sum(b) + sum(c) + sum(d) + rand

        self.func = func
        result_a = func([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        self.cache.delete_memoized(func, [5, 3, 2], [1], [3, 3], [3, 3])
        result_b = func([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        nt.assert_not_equal(result_a, result_b)

        self.cache.delete_memoized(
            func, [5, 3, 2], b=[1], c=[3, 3], d=[3, 3])
        result_b = func([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        nt.assert_not_equal(result_a, result_b)

        self.cache.delete_memoized(func, [5, 3, 2], [1], c=[3, 3], d=[3, 3])
        result_a = func([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        nt.assert_not_equal(result_a, result_b)

        self.cache.delete_memoized(
            func, [5, 3, 2], b=[1], c=[3, 3], d=[3, 3])
        result_a = func([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        nt.assert_not_equal(result_a, result_b)

        self.cache.delete_memoized(func, [5, 3, 2], [1], c=[3, 3], d=[3, 3])
        result_b = func([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        nt.assert_not_equal(result_a, result_b)

        self.cache.delete_memoized(func, [5, 3, 2], [1], [3, 3], [3, 3])
        result_a = func([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        nt.assert_not_equal(result_a, result_b)

    def test_kwargs_to_args(self):
        def func(a, b, c=None, d=None):
            return sum(a) + sum(b) + random.randrange(0, 100000)

        self.func = func
        expected = (1, 2, 'foo', 'bar')

        args, kwargs = self.cache._memoize_kwargs_to_args(
            func, 1, 2, 'foo', 'bar')
        nt.assert_equal(args, expected)

        args, kwargs = self.cache._memoize_kwargs_to_args(
            func, 2, 'foo', 'bar', a=1)
        nt.assert_equal(args, expected)

        args, kwargs = self.cache._memoize_kwargs_to_args(
            func, a=1, b=2, c='foo', d='bar')
        nt.assert_equal(args, expected)

        args, kwargs = self.cache._memoize_kwargs_to_args(
            func, d='bar', b=2, a=1, c='foo')
        nt.assert_equal(args, expected)

        args, kwargs = self.cache._memoize_kwargs_to_args(
            func, 1, 2, d='bar', c='foo')
        nt.assert_equal(args, expected)


class NSCacheTestCase(unittest.TestCase):
    def _get_config(self):
        return {'CACHE_TYPE': 'simple'}

    def setUp(self):
        self.config = self._get_config()
        self.namespace = 'https://github.com/reubano/mezmorize'
        self.cache = Cache(namespace=self.namespace, **self.config)

    def tearDown(self):
        self.cache = {}

    def test_memoize(self):
        def func(a, b):
            return a + b + random.randrange(0, 100000)

        cache_key1 = self.cache._memoize_make_cache_key()(func)
        nt.assert_equal(cache_key1, 'VKQlyaJ2Pm8xCZ8bHmhhp1')

        cache = Cache(namespace=self.namespace, **self.config)
        cache_key2 = cache._memoize_make_cache_key()(func)
        nt.assert_equal(cache_key2, 'VKQlyaJ2Pm8xCZ8bHmhhp1')


class FileSystemCacheTestCase(CacheTestCase):
    def _get_config(self):
        return {'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'}

    def test_dict_config(self):
        nt.assert_equal(self.cache.config['CACHE_TYPE'], 'filesystem')
        nt.assert_is_instance(self.cache.cache, FileSystemCache)

if has_mc():
    class MemcachedCacheTestCase(CacheTestCase):
        def _get_config(self):
            return {
                'CACHE_TYPE': 'memcached',
                'CACHE_MEMCACHED_SERVERS': ['localhost:11211']}

        def test_dict_config(self):
            nt.assert_equal(self.cache.config['CACHE_TYPE'], 'memcached')
            nt.assert_is_instance(self.cache.cache, MemcachedCache)

        def test_mc_large_value(self):
            cache = Cache(**self.config)

            with nt.assert_raises(TooBig):
                cache.set('big', range(1000000))

            nt.assert_is_none(cache.get('big'))

if has_mc():
    class SASLMemcachedCacheTestCase(CacheTestCase):
        def _get_config(self):
            return {
                'CACHE_TYPE': 'saslmemcached',
                'CACHE_MEMCACHED_SERVERS': ['localhost:11211'],
                'CACHE_MEMCACHED_USERNAME': None,
                'CACHE_MEMCACHED_PASSWORD': None}

        def test_dict_config(self):
            nt.assert_equal(self.cache.config['CACHE_TYPE'], 'saslmemcached')
            nt.assert_is_instance(self.cache.cache, SASLMemcachedCache)

        def test_mc_large_value(self):
            cache = Cache(**self.config)

            with nt.assert_raises(TooBig):
                cache.set('big', range(1000000))

            nt.assert_is_none(cache.get('big'))

if has_mc():
    class SpreadSASLMemcachedCacheTestCase(CacheTestCase):
        def _get_config(self):
            return {
                'CACHE_TYPE': 'spreadsaslmemcachedcache',
                'CACHE_MEMCACHED_SERVERS': ['localhost:11211']}

        def test_dict_config(self):
            CACHE_TYPE = self.cache.config['CACHE_TYPE']
            nt.assert_equal(CACHE_TYPE, 'spreadsaslmemcachedcache')
            nt.assert_is_instance(self.cache.cache, SpreadSASLMemcachedCache)

        def test_mc_large_value(self):
            cache = Cache(**self.config)
            cache.set('big', range(1000000))
            nt.assert_equal(len(cache.get('big')), 1000000)

            cache.delete('big')
            nt.assert_is_none(cache.get('big'))
else:
    print('requires Memcache')

if has_redis():
    class RedisCacheTestCase(CacheTestCase):
        def _get_config(self):
            return {
                'CACHE_TYPE': 'redis',
                'CACHE_REDIS_URL': 'redis://localhost:6379'}

        def test_dict_config(self):
            nt.assert_equal(self.cache.config['CACHE_TYPE'], 'redis')
            nt.assert_is_instance(self.cache.cache, RedisCache)

        def test_redis_url_default_db(self):
            client = self.cache.cache._client
            rconn = client.connection_pool.get_connection('foo')
            nt.assert_equal(rconn.db, 0)

        def test_redis_url_custom_db(self):
            self.config.update({'CACHE_REDIS_URL': 'redis://localhost:6379/2'})
            cache = Cache(**self.config)
            rconn = cache.cache._client.connection_pool.get_connection('foo')
            nt.assert_equal(rconn.db, 2)
else:
    print('requires Redis')

if __name__ == '__main__':
    unittest.main()
