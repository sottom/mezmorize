# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab
"""
    tests.test_cache
    ~~~~~~~~~~~~~~~~

    Provides unit tests.
"""
from __future__ import absolute_import, division, print_function

import time
import random

import nose.tools as nt

from mezmorize import Cache, function_namespace
from mezmorize.utils import HAS_MEMCACHE, HAS_REDIS, get_cache_config

from mezmorize.backends import (
    SimpleCache, FileSystemCache, RedisCache, MemcachedCache,
    SASLMemcachedCache, SpreadSASLMemcachedCache, AVAIL_MEMCACHES)

BIGINT = 2 ** 21
BIGGERINT = 2 ** 28


def setup_func(*args, **kwargs):
    namespace = kwargs.pop('namespace', None)
    client_name = kwargs.pop('client_name', None)

    if client_name:
        CACHE_OPTIONS = kwargs.get('CACHE_OPTIONS', {})
        CACHE_OPTIONS['preferred_memcache'] = client_name
        kwargs['CACHE_OPTIONS'] = CACHE_OPTIONS

    config = get_cache_config(*args, **kwargs)
    cache = Cache(namespace=namespace, **config)
    return cache


def check_cache_type(cache, cache_type):
    nt.assert_equal(cache.config['CACHE_TYPE'], cache_type)


def check_cache_instance(cache, cache_instance):
    nt.assert_is_instance(cache.cache, cache_instance)


def check_client_name(cache, expected):
    nt.assert_equal(cache.cache.client_name, expected)


def check_too_big(cache, times, error=None):
    if cache.cache.TooBig:
        with nt.assert_raises(error or cache.cache.TooBig):
            cache.set('big', 'a' * times)

        nt.assert_is_none(cache.get('big'))
    else:
        cache.set('big', 'a' * times)
        nt.assert_equal(cache.get('big'), 'a' * times)


def check_set_delete(cache, key, value, multiplier=None):
    if multiplier:
        value *= multiplier

    cache.set(key, value)
    nt.assert_equal(cache.get(key), value)

    cache.delete(key)
    nt.assert_is_none(cache.get(key))


class TestCache(object):
    def setup(self):
        self.cache = setup_func('simple')

    def teardown(self):
        self.cache.clear()

    def test_dict_config(self):
        check_cache_type(self.cache, 'simple')
        check_cache_instance(self.cache, SimpleCache)

    def test_000_set(self):
        self.cache.set('hi', 'hello')
        nt.assert_equal(self.cache.get('hi'), 'hello')

    def test_add(self):
        self.cache.add('hi', 'hello')
        nt.assert_equal(self.cache.get('hi'), 'hello')

        self.cache.add('hi', 'foobar')
        nt.assert_equal(self.cache.get('hi'), 'hello')

    def test_add_unicode(self):
        self.cache.add('ȟį', 'ƕɛĺłö')
        nt.assert_equal(self.cache.get('ȟį'), 'ƕɛĺłö')

        self.cache.add('ȟį', 'fööƀåř')
        nt.assert_equal(self.cache.get('ȟį'), 'ƕɛĺłö')

    def test_delete(self):
        check_set_delete(self.cache, 'hi', 'hello')

    def test_delete_unicode(self):
        check_set_delete(self.cache, 'ȟį', 'ƕɛĺłö')

    def test_memoize(self):
        @self.cache.memoize(5)
        def func(a, b):
            return a + b + random.randrange(0, 100000)

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
        config = get_cache_config('simple', CACHE_DEFAULT_TIMEOUT=1)
        self.cache = Cache(**config)

        @self.cache.memoize(50)
        def func(a, b):
            return a + b + random.randrange(0, 100000)

        result = func(5, 2)
        time.sleep(2)
        nt.assert_equal(func(5, 2), result)

    def test_delete_timeout(self):
        @self.cache.memoize(5)
        def func(a, b):
            return a + b + random.randrange(0, 100000)

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

        expected = (1, 2, 'foo', 'bar')

        args = self.cache._gen_args(func, 1, 2, 'foo', 'bar')
        nt.assert_equal(tuple(args), expected)

        args = self.cache._gen_args(func, 2, 'foo', 'bar', a=1)
        nt.assert_equal(tuple(args), expected)

        args = self.cache._gen_args(func, a=1, b=2, c='foo', d='bar')
        nt.assert_equal(tuple(args), expected)

        args = self.cache._gen_args(func, d='bar', b=2, a=1, c='foo')
        nt.assert_equal(tuple(args), expected)

        args = self.cache._gen_args(func, 1, 2, d='bar', c='foo')
        nt.assert_equal(tuple(args), expected)


class TestNSCache(object):
    def setup(self):
        self.namespace = 'https://github.com/reubano/mezmorize'
        self.cache = setup_func('simple', namespace=self.namespace)

    def teardown(self):
        self.cache.clear()

    def test_memoize(self):
        def func(a, b):
            return a + b + random.randrange(0, 100000)

        config = get_cache_config('simple')
        cache = Cache(namespace=self.namespace, **config)
        cache_key1 = self.cache._memoize_make_cache_key()(func)
        cache_key2 = cache._memoize_make_cache_key()(func)
        nt.assert_equal(cache_key1, cache_key2)


class TestFileSystemCache(TestCache):
    def setup(self):
        self.cache = setup_func('filesystem', CACHE_DIR='/tmp')

    def teardown(self):
        self.cache.clear()

    def test_dict_config(self):
        check_cache_type(self.cache, 'filesystem')
        check_cache_instance(self.cache, FileSystemCache)


if HAS_MEMCACHE:
    class TestMemcachedCache(TestCache):
        def setup(self, client_name=None):
            self.cache = setup_func('memcached', client_name=client_name)

        def teardown(self):
            self.cache.clear()

        def test_dict_config(self):
            for client_name in AVAIL_MEMCACHES:
                self.setup(client_name=client_name)
                yield check_cache_type, self.cache, 'memcached'
                yield check_cache_instance, self.cache, MemcachedCache
                yield check_client_name, self.cache, client_name
                self.teardown()

        def test_mc_large_value(self):
            for client_name in AVAIL_MEMCACHES:
                self.setup(client_name=client_name)
                yield check_too_big, self.cache, BIGINT
                self.teardown()

else:
    print('TestMemcachedCache requires Memcache')

if HAS_MEMCACHE:
    class TestSASLMemcachedCache(TestCache):
        def setup(self, client_name=None):
            self.cache = setup_func('saslmemcached', client_name=client_name)

        def teardown(self):
            self.cache.clear()

        def test_dict_config(self):
            for client_name in AVAIL_MEMCACHES:
                self.setup(client_name=client_name)
                yield check_cache_type, self.cache, 'saslmemcached'
                yield check_cache_instance, self.cache, SASLMemcachedCache
                self.teardown()

        def test_mc_large_value(self):
            for client_name in AVAIL_MEMCACHES:
                self.setup(client_name=client_name)
                yield check_too_big, self.cache, BIGINT
                self.teardown()

else:
    print('TestSASLMemcachedCache requires Memcache')

if HAS_MEMCACHE:
    class TestSpreadSASLMemcachedCache(TestCache):
        def setup(self, client_name=None):
            cache_type = 'spreadsaslmemcached'
            self.cache = setup_func(cache_type, client_name=client_name)

        def teardown(self):
            self.cache.clear()

        def test_dict_config(self):
            for client_name in AVAIL_MEMCACHES:
                self.setup(client_name=client_name)
                cache_instance = SpreadSASLMemcachedCache
                yield check_cache_type, self.cache, 'spreadsaslmemcached'
                yield check_cache_instance, self.cache, cache_instance
                self.teardown()

        def test_mc_large_value(self):
            for client_name in AVAIL_MEMCACHES:
                self.setup(client_name=client_name)
                yield check_set_delete, self.cache, 'big', 'a', BIGINT
                yield check_too_big, self.cache, BIGGERINT, ValueError
                self.teardown()
else:
    print('TestSpreadSASLMemcachedCache requires Memcache')

if HAS_REDIS:
    class TestRedisCache(TestCache):
        def setup(self, db=0):
            self.cache = setup_func('redis', db=db)
            self.client = self.cache.cache._client

        def teardown(self):
            self.cache.clear()

        def test_dict_config(self):
            check_cache_type(self.cache, 'redis')
            check_cache_instance(self.cache, RedisCache)

        def test_redis_url_default_db(self):
            rconn = self.client.connection_pool.get_connection('foo')
            nt.assert_equal(rconn.db, 0)

        def test_redis_url_custom_db(self):
            self.setup(db=2)
            rconn = self.client.connection_pool.get_connection('foo')
            nt.assert_equal(rconn.db, 2)
else:
    print('TestRedisCache requires Redis')

if __name__ == '__main__':
    nt.main()
