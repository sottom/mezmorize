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

from mezmorize import Cache, function_namespace

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest


class CacheTestCase(unittest.TestCase):
    def _get_config(self):
        return {'CACHE_TYPE': 'simple'}

    def setUp(self):
        self.config = self._get_config()
        self.cache = Cache(**self.config)

    def tearDown(self):
        self.cache = {}

    def test_00_set(self):
        self.cache.set('hi', 'hello')
        assert self.cache.get('hi') == 'hello'

    def test_01_add(self):
        self.cache.add('hi', 'hello')
        assert self.cache.get('hi') == 'hello'

        self.cache.add('hi', 'foobar')
        assert self.cache.get('hi') == 'hello'

    def test_02_delete(self):
        self.cache.set('hi', 'hello')
        self.cache.delete('hi')
        assert self.cache.get('hi') is None

    def test_06_memoize(self):
        @self.cache.memoize(5)
        def big_foo(a, b):
            return a + b + random.randrange(0, 100000)

        result = big_foo(5, 2)
        time.sleep(1)
        assert big_foo(5, 2) == result

        result2 = big_foo(5, 3)
        assert result2 != result

        time.sleep(6)
        assert big_foo(5, 2) != result

        time.sleep(1)
        assert big_foo(5, 3) != result2

    def test_06a_memoize(self):
        self.config['CACHE_DEFAULT_TIMEOUT'] = 1
        self.cache = Cache(**self.config)

        @self.cache.memoize(50)
        def big_foo(a, b):
            return a + b + random.randrange(0, 100000)

        result = big_foo(5, 2)
        time.sleep(2)
        assert big_foo(5, 2) == result

    def test_07_delete_memoize(self):
        @self.cache.memoize(5)
        def big_foo(a, b):
            return a + b + random.randrange(0, 100000)

        result = big_foo(5, 2)
        result2 = big_foo(5, 3)

        time.sleep(1)

        assert big_foo(5, 2) == result
        assert big_foo(5, 2) == result
        assert big_foo(5, 3) != result
        assert big_foo(5, 3) == result2

        self.cache.delete_memoized(big_foo)
        assert big_foo(5, 2) != result
        assert big_foo(5, 3) != result2

    def test_07b_delete_memoized_verhash(self):
        @self.cache.memoize(5)
        def big_foo(a, b):
            return a + b + random.randrange(0, 100000)

        result = big_foo(5, 2)
        result2 = big_foo(5, 3)

        time.sleep(1)
        assert big_foo(5, 2) == result
        assert big_foo(5, 2) == result
        assert big_foo(5, 3) != result
        assert big_foo(5, 3) == result2

        self.cache.delete_memoized_verhash(big_foo)
        _fname, _fname_instance = function_namespace(big_foo)
        version_key = self.cache._memvname(_fname)
        assert self.cache.get(version_key) is None
        assert big_foo(5, 2) != result
        assert big_foo(5, 3) != result2
        assert self.cache.get(version_key) is not None

    def test_08_delete_memoize(self):
        @self.cache.memoize()
        def big_foo(a, b):
            return a + b + random.randrange(0, 100000)

        result_a = big_foo(5, 1)
        result_b = big_foo(5, 2)
        assert big_foo(5, 1) == result_a
        assert big_foo(5, 2) == result_b

        self.cache.delete_memoized(big_foo, 5, 2)
        assert big_foo(5, 1) == result_a
        assert big_foo(5, 2) != result_b

        ## Cleanup bigfoo 5, 1; 5, 2 or it might conflict with
        ## following run if it also uses memcache
        self.cache.delete_memoized(big_foo, 5, 2)
        self.cache.delete_memoized(big_foo, 5, 1)

    def test_09_args_memoize(self):
        @self.cache.memoize()
        def big_foo(a, b):
            return sum(a) + sum(b) + random.randrange(0, 100000)

        result_a = big_foo([5, 3, 2], [1])
        result_b = big_foo([3, 3], [3, 1])

        assert big_foo([5, 3, 2], [1]) == result_a
        assert big_foo([3, 3], [3, 1]) == result_b

        self.cache.delete_memoized(big_foo, [5, 3, 2], [1])

        assert big_foo([5, 3, 2], [1]) != result_a
        assert big_foo([3, 3], [3, 1]) == result_b

        ## Cleanup bigfoo 5, 1; 5,2 or it might conflict with
        ## following run if it also uses memecache
        self.cache.delete_memoized(big_foo, [5, 3, 2], [1])
        self.cache.delete_memoized(big_foo, [3, 3], [1])

    def test_10_kwargs_memoize(self):
        @self.cache.memoize()
        def big_foo(a, b=None):
            return a + sum(b.values()) + random.randrange(0, 100000)

        result_a = big_foo(1, dict(one=1, two=2))
        result_b = big_foo(5, dict(three=3, four=4))

        assert big_foo(1, dict(one=1, two=2)) == result_a
        assert big_foo(5, dict(three=3, four=4)) == result_b

        self.cache.delete_memoized(big_foo, 1, dict(one=1, two=2))

        assert big_foo(1, dict(one=1, two=2)) != result_a
        assert big_foo(5, dict(three=3, four=4)) == result_b

    def test_10a_kwargonly_memoize(self):
        @self.cache.memoize()
        def big_foo(a=None):
            if a is None:
                a = 0
            return a + random.random()

        result_a = big_foo()
        result_b = big_foo(5)

        assert big_foo() == result_a
        assert big_foo() < 1
        assert big_foo(5) == result_b
        assert big_foo(5) >= 5 and big_foo(5) < 6

    def test_10a_arg_kwarg_memoize(self):
        @self.cache.memoize()
        def f(a, b, c=1):
            return a + b + c + random.randrange(0, 100000)

        assert f(1, 2) == f(1, 2, c=1)
        assert f(1, 2) == f(1, 2, 1)
        assert f(1, 2) == f(1, 2)
        assert f(1, 2, 3) != f(1, 2)
        with self.assertRaises(TypeError):
            f(1)

    def test_10b_classarg_memoize(self):
        @self.cache.memoize()
        def bar(a):
            return a.value + random.random()

        class Adder(object):
            def __init__(self, value):
                self.value = value

        adder = Adder(15)
        adder2 = Adder(20)

        y = bar(adder)
        z = bar(adder2)

        assert y != z
        assert bar(adder) == y
        assert bar(adder) != z
        adder.value = 14
        assert bar(adder) == y
        assert bar(adder) != z

        assert bar(adder) != bar(adder2)
        assert bar(adder2) == z

    def test_10c_classfunc_memoize(self):
        class Adder(object):
            def __init__(self, initial):
                self.initial = initial

            @self.cache.memoize()
            def add(self, b):
                return self.initial + b

        adder1 = Adder(1)
        adder2 = Adder(2)

        x = adder1.add(3)
        assert adder1.add(3) == x
        assert adder1.add(4) != x
        assert adder1.add(3) != adder2.add(3)

    def test_10d_classfunc_memoize_delete(self):
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

        assert a1 != a2
        assert adder1.add(3) == a1
        assert adder2.add(3) == a2

        self.cache.delete_memoized(adder1.add)

        a3 = adder1.add(3)
        a4 = adder2.add(3)

        self.assertNotEqual(a1, a3)
        assert a1 != a3
        self.assertEqual(a2, a4)

        self.cache.delete_memoized(Adder.add)

        a5 = adder1.add(3)
        a6 = adder2.add(3)

        self.assertNotEqual(a5, a6)
        self.assertNotEqual(a3, a5)
        self.assertNotEqual(a4, a6)

    def test_10e_delete_memoize_classmethod(self):
        class Mock(object):
            @classmethod
            @self.cache.memoize(5)
            def big_foo(cls, a, b):
                return a + b + random.randrange(0, 100000)

        result = Mock.big_foo(5, 2)
        result2 = Mock.big_foo(5, 3)
        time.sleep(1)
        assert Mock.big_foo(5, 2) == result
        assert Mock.big_foo(5, 2) == result
        assert Mock.big_foo(5, 3) != result
        assert Mock.big_foo(5, 3) == result2

        self.cache.delete_memoized(Mock.big_foo)
        assert Mock.big_foo(5, 2) != result
        assert Mock.big_foo(5, 3) != result2

    def test_14_memoized_multiple_arg_kwarg_calls(self):
        @self.cache.memoize()
        def big_foo(a, b, c=[1, 1], d=[1, 1]):
            rand = random.randrange(0, 100000)
            return sum(a) + sum(b) + sum(c) + sum(d) + rand

        result_a = big_foo([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        assert big_foo([5, 3, 2], [1], d=[3, 3], c=[3, 3]) == result_a
        assert big_foo(b=[1], a=[5, 3, 2], c=[3, 3], d=[3, 3]) == result_a
        assert big_foo([5, 3, 2], [1], [3, 3], [3, 3]) == result_a

    def test_15_memoize_multiple_arg_kwarg_delete(self):
        @self.cache.memoize()
        def big_foo(a, b, c=[1, 1], d=[1, 1]):
            rand = random.randrange(0, 100000)
            return sum(a) + sum(b) + sum(c) + sum(d) + rand

        result_a = big_foo([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        self.cache.delete_memoized(big_foo, [5, 3, 2], [1], [3, 3], [3, 3])
        result_b = big_foo([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        assert result_a != result_b

        self.cache.delete_memoized(
            big_foo, [5, 3, 2], b=[1], c=[3, 3], d=[3, 3])
        result_b = big_foo([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        assert result_a != result_b

        self.cache.delete_memoized(big_foo, [5, 3, 2], [1], c=[3, 3], d=[3, 3])
        result_a = big_foo([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        assert result_a != result_b

        self.cache.delete_memoized(
            big_foo, [5, 3, 2], b=[1], c=[3, 3], d=[3, 3])
        result_a = big_foo([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        assert result_a != result_b

        self.cache.delete_memoized(big_foo, [5, 3, 2], [1], c=[3, 3], d=[3, 3])
        result_b = big_foo([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        assert result_a != result_b

        self.cache.delete_memoized(big_foo, [5, 3, 2], [1], [3, 3], [3, 3])
        result_a = big_foo([5, 3, 2], [1], c=[3, 3], d=[3, 3])
        assert result_a != result_b

    def test_16_memoize_kwargs_to_args(self):
        def big_foo(a, b, c=None, d=None):
            return sum(a) + sum(b) + random.randrange(0, 100000)

        expected = (1, 2, 'foo', 'bar')

        args, kwargs = self.cache._memoize_kwargs_to_args(
            big_foo, 1, 2, 'foo', 'bar')
        assert (args == expected)

        args, kwargs = self.cache._memoize_kwargs_to_args(
            big_foo, 2, 'foo', 'bar', a=1)
        assert (args == expected)

        args, kwargs = self.cache._memoize_kwargs_to_args(
            big_foo, a=1, b=2, c='foo', d='bar')
        assert (args == expected)

        args, kwargs = self.cache._memoize_kwargs_to_args(
            big_foo, d='bar', b=2, a=1, c='foo')
        assert (args == expected)

        args, kwargs = self.cache._memoize_kwargs_to_args(
            big_foo, 1, 2, d='bar', c='foo')
        assert (args == expected)

    def test_17_dict_config(self):
        from werkzeug.contrib.cache import SimpleCache
        cache = Cache(CACHE_TYPE='simple')
        assert cache.config['CACHE_TYPE'] == 'simple'
        assert isinstance(cache.cache, SimpleCache)


if 'TRAVIS' in os.environ:
    try:
        import redis
    except ImportError:
        has_redis = False
    else:
        has_redis = True

    if sys.version_info <= (2, 7):

        class CacheMemcachedTestCase(CacheTestCase):
            def _get_config(self):
                return {'CACHE_TYPE': 'memcached'}

        class SpreadCacheMemcachedTestCase(CacheTestCase):
            def _get_config(self):
                return {'CACHE_TYPE': 'spreadsaslmemcachedcache'}

    class CacheRedisTestCase(CacheTestCase):
        def _get_config(self):
            return {'CACHE_TYPE': 'redis'}

        @unittest.skipUnless(has_redis, "requires Redis")
        def test_20_redis_url_default_db(self):
            from werkzeug.contrib.cache import RedisCache

            self.config.update(
                {
                    'CACHE_TYPE': 'redis',
                    'CACHE_REDIS_URL': 'redis://localhost:6379'})

            cache = Cache(**self.config)
            assert isinstance(cache.cache, RedisCache)
            rconn = cache.cache._client.connection_pool.get_connection('foo')
            assert rconn.db == 0

        @unittest.skipUnless(has_redis, "requires Redis")
        def test_21_redis_url_custom_db(self):
            self.config.update(
                {
                    'CACHE_TYPE': 'redis',
                    'CACHE_REDIS_URL': 'redis://localhost:6379/2'})

            cache = Cache(**self.config)
            rconn = cache.cache._client.connection_pool.get_connection('foo')
            assert rconn.db == 2

        @unittest.skipUnless(has_redis, "requires Redis")
        def test_22_redis_url_explicit_db_arg(self):
            self.config.update(
                {
                    'CACHE_TYPE': 'redis',
                    'CACHE_REDIS_URL': 'redis://localhost:6379/2',
                    'CACHE_REDIS_DB': 1})

            cache = Cache(**self.config)
            rconn = cache.cache._client.connection_pool.get_connection('foo')

            assert rconn.db == 1

    class CacheFilesystemTestCase(CacheTestCase):
        def _get_config(self):
            return {'CACHE_TYPE': 'filesystem', 'CACHE_DIR': '/tmp'}


if __name__ == '__main__':
    unittest.main()
