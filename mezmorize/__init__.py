#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: sw=4:ts=4:expandtab
"""
    mezmorize
    ~~~~~~~~~

    Adds function memoization support
"""

from __future__ import absolute_import, division, print_function

import base64
import functools
import hashlib
import inspect
import uuid
import warnings

from importlib import import_module
from . import backends

__version__ = '0.18.1'
__title__ = 'mezmorize'
__package_name__ = 'mezmorize'
__author__ = 'Reuben Cummings'
__description__ = 'Adds function memoization support'
__email__ = 'reubano@gmail.com'
__license__ = 'BSD'
__copyright__ = 'Copyright 2015 Reuben Cummings'

# Used to remove control characters and whitespace from cache keys.
is_invalid = lambda c: not (c in {'_', '.'} or c.isalnum())
delchars = filter(is_invalid, map(chr, range(256)))

try:
    trans_tbl = ''.maketrans({k: None for k in delchars})
    null_control = (trans_tbl,)
except AttributeError:
    null_control = (None, ''.join(delchars))


def function_namespace(f, *args):
    """
    Attempts to returns unique namespace for function
    """
    m_args = inspect.getargspec(f)[0] or []
    instance_token = None
    instance_self = getattr(f, '__self__', None)

    if instance_self and not inspect.isclass(instance_self):
        instance_token = repr(f.__self__)
    elif args and m_args and m_args[0] == 'self':
        instance_token = repr(args[0])

    module = f.__module__

    if hasattr(f, '__qualname__'):
        name = f.__qualname__
    else:
        klass = getattr(f, '__self__', None)

        if klass and not inspect.isclass(klass):
            klass = klass.__class__

        klass = klass or getattr(f, 'im_class', None)

        if not klass and args and m_args and m_args[0] == 'self':
            klass = args[0].__class__
        elif not klass and args and m_args and m_args[0] == 'cls':
            klass = args[0]

        name = klass.__name__ + '.' + f.__name__ if klass else f.__name__

    ns = '.'.join((module, name)).translate(*null_control)

    if instance_token:
        ins = '.'.join((module, name, instance_token)).translate(*null_control)
    else:
        ins = None

    return ns, ins


#: Cache Object
################
class Cache(object):
    """
    This class is used to control the cache objects.
    """
    def __init__(self, namespace='', **config):
        config.setdefault('CACHE_DEFAULT_TIMEOUT', 300)
        config.setdefault('CACHE_THRESHOLD', 500)
        config.setdefault('CACHE_KEY_PREFIX', 'mezmorize_')
        config.setdefault('CACHE_MEMCACHED_SERVERS', None)
        config.setdefault('CACHE_DIR', None)
        config.setdefault('CACHE_OPTIONS', None)
        config.setdefault('CACHE_ARGS', [])
        config.setdefault('CACHE_TYPE', 'simple')
        config.setdefault('CACHE_NO_NULL_WARNING', False)

        warning = not config['CACHE_NO_NULL_WARNING']

        if config['CACHE_TYPE'] == 'null' and warning:
            warnings.warn(
                "CACHE_TYPE is set to null, caching is effectively disabled.")

        self.namespace = str(namespace)
        self.config = config
        self._set_cache()

    def _set_cache(self):
        module_string = self.config['CACHE_TYPE']

        if '.' not in module_string:
            try:
                cache_obj = getattr(backends, module_string)
            except AttributeError:
                msg = '{} is not a valid FlaskCache backend'
                raise ImportError(msg.format(module_string))
        else:
            cache_obj = import_module(module_string)

        args = self.config['CACHE_ARGS'][:]
        kwargs = {'default_timeout': self.config['CACHE_DEFAULT_TIMEOUT']}

        if self.config['CACHE_OPTIONS']:
            kwargs.update(self.config['CACHE_OPTIONS'])

        self.cache = cache_obj(self.config, *args, **kwargs)

    def get(self, *args, **kwargs):
        "Proxy function for internal cache object."
        return self.cache.get(*args, **kwargs)

    def set(self, *args, **kwargs):
        "Proxy function for internal cache object."
        self.cache.set(*args, **kwargs)

    def add(self, *args, **kwargs):
        "Proxy function for internal cache object."
        self.cache.add(*args, **kwargs)

    def delete(self, *args, **kwargs):
        "Proxy function for internal cache object."
        self.cache.delete(*args, **kwargs)

    def delete_many(self, *args, **kwargs):
        "Proxy function for internal cache object."
        self.cache.delete_many(*args, **kwargs)

    def clear(self):
        "Proxy function for internal cache object."
        self.cache.clear()

    def get_many(self, *args, **kwargs):
        "Proxy function for internal cache object."
        return self.cache.get_many(*args, **kwargs)

    def set_many(self, *args, **kwargs):
        "Proxy function for internal cache object."
        self.cache.set_many(*args, **kwargs)

    def _memvname(self, funcname):
        return funcname + '_memver'

    def _memoize_make_version_hash(self):
        if self.namespace.startswith('http'):
            UUID = uuid.uuid3(uuid.NAMESPACE_URL, self.namespace)
        if self.namespace:
            UUID = uuid.uuid3(uuid.NAMESPACE_DNS, self.namespace)
        else:
            UUID = uuid.uuid4()

        return base64.b64encode(UUID.bytes)[:6].decode('utf-8')

    def _memoize_version(self, f, *args, **kwargs):
        """
        Updates the hash version associated with a memoized function or method.
        """
        reset = kwargs.pop('reset', None)
        delete = kwargs.pop('delete', None)
        fname, instance_fname = function_namespace(f, *args)
        version_key = self._memvname(fname)
        fetch_keys = [version_key]

        if instance_fname:
            instance_version_key = self._memvname(instance_fname)
            fetch_keys.append(instance_version_key)

        # Only delete the per-instance version key or per-function version
        # key but not both.
        if delete:
            self.cache.delete_many(fetch_keys[-1])
            return fname, None

        version_data_list = list(self.cache.get_many(*fetch_keys))
        dirty = False

        if version_data_list[0] is None:
            version_data_list[0] = self._memoize_make_version_hash()
            dirty = True

        if instance_fname and version_data_list[1] is None:
            version_data_list[1] = self._memoize_make_version_hash()
            dirty = True

        # Only reset the per-instance version or the per-function version
        # but not both.
        if reset:
            fetch_keys = fetch_keys[-1:]
            version_data_list = [self._memoize_make_version_hash()]
            dirty = True

        if dirty:
            zipped = zip(fetch_keys, version_data_list)
            self.cache.set_many(dict(zipped), **kwargs)

        return fname, ''.join(version_data_list)

    def _memoize_make_cache_key(self, make_name=None, timeout=None):
        """
        Function used to create the cache_key for memoized functions.
        """
        def make_cache_key(f, *args, **kwargs):
            _timeout = getattr(timeout, 'cache_timeout', timeout)
            fname, version_data = self._memoize_version(
                f, *args, timeout=_timeout)

            #: this should have to be after version_data, so that it
            #: does not break the delete_memoized functionality.
            altfname = make_name(fname) if callable(make_name) else fname

            if callable(f):
                keyargs, keykwargs = self._memoize_kwargs_to_args(
                    f, *args, **kwargs)
            else:
                keyargs, keykwargs = args, kwargs

            updated = '{0}{1}{2}'.format(altfname, keyargs, keykwargs)
            cache_key = hashlib.md5()
            cache_key.update(updated.encode('utf-8'))
            cache_key = base64.b64encode(cache_key.digest())[:16]
            cache_key = cache_key.decode('utf-8')
            cache_key += version_data

            return cache_key
        return make_cache_key

    def _memoize_kwargs_to_args(self, f, *args, **kwargs):
        #: Inspect the arguments to the function
        #: This allows the memoization to be the same
        #: whether the function was called with
        #: 1, b=2 is equivalent to a=1, b=2, etc.
        new_args = []
        arg_num = 0
        argspec = inspect.getargspec(f)
        args_len = len(argspec.args)
        defaults = argspec.defaults

        for i in range(args_len):
            if i == 0 and argspec.args[i] in ('self', 'cls'):
                #: use the repr of the class instance
                #: this supports instance methods for
                #: the memoized functions, giving more
                #: flexibility to developers
                arg = repr(args[0])
                arg_num += 1
            elif argspec.args[i] in kwargs:
                arg = kwargs[argspec.args[i]]
            elif arg_num < len(args):
                arg = args[arg_num]
                arg_num += 1
            elif defaults and abs(i - args_len) <= len(defaults):
                arg = defaults[i - args_len]
                arg_num += 1
            else:
                arg = None
                arg_num += 1

            #: Attempt to convert all arguments to a
            #: hash/id or a representation?
            #: Not sure if this is necessary, since
            #: using objects as keys gets tricky quickly.
            # if hasattr(arg, '__class__'):
            #     try:
            #         arg = hash(arg)
            #     except:
            #         arg = repr(arg)

            #: Or what about a special __cacherepr__ function
            #: on an object, this allows objects to act normal
            #: upon inspection, yet they can define a representation
            #: that can be used to make the object unique in the
            #: cache key. Given that a case comes across that
            #: an object "must" be used as a cache key
            # if hasattr(arg, '__cacherepr__'):
            #     arg = arg.__cacherepr__

            new_args.append(arg)

        return tuple(new_args), {}

    def memoize(self, timeout=None, make_name=None, unless=None):
        """
        Use this to cache the result of a function, taking its arguments into
        account in the cache key.

        Information on
        `Memoization <http://en.wikipedia.org/wiki/Memoization>`_.

        Example::
            >>> import random
            >>>
            >>> cache = Cache()
            >>> random.seed(10)
            >>>
            >>> @cache.memoize(timeout=50)
            ... def big_foo(a, b):
            ...     return a + b + random.randrange(0, 1000)

        .. code-block:: pycon

            >>> big_foo(5, 2)
            578
            >>> big_foo(5, 3)
            436
            >>> big_foo(5, 2)
            578

        .. versionadded:: 0.4
            The returned decorated function now has three function attributes
            assigned to it.

                **uncached**
                    The original undecorated function. readable only

                **cache_timeout**
                    The cache timeout value for this function. For a custom
                    value to take affect, this must be set before the function
                    is called.

                    readable and writable

                **make_cache_key**
                    A function used in generating the cache_key used.

                    readable and writable


        :param timeout: Default None. If set to an integer, will cache for that
                        amount of time. Unit of time is in seconds.
        :param make_name: Default None. If set this is a function that accepts
                          a single argument, the function name, and returns a
                          new string to be used as the function name. If not
                          set then the function name is used.
        :param unless: Default None. Cache will *always* execute the caching
                       facilities unless this callable is true.
                       This will bypass the caching entirely.

        .. versionadded:: 0.5
            params ``make_name``, ``unless``
        """

        def _memoize(f):
            @functools.wraps(f)
            def decorated(*args, **kwargs):
                if callable(unless) and unless():  # bypass cache
                    return f(*args, **kwargs)

                cache_key = decorated.make_cache_key(f, *args, **kwargs)
                value = self.cache.get(cache_key)

                if value is None:
                    value = f(*args, **kwargs)
                    ckwargs = {'timeout': decorated.cache_timeout}

                    def set_cache(value, key):
                        self.cache.set(key, value, **ckwargs)
                        return value

                    try:
                        value.addCallback(set_cache, cache_key)
                    except AttributeError:
                        set_cache(value, cache_key)

                return value

            decorated.uncached = f
            decorated.cache_timeout = timeout
            m_make_cache_key = self._memoize_make_cache_key
            decorated.make_cache_key = m_make_cache_key(make_name, decorated)
            decorated.delete_memoized = lambda: self.delete_memoized(f)
            return decorated

        return _memoize

    def delete_memoized(self, f, *args, **kwargs):
        """
        Deletes the specified functions caches, based by given parameters.
        If parameters are given, only the functions that were memoized with
        them will be erased. Otherwise all versions of the caches will be
        forgotten.

        Example::
            >>> import random
            >>>
            >>> cache = Cache()
            >>> random.seed(10)
            >>>
            >>> @cache.memoize(50)
            ... def random_func():
            ...    return random.randrange(1, 50)

            >>> @cache.memoize()
            ... def param_func(a, b):
            ...    return a + b + random.randrange(1, 50)

        .. code-block:: pycon

            >>> random_func()
            28
            >>> random_func()
            28
            >>> cache.delete_memoized(random_func)
            >>> random_func()
            22
            >>> param_func(1, 2)
            32
            >>> param_func(1, 2)
            32
            >>> param_func(2, 2)
            15
            >>> cache.delete_memoized(param_func, 1, 2)
            >>> param_func(1, 2)
            43
            >>> param_func(2, 2)
            15

        Delete memoized is also smart about instance methods vs class methods.

        When passing an instancemethod, it will only clear the cache related
        to that instance of that object. (object uniqueness can be overridden
            by defining the __repr__ method, such as user id).

        When passing a classmethod, it will clear all caches related across
        all instances of that class.

        Example::

            >>> random.seed(10)
            >>>
            >>> class Adder(object):
            ...    @cache.memoize()
            ...    def add(self, b):
            ...        return b + random.random()

        .. code-block:: pycon

            >>> adder1 = Adder()
            >>> adder2 = Adder()
            >>> adder1.add(3)
            3.5714025946899133
            >>> adder2.add(3)
            3.4288890546751145
            >>> cache.delete_memoized(adder1.add)
            >>> adder1.add(3)
            3.5780913011344704
            >>> adder2.add(3)
            3.4288890546751145
            >>> cache.delete_memoized(Adder.add)
            >>> adder1.add(3)
            3.2060982321395017
            >>> adder2.add(3)
            3.81332125135732

        :param fname: Name of the memoized function, or a reference to
            the function.
        :param \*args: A list of positional parameters used with memoized
            function.
        :param \**kwargs: A dict of named parameters used with memoized
            function.

        .. note::

            Flask-Cache uses inspect to order kwargs into positional args when
            the function is memoized. If you pass a function reference into
            ``fname`` instead of the function name, Flask-Cache will be able to
            place the args/kwargs in the proper order, and delete the
            positional cache.

            However, if ``delete_memoized`` is just called with the name of the
            function, be sure to pass in potential arguments in the same order
            as defined in your function as args only, otherwise Flask-Cache
            will not be able to compute the same cache key.

        .. note::

            Flask-Cache maintains an internal random version hash for the
            function. Using delete_memoized will only swap out the version
            hash, causing the memoize function to recompute results and put
            them into another key.

            This leaves any computed caches for this memoized function within
            the caching backend.

            It is recommended to use a very high timeout with memoize if using
            this function, so that when the version has been swapped, the old
            cached results would eventually be reclaimed by the caching
            backend.
        """
        if not callable(f):
            raise DeprecationWarning(
                "Deleting messages by relative name is no longer"
                " reliable, please switch to a function reference")

        if not (args or kwargs):
            self._memoize_version(f, reset=True)
        else:
            cache_key = f.make_cache_key(f.uncached, *args, **kwargs)
            self.cache.delete(cache_key)

    def delete_memoized_verhash(self, f, *args):
        """
        Delete the version hash associated with the function.

        ..warning::

            Performing this operation could leave keys behind that have
            been created with this version hash. It is up to the application
            to make sure that all keys that may have been created with this
            version hash at least have timeouts so they will not sit orphaned
            in the cache backend.
        """
        if not callable(f):
            raise DeprecationWarning(
                "Deleting messages by relative name is no longer"
                " reliable, please use a function reference")

        self._memoize_version(f, delete=True)
