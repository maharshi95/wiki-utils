import collections
import functools

from typing import List, Dict, Tuple, Set, Callable
from typing import Any, Optional, Iterable, Collection, Union

import logging

Strings = Optional[Collection[str]]


class Cached:
    """An auxiliary class for having cached implementations of the methods."""

    def __init__(self, cache_filepath=None, tag=None):
        self._cache = collections.defaultdict(dict)
        self.cache_filepath = cache_filepath
        if cache_filepath:
            self.update_cache_from_file(cache_filepath, tag)

    def read_cache_from_file(self, filepath):
        raise NotImplementedError

    def save_cache_to_file(self, filepath, tag=None):
        raise NotImplementedError

    def update_cache_from_file(self, filepath, tag=None):
        cache_dict = self.read_cache_from_file(filepath)
        self.update_cache(cache_dict, tag)

    def update_cache(self, cache_dict, tag=None):
        if tag:
            self._cache[tag].update(cache_dict)
        else:
            self._cache.update(cache_dict)

    def get_cache(self, tag=None):
        if tag:
            return self._cache[tag]
        else:
            return self._cache

    def get_caches(self, tags: Strings = None):
        tags = tags or self._cache.keys()
        return {tag: self._cache[tag] for tag in tags}

    def _cache_get(self, tag, key):
        return self._cache[tag].get(key, None)

    def _cache_put(self, tag, key, value):
        self._cache[tag][key] = value


def _DEFAULT_KEY_FN(args: tuple):
    return args[0] if len(args) == 1 else args


def cached_method(tag: str, key_fn: Callable[[tuple], Any] = _DEFAULT_KEY_FN):
    """Parameterized Decorator for caching class methods.

    Args:
      tag: key used to access the cache dict of this function.
      key_fn: function to get key from a set of params.
        default key_fn behavior: str(args[0]) if only single argument. else
          str(args)
    """

    def inner_decorator(method):
        @functools.wraps(method)
        def decorated_method(self, *args, **kwargs):
            key = str(key_fn(args + tuple(kwargs.items())))
            values = self._cache_get(tag, key)
            if values is None:
                logging.info(
                    f"{method.__name__}: '{key}' not found in cache. Making API request."
                )
                values = method(self, *args, **kwargs)
            self._cache_put(tag, key, values)
            return values

        return decorated_method

    return inner_decorator
