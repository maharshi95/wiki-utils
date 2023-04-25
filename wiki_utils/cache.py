import os
import json
import collections
import functools
from filelock import FileLock

from typing import Any, Optional, Collection, Dict, Callable

import logging

Strings = Optional[Collection[str]]


def create_lock_for(filepath: str, timeout: int = 5):
    return FileLock(filepath + ".lock", timeout=timeout)


def _write_json(data: Any, filepath: str, pretty=False):
    if not os.path.exists(os.path.dirname(filepath)):
        os.makedirs(os.path.dirname(filepath))
    with open(filepath, "w") as fp:
        indent = 4 if pretty else None
        json.dump(data, fp, indent=indent, sort_keys=True)


def _lock_and_write_json(data: Any, filepath: str, pretty=False):
    with create_lock_for(filepath):
        _write_json(filepath, data, pretty)


class Cached:
    """An auxiliary class for having cached implementations of the methods."""

    def __init__(self, cache_filepath: Optional[str] = None, tag: Optional[str] = None):
        self._cache = collections.defaultdict(dict)
        self.cache_filepath = cache_filepath

        if cache_filepath:
            self._cache_filelock = create_lock_for(cache_filepath, timeout=30)
            if os.path.exists(cache_filepath):
                self.update_cache_from_file(cache_filepath, tag)
                logging.info(
                    f"{self.__class__.__name__}: loading cache from {cache_filepath}"
                )
            else:
                self.save_cache_to_file(cache_filepath, tag)
                logging.info(f"{self.__class__.__name__}: starting with empty cache.")
        else:
            self._cache_filelock = None
            logging.warning(
                "No cache file specified. Initializing emtpy in-memory cache. Make sure"
                " to provide `filepath` when calling `save_cache_to_file`."
            )

    @staticmethod
    def read_cache_from_file(filepath: str):
        with open(filepath) as fp:
            return json.load(fp)

    def save_cache_to_file(self, filepath=None, tags: Strings = None, pretty=False):
        filepath = filepath or self.cache_filepath
        if filepath is None:
            raise ValueError(
                "No default filepath set for cache. Please provide `filepath`."
            )
        _write_json(self.get_caches(tags), filepath, pretty=pretty)

    def update_cache_from_file(self, filepath: str, tag: Optional[str] = None):
        cache_dict = Cached.read_cache_from_file(filepath)
        self.update_cache(cache_dict, tag)

    def sync_with_file(self, tag: Optional[str] = None):
        with self._cache_filelock:
            self.update_cache_from_file(self.cache_filepath, tag)
            self.save_cache_to_file(self.cache_filepath, tag)

    def update_cache(self, cache_dict: Dict[str, Any], tag: Optional[str] = None):
        if tag:
            self._cache[tag].update(cache_dict)
        else:
            for tag in self._cache.keys():
                self._cache[tag].update(cache_dict.get(tag, {}))

    def get_cache(self, tag: Optional[str] = None):
        if tag:
            return self._cache[tag]
        else:
            return self._cache

    def get_caches(self, tags: Strings = None):
        tags = tags or self._cache.keys()
        return {tag: self._cache[tag] for tag in tags}

    def get_cache_keys(self, tags: Optional[str] = None):
        tags = tags or self._cache.keys()
        return {tag: list(self._cache[tag].keys()) for tag in tags}

    def get_summary(self):
        return {tag: len(self._cache[tag]) for tag in self._cache.keys()}

    def _cache_get(self, tag: str, key: str):
        return self._cache[tag].get(key, None)

    def _cache_put(self, tag, key: str, value):
        self._cache[tag][key] = value


def _DEFAULT_KEY_FN(args: tuple):
    return args[0] if len(args) == 1 else args


def cached_method(tag: str, key_fn: Callable[[tuple], Any] = _DEFAULT_KEY_FN):
    """Parameterized Decorator for caching class methods.

    This decorator is used to cache the results of a method of `Cached` subclass.

    Args:
      tag: key used to access the cache dict of this function.
      key_fn: function to get key from a set of params.
        default key_fn behavior: str(args[0]) if only single argument. else
          str(args)
    """

    def inner_decorator(method: Callable[[Any, ...], Any]):
        @functools.wraps(method)
        def decorated_method(self: Cached, *args, **kwargs):
            key = str(key_fn(args + tuple(kwargs.items())))
            values = self._cache_get(tag, key)
            if values is None:
                logging.info(
                    f"{method.__name__}: '{key}' not found in cache. Making API"
                    " request."
                )
                values = method(self, *args, **kwargs)
            self._cache_put(tag, key, values)
            return values

        return decorated_method

    return inner_decorator
