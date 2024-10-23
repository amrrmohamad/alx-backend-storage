#!/usr/bin/env python3
"""
A module for using Redis NoSQL
data storage with additional functionality.
"""

import uuid
import redis
from functools import wraps
from typing import Any, Callable, Union, Optional


def count_calls(method: Callable) -> Callable:
    """Tracks the number of calls made to a method in the Cache class."""

    @wraps(method)
    def invoker(self, *args, **kwargs) -> Any:
        """Invokes the given method after incrementing its call counter."""
        try:
            self._redis.incr(method.__qualname__)
        except redis.RedisError as e:
            print(f"Redis error while incrementing call count: {e}")
        return method(self, *args, **kwargs)

    return invoker


def call_history(method: Callable) -> Callable:
    """Tracks the call details of a method in the Cache class."""

    @wraps(method)
    def invoker(self, *args, **kwargs) -> Any:
        """Stores method inputs and outputs and returns the method's output."""
        in_key = f"{method.__qualname__}:inputs"
        out_key = f"{method.__qualname__}:outputs"

        try:
            self._redis.rpush(in_key, str(args))
        except redis.RedisError as e:
            print(f"Redis error while pushing inputs: {e}")

        output = method(self, *args, **kwargs)

        try:
            self._redis.rpush(out_key, str(output))
        except redis.RedisError as e:
            print(f"Redis error while pushing outputs: {e}")

        return output

    return invoker


def replay(fn: Callable) -> None:
    """Displays the call history of a Cache class method."""
    if not fn or not hasattr(fn, "__self__"):
        print("Invalid method provided for replay.")
        return

    redis_store = getattr(fn.__self__, "_redis", None)
    if not isinstance(redis_store, redis.Redis):
        print("No Redis instance found.")
        return

    fxn_name = fn.__qualname__
    in_key = f"{fxn_name}:inputs"
    out_key = f"{fxn_name}:outputs"

    try:
        fxn_call_count = int(redis_store.get(fxn_name) or 0)
        print(f"{fxn_name} was called {fxn_call_count} times:")

        fxn_inputs = redis_store.lrange(in_key, 0, -1)
        fxn_outputs = redis_store.lrange(out_key, 0, -1)

        for fxn_input, fxn_output in zip(fxn_inputs, fxn_outputs):
            print(
                f"""{fxn_name}(*{fxn_input.decode("utf-8")})->
                  {fxn_output.decode("utf-8")}"""
            )
    except redis.RedisError as e:
        print(f"Redis error during replay: {e}")


class Cache:
    """Represents an object for storing data in Redis."""

    def __init__(self) -> None:
        """Initializes the Cache instance with a Redis connection."""
        try:
            self._redis = redis.Redis()
            self._redis.flushdb(True)  # Flush DB to start fresh
        except redis.RedisError as e:
            print(f"Redis connection error: {e}")

    @call_history
    @count_calls
    def store(self, data: Union[str, bytes, int, float]) -> str:
        """Stores a value in Redis and returns the key."""
        data_key = str(uuid.uuid4())
        try:
            self._redis.set(data_key, data)
        except redis.RedisError as e:
            print(f"Redis error while storing data: {e}")
        return data_key

    def get(
        self, key: str, fn: Optional[Callable] = None
    ) -> Union[str, bytes, int, float, None]:
        """
            Retrieves a value from Redis and applies
        a transformation function if provided.
        """
        try:
            data = self._redis.get(key)
            if data is None:
                return None
            return fn(data) if fn is not None else data
        except redis.RedisError as e:
            print(f"Redis error while getting data: {e}")
            return None

    def get_str(self, key: str) -> Optional[str]:
        """Retrieves a string value from Redis."""
        return self.get(key, lambda x: x.decode("utf-8"))

    def get_int(self, key: str) -> Optional[int]:
        """Retrieves an integer value from Redis."""
        return self.get(key, lambda x: int(x))