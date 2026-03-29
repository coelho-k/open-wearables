"""Centralized Redis client for the application."""

import os
from functools import lru_cache

import redis

from app.config import settings


@lru_cache()
def get_redis_client() -> redis.Redis:
    url = os.getenv("REDIS_URL") or settings.redis_url
    return redis.from_url(url, decode_responses=True)
