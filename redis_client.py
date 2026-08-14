import os

import redis
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL")

if not REDIS_URL:
    raise RuntimeError("REDIS_URL is missing from environment variables")

redis_client = redis.Redis.from_url( # creates the Redis client using your connection URL.
    REDIS_URL,
    decode_responses=True, # makes Redis return normal Python strings instead of bytes.
    socket_connect_timeout=5, # The timeouts stop your API from waiting indefinitely when Redis is unavailable.
    socket_timeout=5,
)