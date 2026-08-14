# test_redis.py

from redis_client import redis_client

try:
    redis_client.ping()
    print("Redis connection successful")
except Exception as exc:
    print(f"Redis connection failed: {exc}")