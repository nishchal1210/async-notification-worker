import os
from dataclasses import dataclass

from redis.exceptions import RedisError

from redis_client import redis_client


RATE_LIMIT_REQUESTS = int(os.getenv("RATE_LIMIT_REQUESTS", "5"))
RATE_LIMIT_WINDOW_SECONDS = int(
    os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60")
)


RATE_LIMIT_SCRIPT = """
local current = redis.call("INCR", KEYS[1]) 

if current == 1 then
    redis.call("EXPIRE", KEYS[1], ARGV[1])
end

local ttl = redis.call("TTL", KEYS[1])

return {current, ttl}
"""

# Increase this Redis value by 1.

# The very first request creates the counter:

# counter = 1

# At that moment we say:

# Delete this counter automatically after 60 seconds.

# That's what EXPIRE does.

# TTL means:

# Time To Live.

# It asks Redis:

# How many seconds until this key disappears?

# This becomes useful when the user exceeds the limit.

# We can tell them:

# Try again in 43 seconds.

# Python receives:

# current_count = 4
# ttl = 37



@dataclass
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    retry_after: int


def check_rate_limit(user_id: int) -> RateLimitResult:
    key = f"rate_limit:notifications:user:{user_id}"

    try:
        current_count, ttl = redis_client.eval(

            # Redis EVAL roughly looks like:

            # EVAL script number_of_keys key arguments
            RATE_LIMIT_SCRIPT,
            1,
            key,
            RATE_LIMIT_WINDOW_SECONDS,


            # script = RATE_LIMIT_SCRIPT

            # number of Redis keys = 1

            # KEYS[1] = key

            # ARGV[1] = RATE_LIMIT_WINDOW_SECONDS
        )
    except RedisError as exc:  # this is for if redis is down
        raise RuntimeError(
            f"Redis rate-limit check failed: {exc}"
        ) from exc

    current_count = int(current_count)
    ttl = max(int(ttl), 0)

    allowed = current_count <= RATE_LIMIT_REQUESTS

    remaining = max(
        RATE_LIMIT_REQUESTS - current_count,
        0,
    )

    return RateLimitResult(
        allowed=allowed,
        limit=RATE_LIMIT_REQUESTS,
        remaining=remaining,
        retry_after=ttl,
    )