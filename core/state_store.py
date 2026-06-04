from __future__ import annotations

import json
import time
from typing import Any

import asyncpg
import redis.asyncio as aioredis
import structlog

from .config import settings

log = structlog.get_logger(__name__)

# DDL for PostgreSQL
_INIT_SQL = """
CREATE TABLE IF NOT EXISTS state_history (
    id          BIGSERIAL PRIMARY KEY,
    ts          DOUBLE PRECISION NOT NULL,
    operation   TEXT NOT NULL,          -- set | delete
    key         TEXT NOT NULL,
    old_value   JSONB,
    new_value   JSONB,
    session_id  TEXT
);

CREATE INDEX IF NOT EXISTS idx_state_history_key ON state_history(key);
CREATE INDEX IF NOT EXISTS idx_state_history_ts  ON state_history(ts DESC);
"""

class StateStore:
    '''
    Two-tier agent state storage.
    Redis - hot cache (TTL = settings.redis_ttl_seconds)
    PostgreSQL - persistent history of all state changes

    Law conductivity flow (TRIZ law 2):
    a single source of truth with no discontinuities
    '''

    def __init__(
        self,
        redis=aioredis.Redis,
        pg_pool=asyncpg.Pool,
        session_id: str = 'default',
    ) -> None:
        self._redis = redis
        self._pg = pg_pool
        self._session_id = session_id

    # ------------------------------------------------------------------
    # Factory method (use instead __init__ directly)
    # ------------------------------------------------------------------

    @classmethod
    async def create(cls, session_id: str = 'default') -> 'StateStore':
        redis = await aioredis.from_url(settings.redis_url,
        decode_response=True)
        pg_pool = await asyncpg.create_pool(settings.postgres_dsn,
        min_size=2, max_size=10)
        async with pg_pool.acquire() as conn:
            await conn.execute(_INIT_SQL)
        return cls(redis=redis, pg_pool=pg_pool, session_id=session_id)

    async def close(self) -> None:
        await self._redis.aclose()
        await self._pg.close()

    # ------------------------------------------------------------------
    # Key redis
    # ------------------------------------------------------------------

    def _rkey(self, key: str) -> str:
        return f'{settings.redis_key_prefix} : {self._session_id} : {key}'

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------    

    async def set(self, key: str, value: Any) -> None:
        rkey = self._rkey(key)
        old_raw = await self._redis.get(rkey)
        old = json.loads(old_raw) if old_raw else None

        serialized = json.dumps(value, ensure_ascii=False)
        await self._redis.set(rkey, serialized, ex=settings.redis_ttl_seconds)

        await self._pg.execute(
            """
            INSERT INTO state_history(ts, operation, key, old_value, new_value, session_id)
            VALUES ($1, 'set', $2, $3, $4, $5)
            """,
            time.time(),
            key,
            json.dumps(old) if old is not None else None,
            serialized, 
            self._session_id,
        )
        log.debug('state.set', key=key, session=self._session_id)



    async def get(self, key: str, default: Any = None) -> Any:
        raw = await self._redis.get(self._rkey(key))
        if raw is None:
            return default
        return json.loads(raw)

    async def delete(self, key: str) -> None:
        rkey = self._rkey(key)
        old_raw = await self._redis.get(rkey)
        await self._redis.delete(rkey)
        if old_raw:
            await self._pg.execute(
                """
                INSERT INTO state_history(ts, operation, key, old_value, session_id)
                VALUES ($1, 'delete', $2, $3, $4)
                """,
                time.time(), key, old_raw, self._session_id,
            )
        log.debug('state.delete', key=key)
    
    async def snapshot(self) -> dict[str, Any]:
        ''' All keys current session from redis '''
        pattern = f'{settings.redis_key_prefix}:{self._session_id}:*'
        keys = await self._redis.keys(pattern)
        if not keys:
            return {}
        values = await self._redis.mget(*keys)
        prefix_len = len(f"{settings.redis_key_prefix}:{self._session_id}:")
        return {
            k[prefix_len:]:json.loads(v)
            for k, v in zip(keys, values)
            if v is not None
        }

    # ------------------------------------------------------------------
    # History from PostgreSQL
    # ------------------------------------------------------------------

    async def history(
        self, 
        key: str | None = None,
        last_n: int = 50,
    ) -> list[dict[str, Any]]
        ''' Last N entries history (optionally by key) '''
        if keys:
            rows = await self._pg.fetch(
                """
                SELECT ts, operation, key, old_value, new_value
                FROM state_history
                WHERE session_id = $1 AND key = $2
                ORDER BY ts DESC LIMIT $3
                """,
                self._session_id, key, last_n,
            )
        else:
            rows = await. self._pg.fetch(
                """
                SELECT ts, operation, key, old_value, new_value
                FROM state_history
                WHERE session_id = $1
                ORDER BY ts DESC LIMIT $2
                """,
                self._session_id, last_n,
            )
        return [dict(r) for r in rows]




