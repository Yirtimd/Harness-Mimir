from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

import asyncpg
import redis.asyncio as aioredis
import structlog

from .config import settings

log = structlog.get_logger(__name__)

# DDL

_INIT_SQL = '''
CREATE TABLE IF NOT EXISTS trust_records (
    id          BIGSERIAL PRIMARY KEY,
    ts          DOUBLE PRECISION NOT NULL,
    agent_id    TEXT NOT NULL,
    action      TEXT NOT NULL,
    success     BOOLEAN NOT NULL,
    trust_level SMALLINT NOT NULL,
    context     JSONB,
    session_id  TEXT
);

CREATE INDEX IF NOT EXISTS idx_trust_agent ON trust_records(agent_id, ts DESC);
'''

class TrustLevel(IntEnum):
    TOOL_EXECUTOR   = 1
    SPECIALIST      = 2
    ORCHESTRATOR    = 3
    SUPERVISOR      = 4
    HUMAN_ARCHITECT = 5


@dataclass
class TrustRecord:
    agent_id: str
    action: str
    success: bool
    trust_level_at_time: TrustLevel
    context: dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)

class TrustLedger:
    '''
    Agent trust registry.

    Redis -> curent trust level + sliding window ( quick access)
    Postgres -> full history

    Promotion and demotion logic:
    - success_rate over last WINDOW actions > PROMOTE_THRESHOLD -> +1
    - success_rate < DEMOTE_THRESHOLD -> -1
    - Level 5 (Human) cannot reached automatically
    '''

    WINDOW            = 20
    PROMOTE_THRESHOLD = 0.90
    DEMOTE_THRESHOLD  = 0.60

    def __init__(self, redis: aioredis.Redis, pg_pool: asyncpg.Pool) -> None:
        self._redis = redis
        self._pg = pg_pool

    @classmethod
    async def create(cls) -> "TrustLedger":
        redis = await aioredis.from_url(settings.redis_url, decode_responses=True)
        pg_pool = await asyncpg.create_pool(settings.postgres_dsn, min_size=2, max_size=10)
        async with pg_pool.acquire() as conn:
            await conn.execute(_INIT_SQL)
        return cls(redis=redis, pg_pool=pg_pool)

    async def close(self) -> None:
        await self._redis.aclose()
        await self._pg.close()
    
    # ------------------------------------------------------------------
    # Key Redis
    # ------------------------------------------------------------------

    def _level_key(self, agent_id: str) -> str:
        return f"{settings.redis_key_prefix}:trust:level:{agent_id}"

    def _window_key(self, agent_id: str) -> str:
        return f'{settings.redis_key_prefix}:trust:window:{agent_id}'

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    async def register_agent(
        self, 
        agent_id: str,
        initial_level: TrustLevel = TrustLevel.TOOL_EXECUTOR,
    ) -> None:
        existing = await self._redis.get(self._level_key(agent_id))
        if existing is None:
            await self._redis.set(self._level_key(agent_id), int
            (initial_level))
            log.info('agent.registered', agent=agent_id,
            level=initial_level.name)
    
    # ------------------------------------------------------------------
    # Actions recording
    # ------------------------------------------------------------------
            

    async def record(
        self, 
        agent_id: str,
        action: str,
        success: bool,
        context: dict[str, Any] | None = None,
    ) -> TrustRecord:
        current = await self.level(agent_id)
        rec=TrustRecord(
            agent_id=agent_id,
            action=action,
            success=success,
            trust_level_at_time=current,
            context=context or {},
        )

        # sliding window in Redis (list byte: 1=success, 0=fail)
        wkey = self._window_key(agent_id)
        await self._redis.rpush(wkey, 1 if success else 0)
        await self._redis.ltrim(wkey, -self.WINDOW, -1)
        await self._redis.expire(wkey, settings.redis_ttl_seconds * 24)

        # full histroy in Postgres

        await self._pg.execute(
            '''
            INSERT INTO trust_records(ts, agent_id, action, success, trust_level, context)
            VALUES ($1, $2, $3, $4, $5, $6)
            ''',
            rec.ts, agent_id, action, success, int(current),
            json.dumps(context or {}),
        )

        await self._recalculate(agent_id)
        log.info('trust.record', agent=agent_id, action=action,
        success=success,
                level=current.name)
        return rec

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def level(self, agent_id: str) -> TrustLevel:
        raw = await self._redis.get(self._level_key(agent_id))
        if raw is None:
            await self.register_agent(agent_id)
            return TrustLevel.TOOL_EXECUTOR
        return TrustLevel(int(raw))

    async def success_rate(self, agent_id: str) -> float:
        window = await self._redis.lrange(self._window_key(agent_id), 0, -1)
        if not window:
            return 0.0
        return sum(int(x) for x in window) / len(window)

    async def can_act_at(self, agent_id: str, required: TrustLevel) -> bool:
        return await self.level(agent_id) >= required

    async def history(self, agent_id: str, last_n: int = 50) -> list[dict[str, Any]]:
        rows = await self._pg.fetch(
            '''
            SELECT ts, action, success, trust_level, context
            FROM trust_records
            WHERE agent_id = $1
            ORDER BY ts DESC LIMIT $2
            ''',
            agent_id, last_n,
        )
        return [dict(r) for r in rows]

    async def summary(self) -> dict[str, Any]:
        rows = await self._pg.fetch(
            "SELECT DISTINCT agent_id FROM trust_records"
        )
        result = {}
        for row in rows:
            aid = row["agent_id"]
            lvl = await self.level(aid)
            rate = await self.success_rate(aid)
            result[aid] = {
                "level": lvl.name,
                "level_value": int(lvl),
                "success_rate": round(rate, 3),
            }
        return result

    # ------------------------------------------------------------------
    # Trust level recalculation
    # ------------------------------------------------------------------

    async def _recalculate(self, agent_id: str) -> None:
        window = await self._redis.lrange(self._window_key(agent_id), 0, -1)
        # A level change (up OR down) requires a full window of evidence.
        if len(window) < 10:
            return

        rate = sum(int(x) for x in window) / len(window)
        current = await self.level(agent_id)

        new_level = current
        if rate > self.PROMOTE_THRESHOLD:
            if current < TrustLevel.SUPERVISOR:
                new_level = TrustLevel(current + 1)
        elif rate < self.DEMOTE_THRESHOLD:
            if current > TrustLevel.TOOL_EXECUTOR:
                new_level = TrustLevel(current - 1)

        if new_level != current:
            await self._redis.set(self._level_key(agent_id), int(new_level))
            # After a level change reset the window:
            # the agent must earn another full window to move again.
            await self._redis.delete(self._window_key(agent_id))
            log.info('trust.level_changed', agent=agent_id, from_=current.name,
             to=new_level, rate=round(rate, 3))