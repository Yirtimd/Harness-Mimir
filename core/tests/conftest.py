import asyncio
import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
import asyncpg
import pytest_asyncio

from core.monitoring import MonitoringLayer
from prometheus_client import CollectorRegistry

@pytest.fixture(scope='session')
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def fake_redis():
    ''' Fake redis without actual server'''
    server = FakeServer()
    client = FakeRedis(server=server, decode_responses=True)
    yield client
    await client.aclose()

@pytest.fixture
async def pg_pool():
    '''
    Actual PostgreSQL via asyncpg
    Requires docker compose to be running
    Create isolated scheme for the duration on the test'''

    import os
    dsn = os.getenv(
        "TEST_POSTGRES_DSN",
        "postgresql://harness:harness@localhost:5432/harness",
    )

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=3)

    # Isolations scheme for the test
    schema = f'test_{id(pool)}'
    async with pool.acquire() as conn:
        await conn.execute(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        await conn.execute(f'SET search_path TO {schema}')

    yield pool

    async with pool.acquire() as conn:
        await conn.execute(f'DROP SCHEMA IF EXISTS {schema} CASCADE')
    await pool.close()

@pytest.fixture
def isolated_registry():
    '''Prometheus registry without global state'''
    return CollectorRegistry()

@pytest.fixture
def monitor(isolated_registry):
    return MonitoringLayer(registry=isolated_registry)
