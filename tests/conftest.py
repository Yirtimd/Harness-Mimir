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
    from uuid import uuid4
    dsn = os.getenv(
        "TEST_POSTGRES_DSN",
        "postgresql://harness:harness@localhost:5432/harness",
    )

    # Unique schema per test so runs never share data.
    schema = f"test_{uuid4().hex[:8]}"

    # Create the schema with a one-off connection BEFORE the pool exists.
    sys_conn = await asyncpg.connect(dsn)
    await sys_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    await sys_conn.close()

    # search_path is applied to EVERY connection the pool opens,
    # so all tables and data land in the isolated schema.
    pool = await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=3,
        server_settings={"search_path": schema},
    )

    yield pool

    await pool.close()
    sys_conn = await asyncpg.connect(dsn)
    await sys_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await sys_conn.close()

@pytest.fixture
def isolated_registry():
    '''Prometheus registry without global state'''
    return CollectorRegistry()

@pytest.fixture
def monitor(isolated_registry):
    return MonitoringLayer(registry=isolated_registry)
