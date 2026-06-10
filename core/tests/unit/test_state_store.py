import pytest
from core.state_store import StateStore

@pytest.fixture
async def store(fake_redis, pg_pool):
    s = StateStore(redis=fake_redis, pg_pool=pg_pool,
                   session_id='test-session')
    # init table
    from core.state_store import _INIT_SQL
    async with pg_pool.acquire() as conn:
        await conn.execute(_INIT_SQL)
    return s


class TestCRUD:
    @pytest.mark.asyncio
    async def test_set_and_get(self, store):
        await store.set('key1', 'value1')
        assert await store.get('key1') == 'value1'

    @pytest.mark.asyncio
    async def test_get_default(self, store):
        assert await store.get('missing', default=42) == 42

    @pytest.mark.asyncio
    async def test_set_complex_value(self, store):
        data = {'nested': [1, 2, 3], 'flag': True}
        await store.set('complex', data)
        assert await store.get('complex') == data

    @pytest.mark.asyncio
    async def test_delete(self, store):
        await store.set('to_delete', 'x')
        await store.delete('to_delete')
        assert await store.get('to_delete') is None

    @pytest.mark.asyncio
    async def test_snapshot(self, store):
        await store.set('a', 1)
        await store.set('b', 2)
        snap = await store.snapshot()
        assert snap['a'] == 1
        assert snap['b'] == 2

class TestHistory:
    @pytest.mark.asyncio
    async def test_history_records_set(self, store):
        await store.set('tracked', 'v1')
        await store.set('tracked', 'v2')
        hist = await store.history(key='tracked')
        assert len(hist) == 2
        # last record be first
        assert hist[0]['new_value'] == '"v2"'

    @pytest.mark.asyncio
    async def test_history_records_delete(self, store):
        await store.set('ephemeral', 'x')
        await store.delete('ephemeral')
        hist = await store.history(key='ephemeral')
        ops = [r['operation'] for r in hist]
        assert 'delete' in ops

