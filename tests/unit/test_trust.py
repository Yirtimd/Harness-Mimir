import pytest
from core.trust import TrustLedger, TrustLevel
from core.state_store import _INIT_SQL as STATE_SQL
from core.trust import _INIT_SQL as TRUST_SQL

@pytest.fixture
async def ledger(fake_redis, pg_pool):
    async with pg_pool.acquire() as conn:
        await conn.execute(TRUST_SQL)
    return TrustLedger(redis=fake_redis, pg_pool=pg_pool)

class TestRegistration:
    @pytest.mark.asyncio
    async def test_default_level(self, ledger):
        lvl = await ledger.level('new_level')
        assert lvl == TrustLevel.TOOL_EXECUTOR

    @pytest.mark.asyncio
    async def test_register_with_level(self, ledger):
        await ledger.register_agent('agent-x', TrustLevel.SPECIALIST)
        assert await ledger.level('agent-x') == TrustLevel.SPECIALIST

class TestPromotion:
    @pytest.mark.asyncio
    async def test_promote_after_success(self, ledger):
        await ledger.register_agent('promo-agent', TrustLevel.TOOL_EXECUTOR)
        for i in range(12):
            await ledger.record('promo-agent', f'action_{i}', success=True)
        assert await ledger.level('promo-agent') == TrustLevel.SPECIALIST

    @pytest.mark.asyncio
    async def test_demote_after_failures(self, ledger):
        await ledger.register_agent('demote-agent', TrustLevel.ORCHESTRATOR)
        for i in range(10):
            await ledger.record('demote-agent', f'fail_{i}', success=False)
        lvl = await ledger.level('demote-agent')
        assert lvl < TrustLevel.ORCHESTRATOR

    @pytest.mark.asyncio
    async def test_no_auto_promote_to_human(self, ledger):
        await ledger.register_agent("top-agent", TrustLevel.SUPERVISOR)
        for i in range(20):
            await ledger.record("top-agent", f"act_{i}", success=True)
        assert await ledger.level("top-agent") == TrustLevel.SUPERVISOR

    @pytest.mark.asyncio
    async def test_no_promote_below_10_actions(self, ledger):
        await ledger.register_agent("new-agent", TrustLevel.TOOL_EXECUTOR)
        for i in range(7):
            await ledger.record("new-agent", f"act_{i}", success=True)
        assert await ledger.level("new-agent") == TrustLevel.TOOL_EXECUTOR


class TestPermissions:
    @pytest.mark.asyncio
    async def test_can_act_at_own_level(self, ledger):
        await ledger.register_agent("spec", TrustLevel.SPECIALIST)
        assert await ledger.can_act_at("spec", TrustLevel.SPECIALIST) is True

    @pytest.mark.asyncio
    async def test_cannot_act_above_level(self, ledger):
        await ledger.register_agent("spec2", TrustLevel.SPECIALIST)
        assert await ledger.can_act_at("spec2", TrustLevel.ORCHESTRATOR) is False


class TestHistory:
    @pytest.mark.asyncio
    async def test_history_persisted(self, ledger):
        await ledger.register_agent("hist-agent")
        await ledger.record("hist-agent", "do_something", success=True, context={"x": 1})
        hist = await ledger.history("hist-agent", last_n=5)
        assert len(hist) == 1
        assert hist[0]["action"] == "do_something"

