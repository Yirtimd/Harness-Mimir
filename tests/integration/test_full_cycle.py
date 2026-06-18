'''
Full intagration cycle: Trust → Shadow → Byzantine → MCP → Escalation
'''
import pytest
from core.mcp_registry import MCPRegistry, ToolDefinition
from core.state_store import StateStore, _INIT_SQL as STATE_SQL
from core.escalation import EscalationChannel, EscalationSeverity
from core.trust import TrustLedger, TrustLevel, _INIT_SQL as TRUST_SQL
from core.shadow import ShadowExecutor
from core.byzantine import ByzantineValidator, JudgeVote

@pytest.fixture
async def full_stack(fake_redis, pg_pool, monitor):
    async with pg_pool.acquire() as conn:
        await conn.execute(STATE_SQL)
        await conn.execute(TRUST_SQL)

    registry   = MCPRegistry()
    store      = StateStore(redis=fake_redis, pg_pool=pg_pool, session_id='integration')
    escalation = EscalationChannel()
    ledger     = TrustLedger(redis=fake_redis, pg_pool=pg_pool)
    shadow     = ShadowExecutor()
    validator  = ByzantineValidator(approve_threshold=0.6)

    return {
        'registry': registry,
        'store': store,
        'escalation': escalation,
        'ledger': ledger,
        'shadow': shadow,
        'validator': validator,
        'monitor': monitor,
    }

@pytest.mark.asyncio
async def test_protected_action_success(full_stack):
    '''
    Agent with sufficient trust level performs the action
    via shadow + byzantine.
    '''
    s = full_stack
    agent_id = 'integration-agent'

    await s['ledger'].register_agent(agent_id, TrustLevel.SPECIALIST)
    await s['store'].set('counter', 0)

    # Регистрируем инструмент
    async def increment(amount: int) -> str:
        current = await s['store'].get('counter', 0)
        await s['store'].set('counter', current + amount)
        return f'counter={current + amount}'

    s['registry'].register(ToolDefinition(
        name='increment', description='Increment counter',
        min_trust_level=2, handler=increment, is_reversible=True,
    ))

    # Shadow + validator
    async def shadow_increment(amount: int) -> str:
        current = await s['store'].get('counter', 0)
        return f'[shadow] would set counter={current + amount}'

    def validate(out: str) -> tuple[bool, str | None]:
        return ('shadow' in out, None if 'shadow' in out else 'Bad output')

    # Byzantine
    async def judge(**kwargs) -> JudgeVote:
        return JudgeVote('j1', True, 'ok', 0.95, provider='internal')

    s['validator'].add_judge('j1', judge)

    # Проверка доверия
    assert await s['ledger'].can_act_at(agent_id, TrustLevel.SPECIALIST)

    # Shadow execution
    result = await s['shadow'].execute(
        action_name='increment',
        real_fn=lambda **kw: increment(**kw),
        shadow_fn=shadow_increment,
        validator=validate,
        state_to_snapshot=s['store'],
        amount=5,
    )
    assert result.success

    # Byzantine validation
    val = await s['validator'].validate(action='increment', amount=5)
    assert val.decision.value == 'approve'

    # Запись в ledger
    await s['ledger'].record(agent_id, 'increment', success=True)
    s['monitor'].record_action(agent_id, 'increment', 'success')

    counter = await s['store'].get('counter')
    assert counter == 5

@pytest.mark.asyncio
async def test_insufficient_trust_escalates(full_stack):
    '''Agents with insufficent trust level -> scalation'''
    s = full_stack
    agent_id = 'low-trust-agent'
    await s['ledger'].register_agent(agent_id, TrustLevel.TOOL_EXECUTOR)

    can = await s['ledger'].can_act_at(agent_id, TrustLevel.ORCHESTRATOR)
    assert can is False

    event = await s['escalation'].escalate(
        EscalationSeverity.WARNING,
        source=agent_id,
        message='Attempted action above trust level',
    )
    assert event.severity == EscalationSeverity.WARNING
    assert len(s['escalation'].pending()) == 1

@pytest.mark.asyncio
async def test_shadow_blocks_bad_action(full_stack):
    '''Shadow blocks invalid action until production'''
    s = full_stack

    async def dangerous_real(**kwargs):
        raise AssertionError('Should never reach production')

    async def shadow_bad(**kwargs):
        raise ValueError('Shadow detected invalid params')

    result = await s['shadow'].execute(
        action_name='dangerous',
        real_fn=dangerous_real,
        shadow_fn=shadow_bad,
        validator=lambda o: (True, None),
    )
    assert result.success is False
    assert 'Shadow raised' in result.shadow.failure_reason