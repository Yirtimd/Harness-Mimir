import asyncio
import pytest
from core.trust import TrustLedger, TrustLevel
from core.trust import _INIT_SQL as TRUST_SQL
from .eval_runner import EvalCase, EvalSuite


TRUST_EVAL_CASES = [
    EvalCase(
        name='steady_promotion',
        input={'actions': [(True,)] * 12},
        expected_outcome='SPECIALIST',
        description='12 успешных подряд → повышение до SPECIALIST',
        tags=['promotion'],
    ),
    EvalCase(
        name='no_early_promotion',
        input={'actions': [(True,)] * 7},
        expected_outcome='TOOL_EXECUTOR',
        description='7 успешных — ещё недостаточно (порог 10)',
        tags=['promotion', 'boundary'],
    ),
    EvalCase(
        name='demotion_after_failures',
        input={
            'initial_level': TrustLevel.ORCHESTRATOR,
            'actions': [(False,)] * 10,
        },
        expected_outcome='SPECIALIST',
        description='10 ошибок с уровня ORCHESTRATOR → понижение',
        tags=['demotion'],
    ),
    EvalCase(
        name='mixed_history_stable',
        input={'actions': [(True,), (False,), (True,), (True,), (True,)] * 4},
        expected_outcome='TOOL_EXECUTOR',
        description='Смешанная история (80% success) — недостаточно для повышения?',
        tags=['mixed'],
    ),
    EvalCase(
        name='recovery_after_demotion',
        input={
            'initial_level': TrustLevel.SPECIALIST,
            'phase1': [(False,)] * 8,
            'phase2': [(True,)] * 15,
        },
        expected_outcome='SPECIALIST',
        description='Сначала ошибки → понижение, потом восстановление',
        tags=['recovery'],
    ),
]

async def run_trust_evals(ledger: TrustLedger):
    suite = EvalSuite('TrustLedger Behavior')

    async def runner(inp: dict) -> str:
        agent_id = f'eval-{id(inp)}'
        initial = inp.get('initial_level', TrustLevel.TOOL_EXECUTOR)
        await ledger.register_agent(agent_id, initial)

        # Фаза 1 (если есть)
        for (success,) in inp.get('phase1', []):
            await ledger.record(agent_id, 'action', success)

        # Основные действия
        for (success,) in inp.get('actions', []):
            await ledger.record(agent_id, 'action', success)

        # Фаза 2 (если есть)
        for (success,) in inp.get('phase2', []):
            await ledger.record(agent_id, 'action', success)

        return (await ledger.level(agent_id)).name

    report = await suite.run_all(TRUST_EVAL_CASES, runner)
    return report

@pytest.mark.asyncio
@pytest.mark.eval
async def test_trust_evals(fake_redis, pg_pool):
    from core.trust import _INIT_SQL
    async with pg_pool.acquire() as conn:
        await conn.execute(_INIT_SQL)
    ledger = TrustLedger(redis=fake_redis, pg_pool=pg_pool)
    report = await run_trust_evals(ledger)
    # Eval считается успешным при score >= 0.8
    assert report['score'] >= 0.8, f'Trust eval score too low: {report}'