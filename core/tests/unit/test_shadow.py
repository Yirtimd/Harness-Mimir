import pytest
from core.shadow import ShadowExecutor


@pytest.fixture
def executor():
    return ShadowExecutor()


async def ok_fn(**kwargs):
    return 'real_result'


async def fail_fn(**kwargs):
    raise RuntimeError('Production failure')


async def shadow_ok(**kwargs):
    return 'shadow_ok'


async def shadow_fail(**kwargs):
    raise ValueError('Shadow validation fail')


def always_pass(output):
    return True, None

def always_fail(output):
    return False, 'Validation rejected'


class TestShadowPass:
    @pytest.mark.asyncio
    async def test_success_path(self, executor):
        result = await executor.execute(
            action_name='test_action',
            real_fn=ok_fn,
            shadow_fn=shadow_ok,
            validator=always_pass,
        )
        assert result.success is True
        assert result.real_output == 'real_result'
        assert result.shadow.shadow_passed is True

    @pytest.mark.asyncio
    async def test_shadow_blocks_production(self, executor):
        result = await executor.execute(
            action_name='blocked_action',
            real_fn=ok_fn,
            shadow_fn=shadow_ok,
            validator=always_fail,
        )
        assert result.success is False
        assert result.real_output is None
        assert result.shadow.failure_reason == 'Validation rejected'

    @pytest.mark.asyncio
    async def test_shadow_exception_blocks(self, executor):
        result = await executor.execute(
            action_name='shadow_crash',
            real_fn=ok_fn,
            shadow_fn=shadow_fail,
            validator=always_pass,
        )
        assert result.success is False
        assert 'Shadow raised' in result.shadow.failure_reason

class TestRollback:
    @pytest.mark.asyncio
    async def test_rollback_on_production_failure(self, executor):
        '''If production fall — rollback restores snapshot.'''

        class FakeStore:
            def __init__(self):
                self.data = {'balance': 1000}

            def snapshot(self):
                return dict(self.data)

            async def set(self, key, value):
                self.data[key] = value

        store = FakeStore()
        store.data['balance'] = 500  # change before test

        with pytest.raises(RuntimeError, match='Production failed'):
            await executor.execute(
                action_name='failing_action',
                real_fn=fail_fn,
                shadow_fn=shadow_ok,
                validator=always_pass,
                state_to_snapshot=store,
            )

        # After rollback balance should return at snapshot (500)
        assert store.data['balance'] == 500
