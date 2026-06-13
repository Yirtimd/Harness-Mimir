import pytest
from core.byzantine import ByzantineValidator, JudgeVote, ValidationDecision


async def make_judge(approved: bool, confidence: float = 1.0, provider: str = "test"):
    async def judge(**kwargs) -> JudgeVote:
        return JudgeVote(
            judge_id=f'judge_{provider}',
            approved=approved,
            reasoning='test',
            confidence=confidence,
            provider=provider,
        )
    return judge


@pytest.fixture
async def validator():
    return ByzantineValidator(approve_threshold=0.6)

class TestDecisions:
    @pytest.mark.asyncio
    async def test_unanimous_approve(self, validator):
        for p in ['anthropic', 'openai', 'google']:
            validator.add_judge(p, await make_judge(True, provider=p))
        result = await validator.validate(action='test')
        assert result.decision == ValidationDecision.APPROVE
        assert result.is_unanimous is True

    @pytest.mark.asyncio
    async def test_unanimous_reject(self):
        v = ByzantineValidator(approve_threshold=0.6)
        for p in ['anthropic', 'openai', 'google']:
            v.add_judge(p, await make_judge(False, provider=p))
        result = await v.validate(action='test')
        assert result.decision == ValidationDecision.REJECT

    @pytest.mark.asyncio
    async def test_majority_approve(self):
        v = ByzantineValidator(approve_threshold=0.6)
        v.add_judge('a', await make_judge(True))
        v.add_judge('b', await make_judge(True))
        v.add_judge('c', await make_judge(False))
        result = await v.validate(action='test')
        assert result.decision == ValidationDecision.APPROVE
        assert result.approve_count == 2

    @pytest.mark.asyncio
    async def test_escalate_on_tie(self):
        v = ByzantineValidator(approve_threshold=0.6)
        v.add_judge('a', await make_judge(True))
        v.add_judge('b', await make_judge(False))
        result = await v.validate(action='test')
        assert result.decision == ValidationDecision.ESCALATE

    @pytest.mark.asyncio
    async def test_judge_failure_counts_as_reject(self):
        v = ByzantineValidator(approve_threshold=0.6)

        async def crashing_judge(**kwargs) -> JudgeVote:
            raise RuntimeError('LLM timeout')

        v.add_judge('crash', crashing_judge)
        v.add_judge('ok1', await make_judge(True))
        v.add_judge('ok2', await make_judge(True))

        result = await v.validate(action='test')
        # 2 approve, 1 reject (crash) → 2/3 = 0.67 > 0.6 → APPROVE
        assert result.decision == ValidationDecision.APPROVE

class TestProviderDiversity:
    @pytest.mark.asyncio
    async def test_providers_in_result(self):
        v = ByzantineValidator()
        v.add_judge('a', await make_judge(True, provider='anthropic'))
        v.add_judge('b', await make_judge(True, provider='openai'))
        result = await v.validate(action='test')
        assert 'anthropic' in result.providers
        assert 'openai' in result.providers


class TestWeightedConfidence:
    @pytest.mark.asyncio
    async def test_weighted_confidence_calculated(self):
        v = ByzantineValidator()
        v.add_judge('high', await make_judge(True, confidence=0.9))
        v.add_judge('low', await make_judge(True, confidence=0.3))
        result = await v.validate(action='test')
        assert 0.3 < result.weighted_confidence < 0.9