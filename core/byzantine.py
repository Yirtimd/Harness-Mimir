from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine

import structlog

log = structlog.get_logger(__name__)

class ValidationDecision(str, Enum):
    APPROVE  = 'approve'
    REJECT   = 'reject'
    ESCALATE = 'escalate'

@dataclass
class JudgeVote:
    judge_id: str
    approved: bool
    reasoning: str
    confidence: float  #0.0 - 1.0
    duration_ms: float = 0.0
    provider: str = 'unknown'  # 'anthropic' | 'openai' | 'google' | 'internal'

@dataclass
class ValidationResult:
    decision: ValidationDecision
    votes: list[JudgeVote]
    approve_count: int
    reject_count: int
    consensus_ratio: float
    weighted_confidence: float  # Average confidence * vote
    ts: float = field(default_factory=time.time)

    @property
    def is_unanimous(self) -> bool:
        return self.approve_count == 0 or self.reject_count == 0
    
    @property
    def providers(self) -> list[str]:
        return list({v.provider for v in self.votes})
    
JudgeFn = Callable[..., Coroutine[Any, Any, JudgeVote]]

class ByzantineValidator:
    '''
    Byzantine fault tolerance for agent decisions.

    TRIZ Principle #27: parallel low-cost judges + majority voting
    TRIZ Principle #26: cross-provider validation for critical operations

    approve_threshold: the fraction of approval votes required for an APPROVE decision.
    With 3 judges, a threshold of 0.6 requires at least 2 approval votes.
    '''
    
    def __init__(self, approve_threshold: float = 0.6) -> None:
        self._threshold = approve_threshold
        self._judges: list[tuple[str, JudgeFn]] = []
    
    def add_judge(self, judge_id: str, judge_fn: JudgeFn) -> None:
        self._judges.append((judge_id, judge_fn))
        log.info('byzantine.judge_added', judge=judge_id)

    def provider_diversity(self) -> set[str]:
        '''Set og registered providers (checked after validation)'''
        return set() # actual value in ValidationResult.provider
    
    async def validate(self, **kwargs: Any) -> ValidationResult:
        if not self._judges:
            raise RuntimeError('No judges registered')
        
        tasks = [self._run_judge(jid, fn, **kwargs) for jid, fn in self._judges]
        votes: list[JudgeVote] = await asyncio.gather(*tasks)

        approve = sum(1 for v in votes if v.approved)
        reject = len(votes) - approve
        ratio = approve / len(votes)

        # Weighted confidence score (accounts for each judge's confidence level)

        weighted = sum(
            v.confidence * (1 if v.approved else 0) for v in votes
        ) / len(votes)

        if ratio > self._threshold:
            decision = ValidationDecision.APPROVE
        elif ratio < (1 - self._threshold):
            decision = ValidationDecision.REJECT
        else:
            decision = ValidationDecision.ESCALATE

        result = ValidationResult(
            decision=decision,
            votes=votes,
            approve_count=approve,
            reject_count=reject,
            consensus_ratio=round(ratio, 3),
            weighted_confidence=round(weighted, 3),
        )

        log.info(
            'byzantine.decision',
            decision=decision,
            approve=approve,
            reject=reject,
            ratio=round(ratio, 3),
            providers=result.providers
        )
        return result
    
    @staticmethod
    async def _run_judge(judge_id: str, fn: JudgeFn, **kwargs: Any) -> JudgeVote:
        t = time.perf_counter()
        try:
            vote = await fn(**kwargs)
            vote.duration_ms = round((time.perf_counter() - t) * 1000, 2)
            return vote
        except Exception as exc:
            log.error('byzantine.judge_failed', judge=judge_id, error=str(exc))
            return JudgeVote(
                judge_id=judge_id,
                approved=False,
                reasoning=f'Judge error: {exc}',
                confidence=0.0,
                duration_ms=round((time.perf_counter() - t) * 1000, 2),
                provider='error',
            )

