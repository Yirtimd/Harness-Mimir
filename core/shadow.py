from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine

import structlog

log = structlog.get_logger(__name__)

ShadowFn = Callable[..., Coroutine[Any, Any, Any]]
ValidatorFn = Callable[[Any], tuple[bool, str | None]]

@dataclass
class ShadowResult:
    action_name: str
    shadow_output: Any
    shadow_duration_ms: float
    shadow_passed: bool
    failure_reason: str | None = None

@dataclass
class ExecutionResult:
    action_name: str
    shadow: ShadowResult
    real_output: Any | None = None
    real_duration_ms: float | None = None
    rollback_taken: bool = False
    ts: float = field(default_factory=time.time)

    @property
    def success(self) -> bool:
        return self.shadow.shadow_passed and self.real_output is not None

class ShadowExecutor:
    '''
    Executor with pre-execution phase.

    TRIZ principle #10: shadow → validate → production
    TRIZ principle #11: rollback-point is created BEFORE entering in production

    Supports any object with method snapshot() as state_to_snapshot.
    StateStore.snapshot() returns a dict that his used for rollback
    '''

    async def execute(
        self,
        action_name: str,
        real_fn: Callable[..., Coroutine[Any, Any, Any]],
        shadow_fn: ShadowFn,
        validator: ValidatorFn,
        state_to_snapshot: Any | None = None
        **kwargs: Any,
    ) -> ExecutionResult:
        # --- Rollback snapshot -----------------------------------
        snapshot = None
        if state_to_snapshot is not None:
            if asyncio.iscoroutinefunction(getattr(state_to_snapshot, 'snapshot', None)):
                snapshot = await state_to_snapshot.snapshot()
            elif hasattr(state_to_snapshot, 'snapshot'):
                snapshot = state_to_snapshot.snapshot()


        # --- Shadow -----------------------------------------------

        t0 = time.perf_counter()
        try:
            shadow_output = await shadow_fn(**kwargs)
            passed, reason = validator(shadow_output)
        except Exception as exc:
            shadow_output = None
            passed, reason = False, f'Shadow raised: {ecx}'

        shadow_ms = (time.perf_counter() - t0) * 100
        shadow_result = ShadowResult(
            action_name=action_name,
            shadow_output=shadow_output,
            shadow_duration_ms=round(shadow_ms, 2),
            shadow_passed=passed,
            failure_reason=reason,
        )

        log.info(
            'shadow.result',
            action=action_name,
            passed=passed,
            reason=reason,
            ms=round(shadow_ms, 2),
        )

        if not passed:
            return ExecutionResult(action_name=action_name,
            shadow=shadow_result)

         # --- Production ---------------------------------------------

         t1 = time.perf_counter()
         rollback_taken = False
         try:
            real_output = await real_fn(**kwargs)
            real_ms = (time.perf_counter() - t1) * 1000
            log.info('production.success', action=action_name,ms=round(real_ms, 2))
        except Exception as exc:
            real_ms = (time.perf_counter() - t1) * 1000
            log.error('production.failed', action=action_name, error=str(exc))

            # Rollback
            if snapshot is not None and state_to_snapshot in not None:
                await self._apply_rollback(state_to_snapshot, snapshot)
                rollback_taken = True
                log.warning('rollback.applied', action=action_name)
            
            raise RunTimeError(f"Production failed, rollback={'applied' if rollback_taken else 'n/a'}") from exc

        return ExecutionResult(
            action_name=action_name,
            shadow=shadow_result,
            real_output=real_output,
            real_duration_ms=round(real_ms, 2),
            rollback_taken=rollback_taken,
        )

    @staticmethod
    async def _apply_rollback(store: Any, snapshot: dict[str, Any]) -> None:
        '''
        Restore to StateStore from snapchot.
        For each key in snapshot -> store.set()
        '''

        for key, value in snapshot.items():
            if asyncio.iscoroutinefunction(getattr(store, 'set', None)):
                await store.set(key, value)
            else:
                store.set(key, value)

                



