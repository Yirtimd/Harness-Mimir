from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine


@dataclass
class EvalCase:
    name: str
    input: dict[str, Any]
    expected_outcome: str          # "approve" | "reject" | "escalate" | "pass" | "block"
    description: str = ''
    tags: list[str] = field(default_factory=list)


@dataclass
class EvalResult:
    case_name: str
    expected: str
    actual: str
    passed: bool
    latency_ms: float
    notes: str = ''

class EvalSuite:
    def __init__(self, name: str) -> None:
        self.name = name
        self.results: list[EvalResult] = []

    async def run_case(
        self,
        case: EvalCase,
        runner: Callable[[dict[str, Any]], Coroutine[Any, Any, str]],
    ) -> EvalResult:
        t = time.perf_counter()
        actual = await runner(case.input)
        ms = (time.perf_counter() - t) * 1000

        result = EvalResult(
            case_name=case.name,
            expected=case.expected_outcome,
            actual=actual,
            passed=(actual == case.expected_outcome),
            latency_ms=round(ms, 2),
        )
        self.results.append(result)
        return result

    async def run_all(
        self,
        cases: list[EvalCase],
        runner: Callable[[dict[str, Any]], Coroutine[Any, Any, str]],
    ) -> dict[str, Any]:
        for case in cases:
            await self.run_case(case, runner)
        return self.report()

    def report(self) -> dict[str, Any]:
        total   = len(self.results)
        passed  = sum(1 for r in self.results if r.passed)
        failed  = [r for r in self.results if not r.passed]
        avg_ms  = sum(r.latency_ms for r in self.results) / total if total else 0

        line = '=' * 50
        print(f'\n{line}')
        print(f'EVAL SUITE: {self.name}')
        print(line)
        print(f'Passed: {passed}/{total}  ({100*passed//total if total else 0}%)')
        print(f'Avg latency: {avg_ms:.1f}ms')
        if failed:
            print('\nFailed cases:')
            for r in failed:
                print(f'  ✗ {r.case_name}: expected={r.expected}, got={r.actual}')
        print()

        return {
            'suite': self.name,
            'total': total,
            'passed': passed,
            'score': passed / total if total else 0,
            'avg_latency_ms': round(avg_ms, 1),
            'failed': [{'name': r.case_name, 'expected': r.expected, 'actual': r.actual}
                       for r in failed],
        }
    
    
