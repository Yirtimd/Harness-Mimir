from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Any, Coroutine

import httpx
import structlog

from .config import settings

log = structlog.get_logger(__name__)

class EscalationSeverity(str, Enum):
    INFO     = 'info'
    WARNING  = 'warning'
    CRITICAL = 'critical'

@dataclass
class EscalationEvent:
    severity: EscalationSeverity
    source: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    ts: float = field(default_factory=time.time)
    resolved: bool = False
    event_id: str = field(default_factory=lambda: f'event_{int(time.time() * 1000)}')

EscalationHandler = Callable[[EscalationEvent], Coroutine[Any, Any, None]]

class EscalationChannel:
    ''' Human escaaltion channel.
        Control components required by TRIZ law of system completeness (Law 1)
    '''
    
    def __init__(self) -> None:
        self._handlers: list[EscalationHandler] = [self._log_handler]
        self._queue: list[EscalationEvent] = []
        if settings.escalation_webhook_url:
            self._handlers.append(self._webhook_handler)

    def add_handler(self, handler: EscalationHandler) -> None:
        self._handlers.append(handler)

    async def escalate(
        self, 
        severity: EscalationSeverity,
        source: str,
        message: str,
        context: dict[str, Any] | None = None,
        block: bool = False,
    ) -> EscalationEvent:
        event = EscalationEvent(
            severity=severity,
            source=source,
            message=message,
            context=context or {},
        )
        self._queue.append(event)
        await asyncio.gather(*[h(event) for h in self._handlers])

        if block and severity == EscalationSeverity.CRITICAL:
            raise RuntimeError(f'[ESCALATION:CRITiCAL] {source}: {message}')
        return event
    
    def pending(self) -> list[EscalationEvent]:
        return [e for e in self._queue if not e.resolved]

    def resolve(self, event_id: str) -> bool:
        for e in self._queue:
            if e.event_id == event_id:
                e.resolved = True
                return True
        return False

    # ------------------------------------------------------------------
    # Handlers
    # ------------------------------------------------------------------

    @staticmethod
    async def _log_handler(event: EscalationEvent) -> None:
        log.bind(
            severity=event.severity.value,
            source=event.source,
            event_id=event.event_id,
            **event.context,
        ).log(
            {'info': 20, 'warning': 30, 'critical': 50}[event.severity.
            value],
            event.message,
        )

    @staticmethod
    async def _webhook_handler(event: EscalationEvent) -> None:
        if not settings.escalation_webhook_url:
            return
        payload = {
            "text": f"[{event.severity.value.upper()}] {event.source}: {event.message}",
            "attachments": [{"fields": [
                {"title": k, "value": str(v), "short": True}
                for k, v in event.context.items()
            ]}],
        }
        async with httpx.AsyncClient(timeout=5.0) as client:
            try:
                await client.post(settings.escalation_webhook_url,
                json=payload)
            except Exception as exc:
                log.error('escalation.webhook_failed', error=str(exc))
