from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

import structlog
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

log = structlog.get_logger(__name__)

class MonitoringLayer:
    '''
    Prometheus-compatible observability layer.
    Flow Conductivity Law (TRIZ law 2)

    Metrics:
    - harness_actions_total       (counter, labels: agent, tool, status)
    - harness_trust_level         (gauge,   labels: agent)
    - harness_pems_score          (gauge,   labels: agent)
    - harness_escalations_total   (counter, labels: severity)
    - harness_tool_duration_ms    (histogram)
    '''

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        reg = registry or CollectorRegistry()

        self.actions_total = Counter(
            'harness_actions_total',
            'Total agent actions', 
            ['agent', 'tool', 'status'],
            registry=reg,
        )

        self.trust_level = Gauge(
            'harness_trust_level',
            'Current trust level per agent',
            ['agent'],
            registry=reg,
        )

        self.pems_score = Gauge(
            'harness_pems_score',
            'PEMS score per agent',
            ['agent'],
            registry=reg,
        )

        self.escalations_total = Counter(
            'harness_escalations_total',
            'Total escalation events',
            ['severity'],
            registry=reg,
        )

        self.tool_duration = Histogram(
            'harness_tool_duration_ms',
            'Tool call duration in milliseconds',
            ['tool'],
            buckets=[5, 10, 25, 50, 100, 250, 500, 1000, 2500],
            registry=reg,
        )

        self._registry = reg
        self._start_time = time.time()

    # ------------------------------------------------------------------
    # Record methods
    # ------------------------------------------------------------------

    def record_action(self, agent: str, tool: str, status: str) -> None:
        self.actions_total.labels(agent=agent, tool=tool, status=status).inc()
        log.info('action.recorded', agent=agent, tool=tool, status=status)

    def set_trust_level(self, agent: str, level: int) -> None:
        self.trust_level.labels(agent=agent).set(level)
    
    def log_pems(self, pems: float, agent: str = 'default') -> None:
        self.pems_score.labels(agent=agent).set(pems)
        log.info('pems.updated', agent=agent, pems=pems)

    def record_escalation(self, severity: str) -> None:
        self.escalations_total.labels(severity=severity).inc()

    def observe_tool_duration(self, tool: str, duration_ms: float) -> None:
        self.tool_duration.labels(tool=tool).observe(duration_ms)

    # ------------------------------------------------------------------
    # Metrics server
    # ------------------------------------------------------------------

    def start_metrics_server(self) -> None:
        ''' Run HTTP-server Prometheus on sattings.metrics_port '''
        from .config import settings
        start_http_server(settings.metrics_port, registry=self._registry)
        log.info('metrics.server_started', port=settings.metrics_port)

    def uptime(self) -> float:
        return time.time() - self._start_time
