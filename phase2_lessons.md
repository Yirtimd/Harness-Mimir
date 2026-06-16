# Фаза 2 — Плоскость памяти (LightMem → FluxMem)
### Стек: Python 3.11+ · LightMem (zjunlp) · PostgreSQL · Redis · pytest · asyncio

> **Принцип:** никаких велосипедов. Память делает готовая библиотека **LightMem** (та же лаборатория, что и FluxMem). Свой код — только тонкий слой интеграции с харнессом Фазы 1: адаптер, мост из trust_records, PEMS-расчёт и губернатор доверия.

---

## Статус FluxMem (проверено 12.06.2026)

- **Статья**: arXiv 2605.28773 «Rethinking Memory as Continuously Evolving Connectivity», v1 от **27.05.2026**, пометка *Ongoing work*.
- **Код**: в статье обещан в `github.com/zjunlp/LightMem` (*«The code will be open-sourced…»*) — **пока не выложен**. В репозитории сейчас: LightMem (ICLR 2026) + StructMem (ACL 2026).
- **Решение**: строим Фазу 2 на LightMem уже сейчас; при выходе FluxMem-кода мигрируем **внутри той же зависимости** (см. раздел «Миграция на FluxMem» в конце).

### Что закрывает LightMem, а что — нет

| Этап FluxMem (теория) | LightMem сегодня | Наш код |
|---|---|---|
| Stage I — формирование связей | `add_memory()`: сегментация, экстракция, embedding-индекс (qdrant) | — |
| Stage II — рефайнмент | `construct_update_queue_all_entries()` + `offline_update_all_entries(score_threshold)` | — |
| Stage III — консолидация | `summarize()` + StructMem `extraction_mode="event"` (event-связи, кросс-событийные сводки) | — |
| 𝒱_epi из истории агентов | — | `TrustBridge` (trust_records → add_memory) |
| PEMS | **нет в библиотеке** | `PEMSLite` (временный, помечен к замене) |
| PEMS → уровень доверия | — (это наша архитектура из AGENTS.md) | `MaturityGovernor` |

**Граница миграции** — интерфейс `MemoryService` (Урок 2.1). Весь харнесс ходит в память только через него; при выходе FluxMem меняется содержимое адаптера, остальной код не трогается.

---

## Структура проекта (добавления Фазы 2)

```
harness/
├── core/
│   ├── ...                        # Фаза 1 — без изменений
│   └── memory/
│       ├── __init__.py
│       ├── service.py             # Урок 2.1  MemoryService — адаптер LightMem
│       ├── bridge.py              # Урок 2.2  trust_records → память
│       ├── pems.py                # Урок 2.3  PEMSLite + MaturityGovernor
│       └── nightly.py             # Урок 2.4  Ночной цикл
├── tests/
│   ├── conftest.py                # + FakeLightMemory, memory_service
│   ├── unit/
│   │   ├── test_memory_service.py
│   │   ├── test_bridge.py
│   │   ├── test_pems.py
│   │   └── test_governor.py
│   ├── integration/
│   │   └── test_memory_cycle.py   # полный цикл + smoke с реальным LightMem
│   └── evals/
│       └── eval_memory.py
└── ...
```

Объём своего кода: ~4 небольших модуля. Хранение, индексация, экстракция, сегментация, offline-update, сводки — целиком библиотека.

---

## Зависимости и инфраструктура

### Установка LightMem

`pip install lightmem` пока «coming soon» — ставим из исходников:

```bash
# в активированном .venv
pip install "lightmem @ git+https://github.com/zjunlp/LightMem.git"
```

Зависимость тяжёлая (torch, sentence-transformers). Поэтому:
- импорт LightMem в нашем коде — **ленивый** (внутри `MemoryService.create()`), unit-тесты работают без установленной библиотеки;
- embedding-модель скачивается один раз: `sentence-transformers/all-MiniLM-L6-v2` (384 dims);
- pre-compression (llmlingua-2) по умолчанию **выключен** — включается конфигом, когда скачана модель.

`requirements.txt` — добавить:

```
# Memory (Фаза 2)
lightmem @ git+https://github.com/zjunlp/LightMem.git
```

### docker-compose.yml — без изменений

LightMem использует **embedded qdrant** (локальная директория, без сервера). PostgreSQL и Redis Фазы 1 достаточно: Postgres хранит `pems_history`, Redis (через `StateStore`) — watermark моста.

### `core/config.py` — добавить в `Settings`

```python
    # LightMem (Фаза 2)
    lightmem_dir: str = "./lightmem_data"        # qdrant-коллекции + history.db
    lightmem_llm_backend: str = "openai"          # openai | deepseek | ollama | vllm
    lightmem_llm_model: str = "gpt-4o-mini"
    lightmem_llm_api_key: str = ""
    lightmem_llm_base_url: str = "https://api.openai.com/v1"
    lightmem_embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    lightmem_embedding_dims: int = 384
    lightmem_llmlingua_path: str = ""             # пусто → pre_compress выключен
    lightmem_device: str = "cpu"

    # PEMS-lite (временный, до релиза FluxMem)
    pems_min_tokens: float = 10.0
```

В `.env`:
```
LIGHTMEM_LLM_API_KEY=sk-...
```

---

---

# Урок 2.1 — MemoryService: адаптер LightMem

## Теория

ТРИЗ-триминг: функцию памяти **не реализуем — поручаем** готовому компоненту. Наша задача — корректное «поле взаимодействия» (Су-поле): один класс, который
1. собирает production-конфиг LightMem из `settings` (никаких хардкодов — конвенция Фазы 1);
2. оборачивает синхронный API библиотеки в async (харнесс целиком async-first) через `asyncio.to_thread` + `Lock` (LightMem не заявляет потокобезопасность);
3. нормализует ответы: формат записей `retrieve()`/`summarize()` зависит от версии библиотеки, поэтому текст извлекается устойчивой эвристикой, а сырой ответ сохраняется рядом.

Включаем StructMem-режим (`extraction_mode="event"`) — событийная экстракция с временными и причинными связями ровно соответствует нашим эпизодам `[состояние → действие → результат]`.

## Задание 2.1

### `core/memory/service.py`

```python
# harness/core/memory/service.py
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import structlog

from ..config import settings

log = structlog.get_logger(__name__)


@dataclass
class MemoryRecord:
    """Нормализованная запись памяти: текст + сырой ответ библиотеки."""
    text: str
    raw: Any = None


@dataclass
class ConsolidationResult:
    updated: bool
    summaries: list[str] = field(default_factory=list)
    raw_summary: Any = None


def build_lightmem_config() -> dict[str, Any]:
    """Production-конфиг LightMem из settings (см. README библиотеки)."""
    base_url_key = f"{settings.lightmem_llm_backend}_base_url"
    config: dict[str, Any] = {
        # pre-compression включаем только если скачана llmlingua-2
        "pre_compress": bool(settings.lightmem_llmlingua_path),
        "topic_segment": False,
        "messages_use": "user_only",
        "metadata_generate": True,
        "text_summary": True,
        "extraction_mode": "event",            # StructMem: событийные связи
        "memory_manager": {
            "model_name": settings.lightmem_llm_backend,
            "configs": {
                "model": settings.lightmem_llm_model,
                "api_key": settings.lightmem_llm_api_key,
                "max_tokens": 16000,
                base_url_key: settings.lightmem_llm_base_url,
            },
        },
        "extract_threshold": 0.5,
        "index_strategy": "embedding",
        "retrieve_strategy": "embedding",
        "text_embedder": {
            "model_name": "huggingface",
            "configs": {
                "model": settings.lightmem_embedding_model,
                "embedding_dims": settings.lightmem_embedding_dims,
                "model_kwargs": {"device": settings.lightmem_device},
            },
        },
        "embedding_retriever": {
            "model_name": "qdrant",
            "configs": {
                "collection_name": "harness_episodes",
                "embedding_model_dims": settings.lightmem_embedding_dims,
                "path": f"{settings.lightmem_dir}/episodes",
            },
        },
        "summary_retriever": {
            "model_name": "qdrant",
            "configs": {
                "collection_name": "harness_summaries",
                "embedding_model_dims": settings.lightmem_embedding_dims,
                "path": f"{settings.lightmem_dir}/summaries",
            },
        },
        "update": "offline",
    }
    if settings.lightmem_llmlingua_path:
        config["pre_compressor"] = {
            "model_name": "llmlingua-2",
            "configs": {
                "llmlingua_config": {
                    "model_name": settings.lightmem_llmlingua_path,
                    "device_map": settings.lightmem_device,
                    "use_llmlingua2": True,
                },
            },
        }
    return config


class MemoryService:
    """
    Единственная точка доступа харнесса к памяти.

    Сегодня внутри LightMem; при выходе FluxMem-кода меняется только
    этот класс (см. «Миграция на FluxMem»). Контракт стабилен:
      add_episode() / retrieve() / consolidate()
    """

    def __init__(self, client: Any) -> None:
        self._client = client                  # LightMemory или тестовый дублёр
        self._lock = asyncio.Lock()
        self.last_summaries: list[str] = []

    @classmethod
    async def create(cls) -> "MemoryService":
        """Production-инициализация. Ленивый импорт — тяжёлая зависимость."""
        def _build():
            from lightmem.memory.lightmem import LightMemory
            return LightMemory.from_config(build_lightmem_config())

        client = await asyncio.to_thread(_build)
        log.info("memory.initialized", backend="lightmem",
                 dir=settings.lightmem_dir)
        return cls(client=client)

    # ------------------------------------------------------------------
    # Запись
    # ------------------------------------------------------------------

    async def add_episode(
        self, content: str, success: bool, ts: float | None = None,
    ) -> Any:
        """Эпизод [действие → результат] → память (Stage I библиотеки)."""
        when = (
            datetime.fromtimestamp(ts, tz=timezone.utc)
            if ts else datetime.now(timezone.utc)
        ).strftime("%Y-%m-%d")

        message = {
            "role": "user",
            "content": f"[{'SUCCESS' if success else 'FAILURE'}] {content}",
            "time_stamp": when,
        }
        async with self._lock:
            result = await asyncio.to_thread(
                self._client.add_memory,
                messages=[message],
                force_segment=True,
                force_extract=True,
            )
        log.debug("memory.episode_added", success=success)
        return result

    # ------------------------------------------------------------------
    # Чтение (pull-модель, ТРИЗ Приём #25)
    # ------------------------------------------------------------------

    async def retrieve(self, query: str, limit: int = 5) -> list[MemoryRecord]:
        async with self._lock:
            raw = await asyncio.to_thread(self._client.retrieve, query, limit=limit)
        records = [MemoryRecord(text=t, raw=raw) for t in _extract_texts(raw)]
        log.debug("memory.retrieved", query=query, count=len(records))
        return records[:limit]

    # ------------------------------------------------------------------
    # Консолидация (Stage II + III библиотеки)
    # ------------------------------------------------------------------

    async def consolidate(
        self,
        score_threshold: float = 0.8,
        time_window: int = 3600,
        top_k: int = 15,
    ) -> ConsolidationResult:
        """
        Stage II: offline-update очереди записей (дедупликация, слияние).
        Stage III: кросс-событийные сводки (StructMem) — аналог 𝒱_proc.
        """
        async with self._lock:
            await asyncio.to_thread(self._client.construct_update_queue_all_entries)
            await asyncio.to_thread(
                self._client.offline_update_all_entries,
                score_threshold=score_threshold,
            )
            raw_summary = await asyncio.to_thread(
                self._client.summarize,
                retrieval_scope="global",
                time_window=time_window,
                top_k=top_k,
                process_all=True,
            )

        summaries = _extract_texts(raw_summary)
        self.last_summaries = summaries
        log.info("memory.consolidated", summaries=len(summaries))
        return ConsolidationResult(updated=True, summaries=summaries,
                                   raw_summary=raw_summary)


def _extract_texts(payload: Any) -> list[str]:
    """Устойчивое извлечение текстов из ответов LightMem (формат зависит
    от версии библиотеки — фиксируем эвристику в одном месте)."""
    if payload is None:
        return []
    if isinstance(payload, str):
        return [payload] if payload.strip() else []
    if isinstance(payload, dict):
        for key in ("summaries", "results", "memories", "entries", "data"):
            if key in payload:
                return _extract_texts(payload[key])
        for key in ("summary", "memory", "text", "content"):
            if isinstance(payload.get(key), str):
                return [payload[key]]
        return []
    if isinstance(payload, (list, tuple)):
        texts: list[str] = []
        for item in payload:
            texts.extend(_extract_texts(item))
        return texts
    return [str(payload)]
```

---

---

# Урок 2.2 — TrustBridge: trust_records → память

## Теория

Хук Фазы 1: «`TrustLedger.history()` → FluxMem 𝒱_epi». История действий агентов уже лежит в `trust_records` — это готовые эпизоды `[агент → действие → результат]`. Мост переливает их в память **идемпотентно**: watermark (id последней импортированной записи) хранится в `StateStore` Фазы 1 — попадает и в Redis, и в аудит-лог Postgres, ничего нового не изобретаем.

Схема `trust_records` не меняется — ключевая конвенция Фазы 1.

## Задание 2.2

### `core/memory/bridge.py`

```python
# harness/core/memory/bridge.py
from __future__ import annotations

import json

import asyncpg
import structlog

from ..state_store import StateStore
from .service import MemoryService

log = structlog.get_logger(__name__)

WATERMARK_KEY = "bridge:trust_watermark"


class TrustBridge:
    """
    trust_records (Фаза 1) → эпизоды памяти (Фаза 2).

    Идемпотентность: watermark в StateStore (Redis + аудит в Postgres).
    Схема trust_records НЕ меняется.
    """

    def __init__(
        self,
        memory: MemoryService,
        trust_pg_pool: asyncpg.Pool,
        state: StateStore,
    ) -> None:
        self._memory = memory
        self._trust_pg = trust_pg_pool
        self._state = state

    async def sync(self, batch: int = 500) -> int:
        watermark = int(await self._state.get(WATERMARK_KEY, 0))
        rows = await self._trust_pg.fetch(
            """
            SELECT id, ts, agent_id, action, success, trust_level, context
            FROM trust_records
            WHERE id > $1
            ORDER BY id
            LIMIT $2
            """,
            watermark, batch,
        )
        for r in rows:
            raw_ctx = r["context"]
            ctx = json.loads(raw_ctx) if isinstance(raw_ctx, str) and raw_ctx else (raw_ctx or {})
            ctx_part = f" context={json.dumps(ctx, ensure_ascii=False)}" if ctx else ""
            content = (
                f"agent={r['agent_id']} action={r['action']} "
                f"result={'success' if r['success'] else 'failure'} "
                f"level={r['trust_level']}{ctx_part}"
            )
            await self._memory.add_episode(content, success=r["success"], ts=r["ts"])

        if rows:
            await self._state.set(WATERMARK_KEY, rows[-1]["id"])
            log.info("bridge.synced", imported=len(rows), watermark=rows[-1]["id"])
        return len(rows)
```

---

---

# Урок 2.3 — PEMS-lite и губернатор доверия

## Теория

PEMS в LightMem **нет** — метрика появится с релизом FluxMem-кода. Но связка «зрелость памяти → потолок доверия» — это наша архитектура из AGENTS.md §2.3, без неё Фаза 2 теряет смысл для харнесса. Поэтому считаем **PEMS-lite**: та же форма формулы, честные прокси вместо внутренних величин графа:

$$\text{PEMS-lite} = \frac{\eta}{\log_{10} \bar{\ell}} \times (1 - \delta)$$

| Величина | В статье FluxMem | Прокси в PEMS-lite | Чем заменится после миграции |
|---|---|---|---|
| η | успешность схем 𝒱_proc | success-rate агента из окна `TrustLedger` (Фаза 1) | родная η из библиотеки |
| ℓ̄ | длина схем в токенах | средняя длина сводок `consolidate()` (`len/4`, пол 10) | родная ℓ |
| δ | расстояние графа k vs k−1 | 1 − Jaccard(токены сводок, предыдущий замер) | родная δ |

Без эмбеддингов и кластеризации — никакой повторной реализации FluxMem, только арифметика над тем, что уже отдают Фаза 1 и LightMem. История замеров — таблица `pems_history` в Postgres.

`MaturityGovernor` не меняет уровни (это делает окно `TrustLedger`) — он наблюдает и эскалирует:
- уровень агента выше PEMS-потолка → WARNING;
- падение PEMS >30% между замерами (память расходится) → CRITICAL;
- значение → `MonitoringLayer.log_pems()` — gauge Фазы 1 получает реальные числа.

| PEMS-lite | δ | Потолок доверия |
|------|---|-----------------|
| < 0.25 | — | Level 2 (SPECIALIST) |
| ≥ 0.25 | — | Level 3 (ORCHESTRATOR) |
| ≥ 0.50 | ≤ 0.10 | Level 4 (SUPERVISOR) |

## Задание 2.3

### `core/memory/pems.py`

```python
# harness/core/memory/pems.py
from __future__ import annotations

import json
import math
import re
import time
from dataclasses import dataclass, field
from typing import Any

import asyncpg
import structlog

from ..config import settings
from ..escalation import EscalationChannel, EscalationSeverity
from ..monitoring import MonitoringLayer
from ..trust import TrustLedger, TrustLevel

log = structlog.get_logger(__name__)

# DDL
_INIT_SQL = """
CREATE TABLE IF NOT EXISTS pems_history (
    id            BIGSERIAL PRIMARY KEY,
    ts            DOUBLE PRECISION NOT NULL,
    agent_id      TEXT NOT NULL,
    eta           DOUBLE PRECISION NOT NULL,
    avg_tokens    DOUBLE PRECISION NOT NULL,
    delta         DOUBLE PRECISION NOT NULL,
    pems          DOUBLE PRECISION NOT NULL,
    summary_count INTEGER NOT NULL,
    summaries     JSONB NOT NULL DEFAULT '[]'
);

CREATE INDEX IF NOT EXISTS idx_pems_agent ON pems_history(agent_id, ts DESC);
"""


@dataclass
class PEMSReport:
    agent_id: str
    eta: float
    avg_tokens: float
    delta: float
    pems: float
    summary_count: int
    ts: float = field(default_factory=time.time)


class PEMSLite:
    """
    ВРЕМЕННАЯ метрика зрелости памяти (до релиза FluxMem-кода).

    PEMS-lite = (η / log₁₀(ℓ̄)) × (1 − δ)
      η  — success-rate агента из окна TrustLedger (прокси)
      ℓ̄  — средняя длина сводок consolidate() в токенах, пол MIN_TOKENS
      δ  — 1 − Jaccard токенов сводок (k vs k−1)

    После миграции на FluxMem класс заменяется родным PEMS библиотеки;
    интерфейс compute()/last()/suggest_trust_cap() сохраняется.
    """

    HIGH_PEMS = 0.50
    MID_PEMS = 0.25
    STABLE_DELTA = 0.10

    def __init__(
        self,
        ledger: TrustLedger,
        pg_pool: asyncpg.Pool,
        monitor: MonitoringLayer | None = None,
    ) -> None:
        self._ledger = ledger
        self._pg = pg_pool
        self._monitor = monitor

    async def compute(
        self,
        agent_id: str,
        summaries: list[str],
        record: bool = True,
    ) -> PEMSReport:
        eta = await self._ledger.success_rate(agent_id)

        if not summaries or eta <= 0.0:
            report = PEMSReport(agent_id=agent_id, eta=round(eta, 4),
                                avg_tokens=0.0, delta=0.0, pems=0.0,
                                summary_count=len(summaries))
        else:
            avg_tokens = sum(max(len(s) / 4, 1.0) for s in summaries) / len(summaries)
            ell = max(avg_tokens, settings.pems_min_tokens)

            previous = await self.last(agent_id, 1)
            delta = (
                self._jaccard_delta(summaries, previous[0]["summaries"])
                if previous else 0.0
            )

            pems = (eta / math.log10(ell)) * (1 - delta)
            report = PEMSReport(
                agent_id=agent_id,
                eta=round(eta, 4),
                avg_tokens=round(avg_tokens, 1),
                delta=round(delta, 4),
                pems=round(pems, 4),
                summary_count=len(summaries),
            )

        if record:
            await self._pg.execute(
                """
                INSERT INTO pems_history
                    (ts, agent_id, eta, avg_tokens, delta, pems,
                     summary_count, summaries)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                report.ts, agent_id, report.eta, report.avg_tokens,
                report.delta, report.pems, report.summary_count,
                json.dumps(summaries, ensure_ascii=False),
            )
        if self._monitor is not None:
            self._monitor.log_pems(report.pems, agent=agent_id)

        log.info("pems.computed", agent=agent_id, pems=report.pems,
                 eta=report.eta, delta=report.delta)
        return report

    async def last(self, agent_id: str, n: int = 1) -> list[dict[str, Any]]:
        rows = await self._pg.fetch(
            """
            SELECT ts, eta, avg_tokens, delta, pems, summary_count, summaries
            FROM pems_history
            WHERE agent_id = $1
            ORDER BY ts DESC LIMIT $2
            """,
            agent_id, n,
        )
        out = []
        for r in rows:
            d = dict(r)
            if isinstance(d["summaries"], str):   # asyncpg: JSONB → строка
                d["summaries"] = json.loads(d["summaries"])
            out.append(d)
        return out

    @staticmethod
    def _jaccard_delta(current: list[str], previous: list[str]) -> float:
        a = set(re.findall(r"\w+", " ".join(current).lower()))
        b = set(re.findall(r"\w+", " ".join(previous).lower()))
        if not a and not b:
            return 0.0
        if not a or not b:
            return 1.0
        return 1.0 - len(a & b) / len(a | b)

    @classmethod
    def suggest_trust_cap(cls, report: PEMSReport) -> TrustLevel:
        """PEMS → потолок уровня доверия (AGENTS.md §2.3)."""
        if report.pems >= cls.HIGH_PEMS and report.delta <= cls.STABLE_DELTA:
            return TrustLevel.SUPERVISOR
        if report.pems >= cls.MID_PEMS:
            return TrustLevel.ORCHESTRATOR
        return TrustLevel.SPECIALIST


class MaturityGovernor:
    """
    Связка Фазы 1 и Фазы 2: PEMS ↔ TrustLedger ↔ EscalationChannel.

    Не меняет уровни напрямую (это делает окно TrustLedger) —
    наблюдает, эскалирует, пишет метрики. Орган управления по Закону 1.
    """

    PEMS_DROP_RATIO = 0.30   # падение >30% между замерами → CRITICAL

    def __init__(
        self,
        calculator: PEMSLite,
        ledger: TrustLedger,
        escalation: EscalationChannel,
    ) -> None:
        self._calc = calculator
        self._ledger = ledger
        self._escalation = escalation

    async def review(self, agent_id: str, summaries: list[str]) -> PEMSReport:
        previous = await self._calc.last(agent_id, 1)     # состояние ДО замера
        report = await self._calc.compute(agent_id, summaries, record=True)

        # (а) уровень доверия выше PEMS-потолка → WARNING
        cap = PEMSLite.suggest_trust_cap(report)
        current = await self._ledger.level(agent_id)
        if current < TrustLevel.HUMAN_ARCHITECT and int(current) > int(cap):
            await self._escalation.escalate(
                EscalationSeverity.WARNING,
                source="maturity_governor",
                message=(
                    f"Agent '{agent_id}' trust level {current.name} "
                    f"exceeds PEMS cap {cap.name}"
                ),
                context={"agent_id": agent_id, "pems": report.pems,
                         "delta": report.delta, "cap": cap.name},
            )

        # (б) деградация PEMS → CRITICAL (память расходится)
        if previous:
            prev_pems = previous[0]["pems"]
            if prev_pems > 0 and (prev_pems - report.pems) / prev_pems >= self.PEMS_DROP_RATIO:
                await self._escalation.escalate(
                    EscalationSeverity.CRITICAL,
                    source="maturity_governor",
                    message="PEMS degradation: memory is not converging",
                    context={"agent_id": agent_id,
                             "previous": prev_pems, "current": report.pems},
                )

        return report
```

---

---

# Урок 2.4 — Ночной цикл

## Теория

Пункты 5–6 интеграционного цикла AGENTS.md §4 целиком: импорт нового опыта → offline-update и сводки (библиотека) → PEMS-ревью (наш слой). LightMem спроектирован под `update="offline"` — батчевые обновления вместо обновления на каждом действии, что совпадает с «ночным циклом» из теории.

## Задание 2.4

### `core/memory/nightly.py`

```python
# harness/core/memory/nightly.py
from __future__ import annotations

from dataclasses import dataclass

import structlog

from .bridge import TrustBridge
from .pems import MaturityGovernor, PEMSReport
from .service import ConsolidationResult, MemoryService

log = structlog.get_logger(__name__)


@dataclass
class NightlyReport:
    imported: int
    consolidation: ConsolidationResult
    pems: PEMSReport


async def nightly_cycle(
    *,
    memory: MemoryService,
    bridge: TrustBridge,
    governor: MaturityGovernor,
    agent_id: str,
    score_threshold: float = 0.8,
) -> NightlyReport:
    """
    Полный ночной цикл (AGENTS.md §4, шаги 5–6):

    1. bridge.sync()        — новые trust_records → эпизоды памяти
    2. memory.consolidate() — Stage II (offline update) + Stage III (сводки)
    3. governor.review()    — PEMS → мониторинг, потолок доверия, эскалации
    """
    imported = await bridge.sync()
    consolidation = await memory.consolidate(score_threshold=score_threshold)
    pems = await governor.review(agent_id, consolidation.summaries)

    report = NightlyReport(imported=imported, consolidation=consolidation,
                           pems=pems)
    log.info("nightly.done", imported=imported,
             summaries=len(consolidation.summaries), pems=pems.pems)
    return report
```

---

### `core/memory/__init__.py`

```python
# harness/core/memory/__init__.py
from .bridge import TrustBridge
from .nightly import NightlyReport, nightly_cycle
from .pems import MaturityGovernor, PEMSLite, PEMSReport
from .service import (
    ConsolidationResult,
    MemoryRecord,
    MemoryService,
    build_lightmem_config,
)

__all__ = [
    "TrustBridge", "NightlyReport", "nightly_cycle",
    "MaturityGovernor", "PEMSLite", "PEMSReport",
    "ConsolidationResult", "MemoryRecord", "MemoryService",
    "build_lightmem_config",
]
```

---

---

# Тесты

Конвенции Фазы 1 сохраняются. Новое: `FakeLightMemory` — тестовый дублёр библиотеки (та же роль, что `fakeredis` в Фазе 1): unit-тесты проверяют **наш слой** быстро и без torch/qdrant/LLM. Качество самой библиотеки тестируют её авторы (LoCoMo-бенчмарки в README LightMem) — мы это не дублируем. Реальный LightMem проверяется отдельным smoke-тестом по флагу.

## `tests/conftest.py` — добавить

```python
# harness/tests/conftest.py  (добавить к фикстурам Фазы 1)
import re

from core.memory.service import MemoryService


class FakeLightMemory:
    """
    Тестовый дублёр LightMemory (аналог fakeredis из Фазы 1).
    Повторяет публичный API: add_memory / retrieve /
    construct_update_queue_all_entries / offline_update_all_entries / summarize.
    """

    def __init__(self):
        self.entries: list[dict] = []
        self.summaries: list[str] = []
        self.update_queue_built = False
        self.offline_updated = False

    def add_memory(self, messages, force_segment=True, force_extract=True, **kw):
        self.entries.extend(messages)
        return {"stored": len(messages)}

    def retrieve(self, question, limit=5):
        q_tokens = set(re.findall(r"\w+", question.lower()))

        def score(entry):
            tokens = set(re.findall(r"\w+", entry["content"].lower()))
            return len(q_tokens & tokens)

        ranked = sorted(self.entries, key=score, reverse=True)
        return [{"memory": e["content"]} for e in ranked[:limit] if score(e) > 0]

    def construct_update_queue_all_entries(self):
        self.update_queue_built = True

    def offline_update_all_entries(self, score_threshold=0.8):
        self.offline_updated = True

    def summarize(self, **kwargs):
        if not self.entries:
            self.summaries = []
            return {"summaries": []}
        joined = " ".join(e["content"] for e in self.entries)
        tokens = re.findall(r"\w+", joined.lower())
        common = [t for t, _ in __import__("collections").Counter(tokens).most_common(8)
                  if len(t) > 2]
        self.summaries = [f"Summary: recurring pattern across "
                          f"{len(self.entries)} episodes: {' '.join(common)}"]
        return {"summaries": self.summaries}


@pytest.fixture
def fake_lightmem():
    return FakeLightMemory()


@pytest.fixture
def memory_service(fake_lightmem):
    return MemoryService(client=fake_lightmem)
```

И в `pytest.ini` — новый маркер:

```ini
[pytest]
asyncio_mode = auto
markers =
    eval: LLM / behavior evaluation tests
    integration: integration tests requiring external services
    lightmem_smoke: smoke tests requiring installed lightmem + models
```

---

## `tests/unit/test_memory_service.py`

```python
# harness/tests/unit/test_memory_service.py
import pytest
from core.memory.service import MemoryService, _extract_texts, build_lightmem_config


class TestAddEpisode:
    async def test_success_episode_is_tagged(self, memory_service, fake_lightmem):
        await memory_service.add_episode("fetched invoice from billing api",
                                         success=True)
        assert len(fake_lightmem.entries) == 1
        entry = fake_lightmem.entries[0]
        assert entry["content"].startswith("[SUCCESS]")
        assert entry["role"] == "user"
        assert "time_stamp" in entry

    async def test_failure_episode_is_tagged(self, memory_service, fake_lightmem):
        await memory_service.add_episode("deploy crashed", success=False)
        assert fake_lightmem.entries[0]["content"].startswith("[FAILURE]")

    async def test_ts_formatted_as_date(self, memory_service, fake_lightmem):
        await memory_service.add_episode("x", success=True, ts=1750000000.0)
        assert fake_lightmem.entries[0]["time_stamp"] == "2025-06-15"


class TestRetrieve:
    async def test_returns_normalized_records(self, memory_service):
        await memory_service.add_episode("payments api retry on 429", success=True)
        await memory_service.add_episode("kubernetes pod scheduling", success=True)

        records = await memory_service.retrieve("payments api retry", limit=5)
        assert records
        assert "payments" in records[0].text
        assert records[0].raw is not None

    async def test_limit_respected(self, memory_service):
        for i in range(10):
            await memory_service.add_episode(f"payments run {i}", success=True)
        records = await memory_service.retrieve("payments", limit=3)
        assert len(records) <= 3


class TestConsolidate:
    async def test_runs_update_then_summarize(self, memory_service, fake_lightmem):
        await memory_service.add_episode("fetch invoice from billing api",
                                         success=True)
        result = await memory_service.consolidate()

        assert fake_lightmem.update_queue_built       # Stage II
        assert fake_lightmem.offline_updated
        assert result.summaries                       # Stage III
        assert memory_service.last_summaries == result.summaries

    async def test_empty_memory_gives_no_summaries(self, memory_service):
        result = await memory_service.consolidate()
        assert result.summaries == []


class TestExtractTexts:
    def test_handles_dict_with_summaries(self):
        assert _extract_texts({"summaries": ["a", "b"]}) == ["a", "b"]

    def test_handles_list_of_memory_dicts(self):
        assert _extract_texts([{"memory": "x"}, {"text": "y"}]) == ["x", "y"]

    def test_handles_nested_results(self):
        assert _extract_texts({"results": [{"content": "z"}]}) == ["z"]

    def test_handles_none_and_empty(self):
        assert _extract_texts(None) == []
        assert _extract_texts("") == []
        assert _extract_texts({}) == []


class TestConfig:
    def test_config_built_from_settings(self):
        config = build_lightmem_config()
        assert config["update"] == "offline"
        assert config["extraction_mode"] == "event"
        assert config["embedding_retriever"]["configs"]["collection_name"] == "harness_episodes"
        assert config["summary_retriever"]["configs"]["collection_name"] == "harness_summaries"

    def test_precompress_off_without_model_path(self):
        config = build_lightmem_config()
        assert config["pre_compress"] is False        # llmlingua не скачана
        assert "pre_compressor" not in config
```

---

## `tests/unit/test_bridge.py`

```python
# harness/tests/unit/test_bridge.py
import pytest
import pytest_asyncio

from core.memory.bridge import WATERMARK_KEY, TrustBridge
from core.state_store import StateStore, _INIT_SQL as STATE_SQL
from core.trust import TrustLedger, TrustLevel, _INIT_SQL as TRUST_SQL


@pytest_asyncio.fixture
async def stack(fake_redis, pg_pool, memory_service):
    async with pg_pool.acquire() as conn:
        await conn.execute(STATE_SQL)
        await conn.execute(TRUST_SQL)
        await conn.execute("TRUNCATE trust_records RESTART IDENTITY")

    state = StateStore(redis=fake_redis, pg_pool=pg_pool, session_id="bridge-test")
    ledger = TrustLedger(redis=fake_redis, pg_pool=pg_pool)
    bridge = TrustBridge(memory_service, pg_pool, state)
    return {"state": state, "ledger": ledger, "bridge": bridge}


class TestSync:
    async def test_imports_trust_records(self, stack, fake_lightmem):
        await stack["ledger"].register_agent("agent-a", TrustLevel.SPECIALIST)
        for i in range(5):
            await stack["ledger"].record("agent-a", "fetch_invoice", success=True,
                                         context={"run": i})

        imported = await stack["bridge"].sync()
        assert imported == 5
        assert len(fake_lightmem.entries) == 5
        assert "agent=agent-a" in fake_lightmem.entries[0]["content"]
        assert "action=fetch_invoice" in fake_lightmem.entries[0]["content"]
        assert fake_lightmem.entries[0]["content"].startswith("[SUCCESS]")

    async def test_idempotent_second_sync(self, stack):
        await stack["ledger"].record("agent-b", "act", success=True)
        assert await stack["bridge"].sync() == 1
        assert await stack["bridge"].sync() == 0          # watermark

    async def test_watermark_persisted_in_state_store(self, stack):
        await stack["ledger"].record("agent-c", "act", success=True)
        await stack["bridge"].sync()
        watermark = await stack["state"].get(WATERMARK_KEY)
        assert watermark >= 1

    async def test_failure_records_tagged(self, stack, fake_lightmem):
        await stack["ledger"].record("agent-d", "deploy", success=False)
        await stack["bridge"].sync()
        assert fake_lightmem.entries[-1]["content"].startswith("[FAILURE]")
        assert "result=failure" in fake_lightmem.entries[-1]["content"]

    async def test_batch_limit(self, stack):
        for i in range(7):
            await stack["ledger"].record("agent-e", f"act_{i}", success=True)
        assert await stack["bridge"].sync(batch=3) == 3
        assert await stack["bridge"].sync(batch=10) == 4   # дочитал остаток
```

---

## `tests/unit/test_pems.py`

```python
# harness/tests/unit/test_pems.py
import pytest
import pytest_asyncio

from core.memory.pems import PEMSLite, PEMSReport, _INIT_SQL as PEMS_SQL
from core.trust import TrustLedger, TrustLevel, _INIT_SQL as TRUST_SQL


@pytest_asyncio.fixture
async def ledger(fake_redis, pg_pool):
    async with pg_pool.acquire() as conn:
        await conn.execute(TRUST_SQL)
        await conn.execute(PEMS_SQL)
        await conn.execute("TRUNCATE trust_records, pems_history RESTART IDENTITY")
    return TrustLedger(redis=fake_redis, pg_pool=pg_pool)


@pytest_asyncio.fixture
async def calc(ledger, pg_pool):
    return PEMSLite(ledger=ledger, pg_pool=pg_pool)


async def _seed_rate(ledger, agent_id, successes, failures):
    await ledger.register_agent(agent_id)
    for i in range(successes):
        await ledger.record(agent_id, f"ok_{i}", success=True)
    for i in range(failures):
        await ledger.record(agent_id, f"fail_{i}", success=False)


class TestFormula:
    async def test_no_summaries_gives_zero(self, calc, ledger):
        await _seed_rate(ledger, "a1", 10, 0)
        report = await calc.compute("a1", summaries=[], record=False)
        assert report.pems == 0.0
        assert report.summary_count == 0

    async def test_no_history_gives_zero(self, calc, ledger):
        await ledger.register_agent("a2")               # окно пустое → η = 0
        report = await calc.compute("a2", summaries=["x" * 400], record=False)
        assert report.pems == 0.0

    async def test_known_values(self, calc, ledger):
        # η = 9/10 = 0.9 (окно ledger), сводка 400 символов → 100 токенов
        # log₁₀(100) = 2 → PEMS = 0.9 / 2 = 0.45 (первый замер, δ=0)
        await _seed_rate(ledger, "a3", 9, 1)
        report = await calc.compute("a3", summaries=["x" * 400], record=False)
        assert report.eta == pytest.approx(0.9)
        assert report.avg_tokens == pytest.approx(100.0)
        assert report.delta == 0.0
        assert report.pems == pytest.approx(0.45, rel=1e-3)

    async def test_stable_summaries_keep_delta_zero(self, calc, ledger):
        await _seed_rate(ledger, "a4", 10, 0)
        summaries = ["billing api retry pattern works reliably"]
        first = await calc.compute("a4", summaries, record=True)
        second = await calc.compute("a4", summaries, record=True)
        assert second.delta == pytest.approx(0.0)
        assert second.pems == pytest.approx(first.pems, rel=1e-6)

    async def test_changed_summaries_penalize(self, calc, ledger):
        await _seed_rate(ledger, "a5", 10, 0)
        await calc.compute("a5", ["billing api retry pattern"], record=True)
        report = await calc.compute(
            "a5", ["completely different navigation heuristics"], record=True,
        )
        assert report.delta > 0.5
        assert report.pems < 0.45

    async def test_history_recorded(self, calc, ledger):
        await _seed_rate(ledger, "a6", 10, 0)
        await calc.compute("a6", ["summary one"], record=True)
        history = await calc.last("a6", 1)
        assert history and history[0]["summaries"] == ["summary one"]


class TestJaccardDelta:
    def test_identical_sets(self):
        assert PEMSLite._jaccard_delta(["a b c"], ["a b c"]) == 0.0

    def test_disjoint_sets(self):
        assert PEMSLite._jaccard_delta(["aaa bbb"], ["ccc ddd"]) == 1.0

    def test_both_empty(self):
        assert PEMSLite._jaccard_delta([], []) == 0.0

    def test_one_empty(self):
        assert PEMSLite._jaccard_delta(["aaa"], []) == 1.0


class TestTrustCap:
    def _report(self, pems, delta):
        return PEMSReport(agent_id="x", eta=0.0, avg_tokens=50.0,
                          delta=delta, pems=pems, summary_count=3)

    def test_high_stable_caps_at_supervisor(self):
        assert PEMSLite.suggest_trust_cap(self._report(0.55, 0.05)) == TrustLevel.SUPERVISOR

    def test_high_unstable_caps_at_orchestrator(self):
        assert PEMSLite.suggest_trust_cap(self._report(0.55, 0.30)) == TrustLevel.ORCHESTRATOR

    def test_mid_caps_at_orchestrator(self):
        assert PEMSLite.suggest_trust_cap(self._report(0.30, 0.0)) == TrustLevel.ORCHESTRATOR

    def test_low_caps_at_specialist(self):
        assert PEMSLite.suggest_trust_cap(self._report(0.10, 0.0)) == TrustLevel.SPECIALIST
```

---

## `tests/unit/test_governor.py`

```python
# harness/tests/unit/test_governor.py
import pytest
import pytest_asyncio

from core.escalation import EscalationChannel, EscalationSeverity
from core.memory.pems import MaturityGovernor, PEMSLite, _INIT_SQL as PEMS_SQL
from core.monitoring import MonitoringLayer
from core.trust import TrustLedger, TrustLevel, _INIT_SQL as TRUST_SQL


@pytest_asyncio.fixture
async def stack(fake_redis, pg_pool, monitor):
    async with pg_pool.acquire() as conn:
        await conn.execute(TRUST_SQL)
        await conn.execute(PEMS_SQL)
        await conn.execute("TRUNCATE trust_records, pems_history RESTART IDENTITY")

    ledger = TrustLedger(redis=fake_redis, pg_pool=pg_pool)
    escalation = EscalationChannel()
    calc = PEMSLite(ledger=ledger, pg_pool=pg_pool, monitor=monitor)
    governor = MaturityGovernor(calc, ledger, escalation)
    return {"ledger": ledger, "escalation": escalation,
            "calc": calc, "governor": governor}


class TestOvertrust:
    async def test_overtrusted_agent_escalates_warning(self, stack):
        """SUPERVISOR при нулевом PEMS → WARNING."""
        s = stack
        await s["ledger"].register_agent("over", TrustLevel.SUPERVISOR)
        report = await s["governor"].review("over", summaries=[])

        assert report.pems == 0.0
        pending = s["escalation"].pending()
        assert len(pending) == 1
        assert pending[0].severity == EscalationSeverity.WARNING
        assert pending[0].source == "maturity_governor"
        assert pending[0].context["cap"] == "SPECIALIST"

    async def test_mature_agent_passes_clean(self, stack):
        """Зрелая память, уровень в пределах потолка → эскалаций нет."""
        s = stack
        await s["ledger"].register_agent("mature", TrustLevel.SPECIALIST)
        for i in range(12):
            await s["ledger"].record("mature", f"act_{i}", success=True)

        report = await s["governor"].review(
            "mature", summaries=["billing api retry pattern works"])
        assert report.pems > 0
        assert s["escalation"].pending() == []


class TestDegradation:
    async def test_pems_drop_escalates_critical(self, stack):
        s = stack
        await s["ledger"].register_agent("degrading", TrustLevel.TOOL_EXECUTOR)
        for i in range(12):
            await s["ledger"].record("degrading", f"act_{i}", success=True)

        # Первый замер — здоровый
        await s["governor"].review("degrading",
                                   summaries=["stable summary pattern"])
        assert s["escalation"].pending() == []

        # Второй — память «рассыпалась» (пустые сводки → PEMS = 0)
        await s["governor"].review("degrading", summaries=[])
        severities = [e.severity for e in s["escalation"].pending()]
        assert EscalationSeverity.CRITICAL in severities


class TestMonitoring:
    async def test_gauge_receives_pems(self, stack, monitor):
        s = stack
        await s["ledger"].register_agent("gauged", TrustLevel.TOOL_EXECUTOR)
        for i in range(10):
            await s["ledger"].record("gauged", f"act_{i}", success=True)

        report = await s["governor"].review(
            "gauged", summaries=["some consolidated pattern"])
        value = monitor.pems_score.labels(agent="gauged")._value.get()
        assert value == pytest.approx(report.pems)
```

---

## `tests/integration/test_memory_cycle.py`

```python
# harness/tests/integration/test_memory_cycle.py
"""
Полный цикл Фазы 2:
trust_records → bridge → память → consolidate → PEMS → governor.

Основной тест — на FakeLightMemory (быстрый, без torch/LLM).
Smoke-тест с реальным LightMem — по маркеру lightmem_smoke.
"""
import os

import pytest
import pytest_asyncio

from core.escalation import EscalationChannel, EscalationSeverity
from core.memory.bridge import TrustBridge
from core.memory.nightly import nightly_cycle
from core.memory.pems import MaturityGovernor, PEMSLite, _INIT_SQL as PEMS_SQL
from core.state_store import StateStore, _INIT_SQL as STATE_SQL
from core.trust import TrustLedger, TrustLevel, _INIT_SQL as TRUST_SQL


@pytest_asyncio.fixture
async def stack(fake_redis, pg_pool, monitor, memory_service):
    async with pg_pool.acquire() as conn:
        await conn.execute(STATE_SQL)
        await conn.execute(TRUST_SQL)
        await conn.execute(PEMS_SQL)
        await conn.execute("TRUNCATE trust_records, pems_history RESTART IDENTITY")

    state = StateStore(redis=fake_redis, pg_pool=pg_pool, session_id="cycle")
    ledger = TrustLedger(redis=fake_redis, pg_pool=pg_pool)
    escalation = EscalationChannel()
    calc = PEMSLite(ledger=ledger, pg_pool=pg_pool, monitor=monitor)

    return {
        "memory": memory_service,
        "ledger": ledger,
        "escalation": escalation,
        "bridge": TrustBridge(memory_service, pg_pool, state),
        "governor": MaturityGovernor(calc, ledger, escalation),
    }


@pytest.mark.asyncio
async def test_full_memory_cycle(stack, monitor):
    """История доверия → память → сводки → PEMS → gauge, без эскалаций."""
    s = stack
    agent_id = "billing-agent"

    # Фаза 1: агент работает, ledger пишет историю
    await s["ledger"].register_agent(agent_id, TrustLevel.SPECIALIST)
    for i in range(12):
        await s["ledger"].record(agent_id, "fetch_invoice", success=True,
                                 context={"run": i})

    report = await nightly_cycle(
        memory=s["memory"],
        bridge=s["bridge"],
        governor=s["governor"],
        agent_id=agent_id,
    )

    assert report.imported == 12
    assert report.consolidation.summaries
    assert report.pems.pems > 0

    # Память отвечает на запрос о прошлом опыте
    records = await s["memory"].retrieve("fetch_invoice", limit=5)
    assert records and "fetch_invoice" in records[0].text

    # PEMS дошёл до Prometheus gauge Фазы 1
    gauge = monitor.pems_score.labels(agent=agent_id)._value.get()
    assert gauge == pytest.approx(report.pems.pems)

    # Повторный цикл идемпотентен по мосту
    second = await nightly_cycle(
        memory=s["memory"], bridge=s["bridge"],
        governor=s["governor"], agent_id=agent_id,
    )
    assert second.imported == 0
    assert s["escalation"].pending() == []


@pytest.mark.asyncio
async def test_overtrusted_agent_caught_by_cycle(stack):
    """Агент на SUPERVISOR с провальной историей → WARNING из ночного цикла."""
    s = stack
    agent_id = "overtrusted"
    await s["ledger"].register_agent(agent_id, TrustLevel.SUPERVISOR)
    for i in range(6):
        await s["ledger"].record(agent_id, "deploy", success=False)

    await nightly_cycle(
        memory=s["memory"], bridge=s["bridge"],
        governor=s["governor"], agent_id=agent_id,
    )

    pending = s["escalation"].pending()
    assert any(
        e.severity == EscalationSeverity.WARNING
        and e.source == "maturity_governor"
        for e in pending
    )


# ---------------------------------------------------------------------------
# Smoke с реальным LightMem (требует установленной библиотеки и моделей)
# Запуск: LIGHTMEM_SMOKE=1 pytest -m lightmem_smoke
# ---------------------------------------------------------------------------

@pytest.mark.lightmem_smoke
@pytest.mark.skipif(
    not os.getenv("LIGHTMEM_SMOKE"),
    reason="LIGHTMEM_SMOKE not set (требует lightmem + HF-модели + LLM API)",
)
@pytest.mark.asyncio
async def test_real_lightmem_roundtrip(tmp_path):
    pytest.importorskip("lightmem")
    from core.config import settings
    from core.memory.service import MemoryService

    settings.lightmem_dir = str(tmp_path / "lightmem")
    service = await MemoryService.create()

    await service.add_episode(
        "fetched invoice 42 from billing api, retried on 429, parsed totals",
        success=True,
    )
    await service.add_episode(
        "fetched invoice 43 from billing api, cache hit, parsed totals",
        success=True,
    )

    records = await service.retrieve("how to fetch invoices from billing api")
    assert records, "real LightMem returned nothing"

    result = await service.consolidate()
    assert result.updated
```

---

---

# Evals

Тесты проверяют корректность нашего слоя. Evals проверяют **поведение системы**: правильно ли PEMS-lite ограничивает доверие и правильно ли губернатор реагирует на состояние памяти. Качество retrieval/консолидации самой библиотеки не дублируем — оно покрыто бенчмарками авторов (LoCoMo в README LightMem); наш слой отвечает только за интеграцию.

Инфраструктура — `eval_runner.py` из Фазы 1 без изменений.

## `tests/evals/eval_memory.py`

```python
# harness/tests/evals/eval_memory.py
"""
Eval: поведение слоя памяти Фазы 2.

1. PEMS → Trust — потолок доверия соответствует таблице зрелости AGENTS.md §2.3
2. Governance — губернатор корректно реагирует на сочетание
   (уровень доверия × состояние памяти)
"""
import pytest
import pytest_asyncio

from core.escalation import EscalationChannel, EscalationSeverity
from core.memory.pems import (
    MaturityGovernor, PEMSLite, PEMSReport, _INIT_SQL as PEMS_SQL,
)
from core.trust import TrustLedger, TrustLevel, _INIT_SQL as TRUST_SQL
from .eval_runner import EvalCase, EvalSuite


# ---------------------------------------------------------------------------
# 1. PEMS → Trust cap (таблица зрелости AGENTS.md §2.3)
# ---------------------------------------------------------------------------

PEMS_TRUST_CASES = [
    EvalCase(name="mature_stable_memory",
             input={"pems": 0.60, "delta": 0.05},
             expected_outcome="SUPERVISOR", tags=["pems"]),
    EvalCase(name="mature_but_diverging",
             input={"pems": 0.55, "delta": 0.30},
             expected_outcome="ORCHESTRATOR",
             description="δ↑ — память не сошлась, Level 4 рано", tags=["pems"]),
    EvalCase(name="growing_memory",
             input={"pems": 0.30, "delta": 0.15},
             expected_outcome="ORCHESTRATOR", tags=["pems"]),
    EvalCase(name="learning_phase",
             input={"pems": 0.10, "delta": 0.40},
             expected_outcome="SPECIALIST", tags=["pems"]),
    EvalCase(name="boundary_exact_mid",
             input={"pems": 0.25, "delta": 0.0},
             expected_outcome="ORCHESTRATOR",
             description="Граница MID включительно", tags=["pems", "boundary"]),
]


@pytest.mark.asyncio
@pytest.mark.eval
async def test_pems_trust_evals():
    suite = EvalSuite("PEMS → Trust Cap")

    async def runner(inp: dict) -> str:
        report = PEMSReport(agent_id="eval", eta=0.0, avg_tokens=50.0,
                            delta=inp["delta"], pems=inp["pems"],
                            summary_count=3)
        return PEMSLite.suggest_trust_cap(report).name

    report = await suite.run_all(PEMS_TRUST_CASES, runner)
    assert report["score"] >= 0.8, f"PEMS trust eval failed: {report}"


# ---------------------------------------------------------------------------
# 2. Governance: уровень доверия × состояние памяти → реакция губернатора
# ---------------------------------------------------------------------------

GOVERNANCE_CASES = [
    EvalCase(
        name="mature_specialist_clean",
        input={"level": TrustLevel.SPECIALIST, "successes": 12, "failures": 0,
               "summaries": ["billing api retry pattern works"]},
        expected_outcome="clear",
        description="Зрелая память, уровень в потолке → тишина",
        tags=["governance"],
    ),
    EvalCase(
        name="overtrusted_supervisor",
        input={"level": TrustLevel.SUPERVISOR, "successes": 0, "failures": 6,
               "summaries": []},
        expected_outcome="warning",
        description="Level 4 при нулевом PEMS → перекос доверия",
        tags=["governance", "safety"],
    ),
    EvalCase(
        name="orchestrator_with_thin_memory",
        input={"level": TrustLevel.ORCHESTRATOR, "successes": 12, "failures": 0,
               "summaries": []},
        expected_outcome="warning",
        description="Хорошая история, но память пуста → Level 3 не подтверждён",
        tags=["governance", "boundary"],
    ),
    EvalCase(
        name="tool_executor_learning",
        input={"level": TrustLevel.TOOL_EXECUTOR, "successes": 3, "failures": 3,
               "summaries": []},
        expected_outcome="clear",
        description="Level 1 ничего не нарушает даже с пустой памятью",
        tags=["governance"],
    ),
]


@pytest.mark.asyncio
@pytest.mark.eval
async def test_governance_evals(fake_redis, pg_pool, monitor):
    async with pg_pool.acquire() as conn:
        await conn.execute(TRUST_SQL)
        await conn.execute(PEMS_SQL)

    suite = EvalSuite("Maturity Governance")
    counter = {"n": 0}

    async def runner(inp: dict) -> str:
        counter["n"] += 1
        agent_id = f"eval-gov-{counter['n']}"

        ledger = TrustLedger(redis=fake_redis, pg_pool=pg_pool)
        escalation = EscalationChannel()
        calc = PEMSLite(ledger=ledger, pg_pool=pg_pool, monitor=monitor)
        governor = MaturityGovernor(calc, ledger, escalation)

        await ledger.register_agent(agent_id, inp["level"])
        for i in range(inp["successes"]):
            await ledger.record(agent_id, f"ok_{i}", success=True)
        for i in range(inp["failures"]):
            await ledger.record(agent_id, f"fail_{i}", success=False)
        # record() мог сдвинуть уровень — возвращаем сценарный
        await fake_redis.set(ledger._level_key(agent_id), int(inp["level"]))

        await governor.review(agent_id, summaries=inp["summaries"])

        pending = escalation.pending()
        if any(e.severity == EscalationSeverity.CRITICAL for e in pending):
            return "critical"
        if any(e.severity == EscalationSeverity.WARNING for e in pending):
            return "warning"
        return "clear"

    report = await suite.run_all(GOVERNANCE_CASES, runner)
    assert report["score"] >= 0.8, f"Governance eval failed: {report}"
```

---

## Запуск тестов и evals

```bash
cd harness

# Инфраструктура Фазы 1 (Redis + PostgreSQL) — этого достаточно
docker compose up -d

# Unit + integration (без LightMem: библиотека не нужна, работает дублёр)
pytest tests/unit/ tests/integration/ -v --cov=core --cov-report=term-missing

# Только Фаза 2
pytest tests/unit/test_memory_service.py tests/unit/test_bridge.py \
       tests/unit/test_pems.py tests/unit/test_governor.py \
       tests/integration/test_memory_cycle.py -v

# Evals
pytest tests/evals/ -v -m eval

# Smoke с реальным LightMem (установленная библиотека + модели + LLM API)
LIGHTMEM_SMOKE=1 LIGHTMEM_LLM_API_KEY=sk-... pytest -m lightmem_smoke -v
```

---

## Миграция на FluxMem

Авторы обещали выложить FluxMem-код в `github.com/zjunlp/LightMem`. Следить: страница News в README репозитория. Когда код появится:

**Что меняется (только 2 файла):**

1. `core/memory/service.py` — `MemoryService.create()` собирает FluxMem-объект вместо `LightMemory`; добавляются методы доступа к слоям, если API их даст (`get_strategy()` → 𝒱_proc и т.д.). Контракт `add_episode/retrieve/consolidate` сохраняется — остальной харнесс не трогается.
2. `core/memory/pems.py` — `PEMSLite` заменяется обёрткой над родным PEMS библиотеки. Интерфейс `compute()/last()/suggest_trust_cap()` сохраняется — `MaturityGovernor` и тесты потолка доверия не меняются. Таблица `pems_history` остаётся (история замеров не пропадает, меняется источник чисел).

**Что не меняется:** `TrustBridge`, `nightly_cycle`, `MaturityGovernor`, все компоненты Фазы 1, схемы `trust_records` / `state_history`.

**Чеклист миграции:**

- [ ] `pip install -U "lightmem @ git+https://github.com/zjunlp/LightMem.git"` — проверить, что FluxMem-модули появились
- [ ] Переписать `MemoryService.create()` под FluxMem-конфиг; прогнать `pytest tests/unit/test_memory_service.py`
- [ ] Заменить нутро `PEMSLite` на родной PEMS; обновить known-values в `test_pems.py` под родную формулу
- [ ] `LIGHTMEM_SMOKE=1 pytest -m lightmem_smoke` — реальный roundtrip
- [ ] Полный прогон: `pytest tests/ -v` + `pytest tests/evals/ -m eval`
- [ ] Обновить CLAUDE.md (раздел архитектуры памяти)

---

## Итог Фазы 2

```
harness/core/memory/
├── service.py   MemoryService — адаптер LightMem (граница миграции на FluxMem)
├── bridge.py    TrustBridge — trust_records → память (watermark в StateStore)
├── pems.py      PEMSLite (временный) + MaturityGovernor (потолок доверия)
└── nightly.py   ночной цикл: sync → consolidate → review
```

**Распределение ответственности:**

| Функция | Кто делает | Закон / Приём ТРИЗ |
|---|---|---|
| Хранение, индекс, экстракция эпизодов | LightMem (`add_memory`) | Триминг: функция → готовый компонент |
| Рефайнмент памяти | LightMem (`offline_update`) | — |
| Консолидация, сводки | LightMem + StructMem (`summarize`, event mode) | — |
| Импорт истории агентов | `TrustBridge` (наш) | Приём #25 (самообслуживание) |
| Метрика зрелости | `PEMSLite` (наш, временный) | — |
| Потолок доверия + эскалации | `MaturityGovernor` (наш) | Закон 1 (орган управления) |

**Закрытые хуки Фазы 1:**
- `trust_records` → эпизоды памяти через `TrustBridge` (схема не изменена, watermark в `StateStore`)
- `MonitoringLayer.log_pems()` получает реальные значения из `PEMSLite`
- `EscalationChannel` получает события от `MaturityGovernor` (перекос доверия → WARNING, деградация PEMS → CRITICAL)

**В Фазе 3 (RLM):**
- `MemoryService.retrieve()` → предзагрузка контекста для Root LM (шаг 2 интеграционного цикла)
- сводки `consolidate()` → starting point планирования (аналог 𝒱_proc до миграции)
- Root LM пишет результаты шагов через `add_episode()` — память пополняется из исполнения, не только из trust_records
- после миграции на FluxMem: `get_strategy()/get_episodes()/get_facts()` из родного API — соответствие слоёв RLM ↔ память из AGENTS.md §3.3
