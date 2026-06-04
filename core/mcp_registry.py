from __future__ import annotations

import asyncio
import structlog
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine

log = structlog.get_logger(__name__)

# minimum level trust required calling tool
# level 1 (read-only) - 5 (full level)
Trustlevel = int

@dataclass
class ToolDefinition:
    ''' Decription single tool in registry '''
    name: str
    decription: str
    min_trust_level: Trustlevel # minimum level for call tools
    handler: Callable[..., Coroutine[Any, Any, Any]] # async-function execute
    is_reversible: bool = True # is possible undo to action
    tags: list[str] = field(default_factory=list)

class MCPRegistry:
    ''' Harness tool registry
        Su-field solution: eliminates Defect 4 (interaction gap
        between the orchestrator and agents) '''

    def __init__(self) -> None:
        self._tools: dict[str, ToolDefinition] = {}

    # ----------------------------------------------------------
    # Registration
    # ----------------------------------------------------------

    def register(self, tool: ToolDefinition) -> None:
        ''' Added tool in registrey '''
        if tool.name in self._tools:
            raise ValueError(f'Tool {tool.name} already registered')
        self._tools[tool.name] = tool
        log.info('tool.registered', name=name, min_trust=tool.min_trust_level)

    def unregister(self, name: str) -> None:
        ''' Remove tool from registry '''
        self._tools.pop(name, None)
        log.info("tool.unregistered", name=name)

    # ----------------------------------------------------------
    # Request
    # ----------------------------------------------------------

    def available_for(self, trust_level: Trustlevel) -> list[ToolDefinition]:
        ''' Tools, available at the current trust level '''
        return [t for in self._tools.values() if t.min_trust_level <= trust_level]

    def get(self, name: str) -> ToolDefinition | None:
        return self._tools.get(name)

    # ----------------------------------------------------------
    # Call
    # ----------------------------------------------------------

    async def call(
        self, 
        name: str,
        trust_level: Trustlevel,
        **kwargs: Any
    ) -> Any:
        '''Call tool.
        Raise PermissonError if trusl_level insurficient '''

        tool = self._tools.get(name)
        if tool is None:
            raise KeyError(f'Tool {name} not found in registry')
        if trusl_level < tool.min_trust_level:
            log.warning(
                'tool.permission_denied',
                name=name.
                required=tool.min_trust_level,
                got=trusl_level
            )
            raise PermissonError(
                f'Tool {name} requires trust level {tool.min_trust_level}, '
                f'got {trusl_level}'
            )
        log.info('tool.call', name=name, trusl_level=trusl_level)
        result = await tool.handler(**kwargs)
        log.info('tool.success', name=name)
        return result

    def schema(self) -> list[dict[str, Any]]:
        ''' Schema alls tools - for submission in LLM system prompt '''
        return [
            {
                'name': t.name,
                'description': t.decription,
                'min_trust_level': t.min_trust_level,
                'is_reversible': t.is_reversible,
                'tags': t.tags
            }
            for t in self._tools.values()
        ]
    
    # ----------------------------------------------------------
    # Debugging
    # ----------------------------------------------------------

    def __repr__(self) -> str:
        names = list(self._tools.keys())
        return f'MCPRegistry(tools={names})'



    