import pytest
from core.mcp_registry import MCPRegistry, ToolDefinition

async def dummy_handler(**kwargs):
    return 'ok'

@pytest.fixture
def registry():
    return MCPRegistry()

@pytest.fixture
def read_tool():
    return ToolDefinition(
        name='read_file',
        description='Read a file',
        min_trust_level=1,
        handler=dummy_handler,
        is_reversible=True,
        tags=['fs', 'readonly'],
    )

@pytest.fixture
def write_tool():
    return ToolDefinition(
        name='write_file',
        description='write a file',
        min_trust_level=3,
        handler=dummy_handler,
        is_reversible=False,
        tags=['fs', 'write'],
    )

class TestRegistration:
    def test_register_tool(self, registry, read_tool):
        registry.register(read_tool)
        assert registry.get('read_file') is not None

    def test_duplicate_raises(self, registry, read_tool):
        registry.register(read_tool)
        with pytest.raises(ValueError, match='already register'):
            registry.register(read_tool)

    def test_unregister(self, registry, read_tool):
        registry.register(read_tool)
        registry.unregister('read_file')
        assert registry.get('read_file') is None

    
class TestAvailabity:
    def test_available_for_level(self, registry, read_tool, write_tool):
        registry.register(read_tool)
        registry.register(write_tool)

        available_l1 = registry.available_for(1)
        available_l3 = registry.available_for(3)

        assert len(available_l1) == 1
        assert available_l1[0].name == 'read_file'
        assert len(available_l3) == 2

class TestCall:
    @pytest.mark.asyncio
    async def test_call_success(self, registry, read_tool):
        registry.register(read_tool)
        result = await registry.call('read_file', trus_level=1)
        assert result == 'ok'

    @pytest.mark.asyncio
    async def test_call_permission_denied(self, registry, write_tool):
        registry.register(write_tool)
        with pytest.raises(PermissionError):
            await registry.call('write_file', trust_level=1)

    @pytest.mark.asyncio
    async def test_call_unknown_tool(self, registry):
        with pytest.raises(KeyError):
            await registry.call('nonexistent', trust_level=5)

    def test_schema(self, registry, read_tool, write_tool):
        registry.register(read_tool)
        registry.register(write_tool)
        schema = registry.schema()
        assert len(schema) == 2
        names = {s['name'] for s in schema}
        assert names == {'read_file', 'write_file'}


