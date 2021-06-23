"""Test bugs."""
import aiosqlite
import pytest

from simple_kp.build_db import add_data
from simple_kp.engine import KnowledgeProvider

from .logging_setup import setup_logger


setup_logger()


@pytest.fixture
async def connection():
    """Return FastAPI app fixture."""
    async with aiosqlite.connect(":memory:") as connection:
        yield connection


@pytest.mark.asyncio
async def test_unrecognized_key(connection: aiosqlite.Connection):
    """Test simple KP."""
    await add_data(
        connection,
        data="""
            MONDO:0005148(( category biolink:Disease ))
            MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
            CHEBI:6801(( category biolink:ChemicalSubstance ))
        """,
    )
    kp = KnowledgeProvider(connection)
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "categories": ["biolink:Disease"],
                    "ids": ["MONDO:0005148"],
                    "foo": "bar",
                },
                "n1": {
                    "categories": ["biolink:ChemicalSubstance"],
                },
            },
            "edges": {
                "e01": {
                    "subject": "n1",
                    "object": "n0",
                    "predicates": ["biolink:treats"],
                },
            },
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert not results