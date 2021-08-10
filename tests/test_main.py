"""Test /query endpoint."""
import aiosqlite
import pytest

from reasoner_pydantic import KnowledgeGraph, Result

from binder.build_db import add_data_from_string
from binder.engine import KnowledgeProvider

from .logging_setup import setup_logger


setup_logger()


@pytest.fixture
async def connection():
    """Return FastAPI app fixture."""
    async with aiosqlite.connect(":memory:") as connection:
        yield connection


@pytest.mark.asyncio
async def test_subcategory(connection: aiosqlite.Connection):
    """Test subcategory with chemical substance."""
    await add_data_from_string(
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
                },
                "n1": {
                    "categories": ["biolink:NamedThing"],
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
    assert results


@pytest.mark.asyncio
async def test_reverse(connection: aiosqlite.Connection):
    """Test simple KP."""
    await add_data_from_string(
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
    assert results


@pytest.mark.asyncio
async def test_list_properties(connection: aiosqlite.Connection):
    """Test that we correctly handle query graph where categories, ids, and predicates are lists."""
    await add_data_from_string(
        connection,
        data="""
            CHEBI:136043(( category biolink:ChemicalSubstance ))
            CHEBI:136043-- predicate biolink:treats -->MONDO:0005148
            MONDO:0005148(( category biolink:Disease ))
        """,
    )
    kp = KnowledgeProvider(connection)
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "categories": ["biolink:ChemicalSubstance"],
                    "ids": ["CHEBI:136043"],
                },
                "n1": {
                    "categories": ["biolink:Disease"],
                },
            },
            "edges": {
                "e01": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:treats"],
                },
            },
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results


@pytest.mark.asyncio
async def test_isittrue(connection: aiosqlite.Connection):
    """Test is-it-true-that query."""
    await add_data_from_string(
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
                },
                "n1": {
                    "categories": ["biolink:ChemicalSubstance"],
                    "ids": ["CHEBI:6801"],
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
    assert results


@pytest.mark.asyncio
async def test_fail(connection: aiosqlite.Connection):
    """Test simple KP."""
    await add_data_from_string(
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
                },
                "n1": {
                    "categories": ["biolink:ChemicalSubstance"],
                },
            },
            "edges": {
                "e01": {
                    "subject": "n1",
                    "object": "n0",
                    "predicates": ["biolink:causes"],
                },
            },
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results == []


@pytest.mark.asyncio
async def test_symmetric(connection: aiosqlite.Connection):
    """Test symmetric predicate."""
    await add_data_from_string(
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
                },
                "n1": {
                    "categories": ["biolink:ChemicalSubstance"],
                },
            },
            "edges": {
                "e01": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:related_to"],
                },
            },
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results
