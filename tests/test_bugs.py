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
async def test_extra_ids(connection: aiosqlite.Connection):
    """Test extra qnode ids."""
    await add_data(
        connection,
        data="""
            CHEBI:6801(( category biolink:ChemicalSubstance ))
            MONDO:0005148(( category biolink:Disease ))
            CHEBI:6801-- predicate biolink:treats -->MONDO:0005148
        """,
    )
    kp = KnowledgeProvider(connection)
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "ids": ["CHEBI:6801", "CHEBI:6802", "CHEBI:6803"],
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
    assert len(results) == 1


@pytest.mark.asyncio
async def test_unknown_predicate(connection: aiosqlite.Connection):
    """Test unknown predicate."""
    await add_data(
        connection,
        data="""
            CHEBI:6801(( category biolink:ChemicalSubstance ))
            MONDO:0005148(( category biolink:Disease ))
            HP:0004324(( category biolink:PhenotypicFeature ))
            CHEBI:6801-- predicate biolink:unknown -->MONDO:0005148
            MONDO:0005148-- predicate biolink:has_phenotype -->HP:0004324
        """,
    )
    kp = KnowledgeProvider(connection)
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "categories": ["biolink:NamedThing"],
                    "ids": ["CHEBI:6801"],
                },
                "n1": {
                    "categories": ["biolink:Disease"],
                },
            },
            "edges": {
                "e01": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": ["biolink:unknown"],
                },
            },
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert results


@pytest.mark.asyncio
async def test_subclass(connection: aiosqlite.Connection):
    """Test unrecognized key."""
    await add_data(
        connection,
        data="""
            CHEBI:6801(( category biolink:ChemicalSubstance ))
            MONDO:0005148(( category biolink:Disease ))
            HP:0004324(( category biolink:PhenotypicFeature ))
            CHEBI:6801-- predicate biolink:treats -->MONDO:0005148
            MONDO:0005148-- predicate biolink:has_phenotype -->HP:0004324
        """,
    )
    kp = KnowledgeProvider(connection)
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "categories": ["biolink:NamedThing"],
                    "ids": ["CHEBI:6801"],
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
async def test_unrecognized_key(connection: aiosqlite.Connection):
    """Test unrecognized key."""
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


@pytest.mark.asyncio
async def test_ignored_key(connection: aiosqlite.Connection):
    """Test ignored key."""
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
                    "is_set": False,
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
