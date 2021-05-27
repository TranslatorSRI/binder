"""Test fancy-shaped query graphs."""
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
async def test_loop(connection: aiosqlite.Connection):
    """Test a simple loop."""
    await add_data(
        connection,
        data="""
            MONDO:0005148(( category biolink:Disease ))
            NCBIGene:123(( category biolink:Gene ))
            CHEBI:6801(( category biolink:ChemicalSubstance ))
            MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
            CHEBI:6801<-- predicate biolink:affected_by --NCBIGene:123
            NCBIGene:123<-- predicate biolink:affected_by --MONDO:0005148
        """,
    )
    kp = KnowledgeProvider(connection)
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "category": "biolink:Disease",
                    "id": "MONDO:0005148",
                },
                "n1": {
                    "category": "biolink:ChemicalSubstance",
                },
                "n2": {
                    "category": "biolink:Gene",
                },
            },
            "edges": {
                "e10": {
                    "subject": "n1",
                    "object": "n0",
                    "predicate": "biolink:treats",
                },
                "e21": {
                    "subject": "n2",
                    "object": "n1",
                    "predicate": "biolink:affected_by",
                },
                "e02": {
                    "subject": "n0",
                    "object": "n2",
                    "predicate": "biolink:affected_by",
                },
            },
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert len(results) == 1


@pytest.mark.asyncio
async def test_branch(connection: aiosqlite.Connection):
    """Test a simple branch."""
    await add_data(
        connection,
        data="""
            MONDO:0005148(( category biolink:Disease ))
            NCBIGene:123(( category biolink:Gene ))
            CHEBI:6801(( category biolink:ChemicalSubstance ))
            CELL:123(( category biolink:Cell ))
            NCBIGene:123<-- predicate biolink:affected_by --MONDO:0005148
            NCBIGene:123<-- predicate biolink:affects --CHEBI:6801
            CELL:123<-- predicate biolink:affected_by --NCBIGene:123
        """,
    )
    kp = KnowledgeProvider(connection)
    message = {
        "query_graph": {
            "nodes": {
                "disease": {
                    "category": "biolink:Disease",
                    "id": "MONDO:0005148",
                },
                "gene": {
                    "category": "biolink:Gene",
                },
                "drug": {
                    "category": "biolink:ChemicalSubstance",
                },
                "cell": {
                    "category": "biolink:Cell",
                },
            },
            "edges": {
                "e10": {
                    "subject": "drug",
                    "object": "gene",
                    "predicate": "biolink:affects",
                },
                "e21": {
                    "subject": "disease",
                    "object": "gene",
                    "predicate": "biolink:affected_by",
                },
                "e02": {
                    "subject": "gene",
                    "object": "cell",
                    "predicate": "biolink:affected_by",
                },
            },
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    assert len(results) == 1
