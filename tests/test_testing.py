"""Test testing."""
from binder.engine import KnowledgeProvider
import tempfile

import aiosqlite
import httpx
import pytest

from binder.build_db import add_data_from_string
from binder.testing import kp_overlay

from tests.logging_setup import setup_logger

setup_logger()


@kp_overlay(
    "kp",
    data="""
    MONDO:0005148(( category biolink:Disease ))
    MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
    CHEBI:6801(( category biolink:ChemicalSubstance ))
    """,
)
@pytest.mark.asyncio
async def test_overlay():
    """Test KP overlay."""
    request = {
        "message": {
            "query_graph": {
                "nodes": {
                    "n0": {
                        "categories": ["biolink:ChemicalSubstance"],
                    },
                    "n1": {
                        "categories": ["biolink:Disease"],
                        "ids": ["MONDO:0005148"],
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
    }
    async with httpx.AsyncClient() as client:
        response = await client.post("http://kp/query", json=request)
    response.raise_for_status()
    kedges = response.json()["message"]["knowledge_graph"]["edges"]
    for kedge in kedges.values():
        assert any(
            attribute["attribute_type_id"] == "biolink:knowledge_source"
            and attribute["value"] == "infores:kp"
            for attribute in kedge["attributes"]
        )


@pytest.mark.asyncio
async def test_database_file():
    """Test database file."""
    f = tempfile.NamedTemporaryFile()
    filename = f.name
    async with aiosqlite.connect(filename) as connection:
        # add data to sqlite
        await add_data_from_string(
            connection,
            data="""
                MONDO:0005148(( category biolink:Disease ))
                MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
                CHEBI:6801(( category biolink:ChemicalSubstance ))
            """,
        )
    async with KnowledgeProvider(filename) as kp:
        qgraph = {
            "nodes": {
                "n0": {
                    "categories": ["biolink:ChemicalSubstance"],
                },
                "n1": {
                    "categories": ["biolink:Disease"],
                    "ids": ["MONDO:0005148"],
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
        kgraph, results = await kp.get_results(qgraph)
        assert results
