"""Test server."""
import httpx
import pytest

from simple_kp.testing import kp_overlay

from tests.logging_setup import setup_logger

setup_logger()


@kp_overlay("kp", data="""
    MONDO:0005148(( category biolink:Disease ))
    MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
    CHEBI:6801(( category biolink:ChemicalSubstance ))
    """
)
@pytest.mark.asyncio
async def test_unsupported_operation():
    """Test unsupported operation."""
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
        },
        "workflow": [
            "restate",
        ],
    }
    async with httpx.AsyncClient() as client:
        response = await client.post("http://kp/query", json=request)
    assert response.status_code == 400


@kp_overlay("kp", data="""
    MONDO:0005148(( category biolink:Disease ))
    MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
    CHEBI:6801(( category biolink:ChemicalSubstance ))
    """
)
@pytest.mark.asyncio
async def test_lookup():
    """Test lookup."""
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
        },
        "workflow": [
            "lookup",
        ],
    }
    async with httpx.AsyncClient() as client:
        response = await client.post("http://kp/query", json=request)
    assert response.status_code == 200
    assert response.json()["message"]["results"]


@kp_overlay("kp", data="")
@pytest.mark.asyncio
async def test_bind():
    """Test bind."""
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
            },
            "knowledge_graph": {
                "nodes": {
                    "MONDO:0005148": {
                        "categories": ["biolink:Disease"],
                    },
                    "CHEBI:6801": {
                        "categories": ["biolink:ChemicalSubstance"],
                    },
                },
                "edges": {
                    "foo": {
                        "subject": "CHEBI:6801",
                        "predicate": "biolink:treats",
                        "object": "MONDO:0005148",
                    },
                },
            },
        },
        "workflow": [
            "bind",
        ],
    }
    async with httpx.AsyncClient() as client:
        response = await client.post("http://kp/query", json=request)
    assert response.status_code == 200
    assert response.json()["message"]["results"]


@kp_overlay("kp", data="""
    MONDO:0005148(( category biolink:Disease ))
    MONDO:0005148<-- predicate biolink:treats --CHEBI:6801
    CHEBI:6801(( category biolink:ChemicalSubstance ))
    """
)
@pytest.mark.asyncio
async def test_metakg():
    """Test /metakg."""
    async with httpx.AsyncClient() as client:
        response = await client.get("http://kp/meta_knowledge_graph")
    assert response.status_code == 200
