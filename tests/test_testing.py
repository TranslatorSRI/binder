"""Test testing."""
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
