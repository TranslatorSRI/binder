"""Test fancy-shaped query graphs."""
from simple_kp.async_engine import AsyncBinder
import aiosqlite
import pytest

from simple_kp.build_db import add_data
from simple_kp.engine import DisTRAPI, KnowledgeProvider

from .logging_setup import setup_logger


setup_logger()


@pytest.mark.asyncio
async def test_simple():
    """Test a simple one-hop."""
    kp = DisTRAPI("https://automat.renci.org/robokopkg/1.1/query")
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "categories": [
                        "biolink:Gene"
                    ]
                },
                "n1": {
                    "ids": [
                        "MONDO:0005737"
                    ],
                    "categories": [
                        "biolink:Disease"
                    ]
                },
                "n2": {
                    "categories": ["biolink:ChemicalSubstance"]
                }
            },
            "edges": {
                "e0": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": [
                        "biolink:related_to"
                    ]
                },
                "e1": {
                    "subject": "n0",
                    "object": "n2",
                }
            }
        }
    }
    kgraph, results = await kp.get_results(message["query_graph"])
    print(len(results))
    # assert len(results) == 1



@pytest.mark.asyncio
async def test_async():
    """Test a simple one-hop."""
    kp = AsyncBinder(
        "https://automat.renci.org/robokopkg/1.1/query",
        num_workers=20,
    )
    message = {
        "query_graph": {
            "nodes": {
                "n0": {
                    "categories": [
                        "biolink:Gene"
                    ]
                },
                "n1": {
                    "ids": [
                        "MONDO:0005737"
                    ],
                    "categories": [
                        "biolink:Disease"
                    ]
                },
                "n2": {
                    "categories": ["biolink:ChemicalSubstance"]
                }
            },
            "edges": {
                "e0": {
                    "subject": "n0",
                    "object": "n1",
                    "predicates": [
                        "biolink:related_to"
                    ]
                },
                "e1": {
                    "subject": "n0",
                    "object": "n2",
                }
            }
        }
    }

    await kp.put(message)
    await kp.run(
        "20210709_results",
        wait=True,
    )
    # kgraph, results = await kp.get_results(message["query_graph"])
    # print(len(results))
    # # assert len(results) == 1
