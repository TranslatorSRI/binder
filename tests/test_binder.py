import pytest
from simple_kp.engine import KnowledgeProvider


@pytest.mark.asyncio
async def test_adding_data():
    async with KnowledgeProvider() as kp:
        await kp.add_data({
            "nodes": {
                "MONDO:0005737": {"category": "biolink:Disease", "name": "Ebola"},
                "MONDO:0005148": {"category": "biolink:Disease", "name": "diabetes"},
            },
            "edges": {
                "foo": {"subject": "MONDO:0005737", "object": "MONDO:0005148", "predicate": "biolink:related_to"},
            },
        })

        kgraph, results = await kp.get_results({
            "nodes": {
                "ebola": {"ids": ["MONDO:0005737"], "categories": ["biolink:Disease"]},
                "disease": {"categories": ["biolink:Disease"]},
            },
            "edges": {
                "related_to": {"subject": "ebola", "object": "disease", "predicates": ["biolink:related_to"]},
            },
        })

        assert results
