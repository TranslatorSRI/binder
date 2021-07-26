"""FastAPI router."""
import logging
from typing import List, Union

import aiosqlite
from fastapi import Depends, APIRouter, HTTPException
from reasoner_pydantic import Query, Response

from .engine import KnowledgeProvider

LOGGER = logging.getLogger(__name__)


def get_kp(
    database_file: Union[str, aiosqlite.Connection],
    **kwargs,
):
    """Get KP dependable."""
    async def kp_dependable():
        """Get knowledge provider."""
        async with KnowledgeProvider(database_file, **kwargs) as kp:
            yield kp
    return kp_dependable


def kp_router(
        database_file: Union[str, aiosqlite.Connection],
        **kwargs,
):
    """Add KP to server."""
    router = APIRouter()

    @router.post("/query", response_model=Response)
    async def answer_question(
            query: Query,
            kp: KnowledgeProvider = Depends(get_kp(database_file))
    ) -> Response:
        """Get results for query graph."""
        query = query.dict(exclude_unset=True)
        workflow = query.get("workflow", ["lookup"])
        if len(workflow) > 1:
            raise HTTPException(400, "Binder does not support workflows of length >1")
        operation = workflow[0]
        if operation == "lookup":
            qgraph = query["message"]["query_graph"]

            kgraph, results = await kp.get_results(qgraph)

            response = {
                "message": {
                    "knowledge_graph": kgraph,
                    "results": results,
                    "query_graph": qgraph,
                }
            }
        elif operation == "bind":
            raise NotImplementedError()
        else:
            raise HTTPException(400, f"Unsupported operation {operation}")
        return response

    @router.get("/ops")
    async def get_operations(
            kp: KnowledgeProvider = Depends(get_kp(database_file)),
    ):
        """Get KP operations."""
        return await kp.get_operations()

    @router.get("/metadata")
    async def get_metadata(
            kp: KnowledgeProvider = Depends(get_kp(database_file)),
    ):
        """Get metadata."""
        return {
            "curie_prefixes": await kp.get_curie_prefixes(),
        }

    @router.get("/meta_knowledge_graph")
    async def get_metakg(
            kp: KnowledgeProvider = Depends(get_kp(database_file)),
    ):
        """Get meta knowledge graph."""
        meta_kg = {
            "edges": [
                {
                    "subject": op["subject_category"],
                    "predicate": op["predicate"],
                    "object": op["object_category"],
                }
                for op in await kp.get_operations()
            ],
            "nodes": {
                category: {"id_prefixes": data}
                for category, data in (await kp.get_curie_prefixes()).items()
            },
        }
        return meta_kg

    return router
