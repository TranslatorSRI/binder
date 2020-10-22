"""Simple API server."""
import glob
import os
from typing import Union

import aiosqlite
from fastapi import Depends, FastAPI, APIRouter
from reasoner_pydantic import Message

from .engine import KnowledgeProvider
from .util import NoAnswersException


def get_kp(database_file: Union[str, aiosqlite.Connection]):
    """Get KP dependable."""
    async def kp_dependable():
        """Get knowledge provider."""
        async with KnowledgeProvider(database_file) as kp:
            yield kp
    return kp_dependable


app = FastAPI(
    title="Test KP",
    description="Simple dummy KP for testing",
    version="0.1.0",
)


def kp_router(
        database_file: Union[str, aiosqlite.Connection],
        name: str = None,
        curie_prefix: str = None,
):
    """Add KP to server."""
    router = APIRouter()

    @router.post("/query")
    async def answer_question(
            message: Message,
            kp: KnowledgeProvider = Depends(get_kp(database_file))
    ):
        """Get results for query graph."""
        message = message.dict()
        qgraph = message["query_graph"]

        kgraph, results = await kp.get_results(qgraph)

        message = {
            "knowledge_graph": kgraph,
            "results": results,
            "query_graph": qgraph,
        }
        return message

    @router.get("/ops")
    async def get_operations(
            kp: KnowledgeProvider = Depends(get_kp(database_file)),
    ):
        """Get KP operations."""
        return await kp.get_operations()

    @router.get("/metadata")
    async def get_metadata():
        """Get metadata."""
        return {
            "curie_prefix": curie_prefix,
        }

    return router


database_files = glob.glob("data/*.db")
for database_file in database_files:
    name = os.path.splitext(os.path.basename(database_file))[0]
    app.include_router(kp_router(database_file), prefix="/" + name)
