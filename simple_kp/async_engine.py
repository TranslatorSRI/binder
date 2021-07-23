"""Base Worker class."""
from abc import ABC, abstractmethod
import asyncio
from contextlib import suppress
import copy
import itertools
import json
import logging
from pathlib import Path
from typing import List

import httpx
from reasoner_pydantic import Message

from .cypher import get_degree
from .graph import QGraph

LOGGER = logging.getLogger(__name__)


class Worker(ABC):
    """Asynchronous worker to consume messages from input_queue."""

    def __init__(
            self,
            num_workers: int = 1,
            logger: logging.Logger = LOGGER,
            **kwargs,
    ):
        """Initialize."""
        self.num_workers = num_workers
        self.logger = logger

        self.queue = asyncio.PriorityQueue()
        self.counter = itertools.count()

    @abstractmethod
    async def on_message(self, message):
        """Handle message from results queue."""

    async def _on_message(self, message):
        """Handle message from results queue."""
        try:
            await self.on_message(message)
        except Exception as err:
            self.logger.exception({
                "message": "Aborted processing of queue item due to exception",
                "queue_item": message,
            })

    async def consume(self):
        """Consume messages."""
        while True:
            # wait for an item from the producer
            priority, item = await self.queue.get()

            self.logger.debug(f"Processing item with priority {priority}")

            # process message
            await self._on_message(item)

            # Notify the queue that the item has been processed
            self.queue.task_done()

    async def finish(self, tasks: List[asyncio.Task]):
        """Wait for Strider to finish, then tear everything down."""
        # wait until the consumer has processed all items
        await self.queue.join()
        await self.teardown(tasks)

    async def teardown(self, tasks: List[asyncio.Task]):
        """Tear down consumers after queue is emptied."""
        # the consumers are still waiting for items, cancel them
        for consumer in tasks:
            consumer.cancel()

        for consumer in tasks:
            with suppress(asyncio.CancelledError):
                await consumer

    async def run(self, outdir, *args, wait: bool = False, **kwargs):
        """Run async consumer."""
        # schedule the consumers
        # create max_jobs worker tasks to process the queue concurrently
        self.outdir = Path(outdir)
        self.outcounter = itertools.count()
        tasks = [
            asyncio.create_task(self.consume())
            for _ in range(self.num_workers)
        ]
        finish = asyncio.create_task(self.finish(tasks))
        if wait:
            await finish

    async def put(self, message, priority=0):
        """Put message on queue."""
        await self.queue.put(((priority, next(self.counter)), message))


class AsyncBinder(Worker):
    """Asynchronous binder."""

    def __init__(self, url: str, *args, **kwargs):
        """Initialize."""
        super().__init__(*args, **kwargs)
        self.url = url

    async def lookup_onehop(self, qgraph):
        """Look-up one-hop qgraph using TRAPI service."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.url,
                json={"message": {"query_graph": qgraph}},
                timeout=None,
            )
        response_json = response.json()
        return response_json["message"]["knowledge_graph"], response_json["message"]["results"]

    async def on_message(
            self,
            message: Message,
    ):
        """Process partial result."""
        qgraph = QGraph(message["query_graph"])
        kgraph = message.get("knowledge_graph", {"nodes": dict(), "edges": dict()})
        results = message.get("results", [{"node_bindings": dict(), "edge_bindings": dict()}])

        # if there are no qedges, we're done
        if not qgraph["edges"]:
            return {"nodes": dict(), "edges": dict()}, [{"node_bindings": dict(), "edge_bindings": dict()}]

        self.logger.info(f"[{hash(qgraph)}] Running lookup for qgraph: {qgraph}")

        # find a traversable qedge - one with at least one pinned endpoint
        try:
            qedge_id = next(qgraph.traversable_edges())
        except StopIteration:
            raise RuntimeError("Cannot find a qedge with pinned endpoint in qgraph %s", str(qgraph))
        
        one_hop = qgraph.onehop_from(qedge_id)

        self.logger.debug(f"[{hash(qgraph)}] Outsourcing one-hop: {one_hop}")
        onehop_kgraph, onehop_results = await self.lookup_onehop(one_hop)
        self.logger.debug(f"[{hash(qgraph)}] Got {len(onehop_results)} results.")

        for knode_id, knode in onehop_kgraph["nodes"].items():
            knode["degree"] = await get_degree(knode_id.upper(), self.logger)

        for onehop_result in onehop_results:
            priority = None

            # now solve the smaller question
            qgraph_ = copy.deepcopy(qgraph)
            # remove traversed edge
            qgraph_["edges"].pop(qedge_id)
            # remove orphaned nodes
            qgraph_.remove_orphaned()
            # pin endpoint nodes
            for qnode_id, bindings in onehop_result["node_bindings"].items():
                if qnode_id not in qgraph_["nodes"]:
                    continue
                knode_id = bindings[0]["id"]
                qgraph_["nodes"][qnode_id]["ids"] = [knode_id]
                if priority:
                    raise RuntimeError("Something has gone wrong")
                priority = onehop_kgraph["nodes"][knode_id]["degree"]
            # recursively look-up

            # build updated kgraph
            kgraph_ = copy.deepcopy(kgraph)
            for qnode_id, bindings in onehop_result["node_bindings"].items():
                knode_id = bindings[0]["id"]
                kgraph_["nodes"][knode_id] = onehop_kgraph["nodes"][knode_id]
            for qedge_id, bindings in onehop_result["edge_bindings"].items():
                kedge_id = bindings[0]["id"]
                kgraph_["edges"][kedge_id] = onehop_kgraph["edges"][kedge_id]

            # build updated results
            results_ = copy.deepcopy(results)
            results_ = [
                {
                    "node_bindings": {
                        **result["node_bindings"],
                        **onehop_result["node_bindings"],
                    },
                    "edge_bindings": {
                        **result["edge_bindings"],
                        **onehop_result["edge_bindings"],
                    },
                }
                for result in results_
            ]

            # assemble next message
            message = {
                "query_graph": qgraph_,
                "knowledge_graph": kgraph_,
                "results": results_,
            }
            if not message["query_graph"]["edges"]:
                # finished
                filename = f"result_{next(self.outcounter):04d}.json"
                self.logger.warning(f"[{hash(qgraph)}] Saving result {filename}.")
                with open(self.outdir / filename, "w") as stream:
                    json.dump(message, stream, indent=4)
                continue
            await self.put(message, priority=priority)
