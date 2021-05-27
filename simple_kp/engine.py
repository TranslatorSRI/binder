"""SQL query graph engine."""
from collections import defaultdict
import copy
import itertools
import logging
import os
import re
import sqlite3
from typing import Any, Dict, Tuple, Union

import aiosqlite

from .graph import Graph
from .util import to_list, NoAnswersException

LOGGER = logging.getLogger(__name__)


def normalize_qgraph(qgraph):
    """Normalize query graph."""
    for node in qgraph["nodes"].values():
        node["category"] = node.get("category", "biolink:NamedThing")
    for edge in qgraph["edges"].values():
        edge["predicate"] = to_list(
            edge.get("predicate", "biolink:related_to"))


def custom_row_factory(cursor, row):
    """
    Convert row to dictionary and
    convert some of the fields to lists
    """
    row_output = {}
    for idx, col in enumerate(cursor.description):
        row_output[col[0]] = row[idx]

    return row_output


class KnowledgeProvider():
    """Knowledge provider."""

    def __init__(
            self,
            arg: Union[str, aiosqlite.Connection],
    ):
        """Initialize."""
        if isinstance(arg, str):
            self.database_file = arg
            self.name = os.path.splitext(
                os.path.basename(self.database_file)
            )[0]
            self.db = None
        elif isinstance(arg, aiosqlite.Connection):
            self.database_file = None
            self.name = None
            self.db = arg
            self.db.row_factory = custom_row_factory
        else:
            raise ValueError(
                "arg should be of type str or aiosqlite.Connection"
            )

    async def __aenter__(self):
        """Enter context."""
        if self.db is not None:
            return self
        self.db = await aiosqlite.connect(self.database_file)
        self.db.row_factory = sqlite3.Row
        return self

    async def __aexit__(self, *args):
        """Exit context."""
        if not self.database_file:
            return
        tmp_db = self.db
        self.db = None
        await tmp_db.close()

    async def get_operations(self):
        """Get operations."""
        async with self.db.execute(
                "SELECT * FROM edges",
        ) as cursor:
            edges = await cursor.fetchall()

        async with self.db.execute(
                "SELECT * FROM nodes",
        ) as cursor:
            nodes = await cursor.fetchall()
        nodes = {
            node["id"]: node
            for node in nodes
        }

        ops = set()
        for edge in edges:
            subject_node = nodes[edge["subject"]]
            object_node = nodes[edge["object"]]

            operation_iterator = itertools.product(
                [subject_node["category"]],
                [edge["predicate"]],
                [object_node["category"]],
            )

            ops.update(operation_iterator)

        return [
            {
                "subject": op[0],
                "predicate": op[1],
                "object": op[2],
            }
            for op in ops
        ]

    async def get_curie_prefixes(self):
        """Get CURIE prefixes."""
        async with self.db.execute(
                "SELECT * FROM nodes",
        ) as cursor:
            nodes = await cursor.fetchall()

        prefixes = defaultdict(set)
        for node in nodes:
            prefixes[node["category"]].add(node["id"].split(":")[0])
        return {
            category: list(prefix_set)
            for category, prefix_set in prefixes.items()
        }

    async def get_kedges(self, **kwargs):
        """Get kedges."""
        assert kwargs
        conditions = []
        for key, value in kwargs.items():
            if isinstance(value, list):
                placeholders = ", ".join("?" for _ in value)
                conditions.append(f"{key} in ({placeholders})")
            else:
                conditions.append(f"{key} = ?")
        conditions = " AND ".join(conditions)
        async with self.db.execute(
                (
                    "SELECT edge.id AS id, subject.id AS subject, edge.predicate as predicate, object.id as object "
                    "FROM edges AS edge "
                    "JOIN nodes AS subject ON edge.subject = subject.id "
                    "JOIN nodes AS object ON edge.object = object.id "
                 ) + "WHERE " + conditions,
                list(
                    x for value in kwargs.values()
                    for x in to_list(value)
                ),
        ) as cursor:
            rows = await cursor.fetchall()

        return {
            row["id"]: {key: value for key, value in row.items() if key != "id"}
            for row in rows
        }

    async def lookup(
            self,
            qgraph: Graph,
    ):
        """Expand from query graph node."""
        LOGGER.debug(f"Lookup for qgraph: {qgraph}")
        # if this is a leaf node, we're done
        if not qgraph["edges"]:
            return {"nodes": dict(), "edges": dict()}, [{"node_bindings": dict(), "edge_bindings": dict()}]
        kgraph = {"nodes": dict(), "edges": dict()}
        results = []
        source_qnode_id, source_qnode = next(
            (key, qnode)
            for key, qnode in qgraph["nodes"].items()
            if qnode.get("id", None) is not None
        )

        # look up associated knode(s)
        curies = to_list(source_qnode["id"])
        for curie in curies:
            source_knode_id, source_knode = await self.get_knode(curie)
            LOGGER.debug(
                "Expanding from node %s/%s...",
                source_qnode_id,
                source_knode_id,
            )

            qgraph_ = copy.deepcopy(qgraph)
            qedge_id, qedge = next(
                (qedge_id, qedge)
                for qedge_id, qedge in list(qgraph["edges"].items())
                if source_qnode_id in (qedge["subject"], qedge["object"])
                and qgraph_["edges"].pop(qedge_id) is not None
            )

            # get kedges for qedge
            kwargs = dict()
            for key, value in qedge.items():
                if key in ("subject", "object"):
                    continue
                kwargs[f"edge.{key}"] = value
            for role in ("subject", "object"):
                for key, value in qgraph_["nodes"][qedge[role]].items():
                    if key in ():
                        continue
                    kwargs[f"{role}.{key}"] = value

            kedges = await self.get_kedges(**kwargs)

            for kedge_id, kedge in kedges.items():
                LOGGER.debug(
                    "Expanding along edge %s/%s...",
                    qedge_id,
                    kedge_id,
                )
                # now solve the smaller question

                qgraph__ = copy.deepcopy(qgraph_)
                # pin node
                qgraph__["nodes"][qedge["subject"]]["id"] = [kedge["subject"]]
                qgraph__["nodes"][qedge["object"]]["id"] = [kedge["object"]]
                # remove orphaned nodes
                qgraph__.remove_orphaned()
                
                kgraph_, results_ = await self.lookup(qgraph__)

                # add edge to results and kgraph
                kgraph["edges"][kedge_id] = kedge
                subject_knode_id, subject_knode = await self.get_knode(kedge["subject"])
                object_knode_id, object_knode = await self.get_knode(kedge["object"])
                kgraph["nodes"][subject_knode_id] = subject_knode
                kgraph["nodes"][object_knode_id] = object_knode
                results_ = [
                    {
                        "node_bindings": {
                            **result["node_bindings"],
                            qedge["subject"]: [{
                                "id": kedge["subject"],
                            }],
                            qedge["object"]: [{
                                "id": kedge["object"],
                            }],
                        },
                        "edge_bindings": {
                            **result["edge_bindings"],
                            qedge_id: [{
                                "id": kedge_id,
                            }],
                        },
                    }
                    for result in results_
                ]
                kgraph["nodes"].update(kgraph_["nodes"])
                kgraph["edges"].update(kgraph_["edges"])
                results.extend(results_)

        return kgraph, results

    async def get_knode(self, knode_id: str) -> Tuple[str, Dict]:
        """Get knode by id."""
        async with self.db.execute(
                "SELECT * FROM nodes WHERE id = ?",
                [knode_id],
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            raise NoAnswersException()
        return row["id"], {k: v for k, v in dict(row).items() if k != "id"}

    async def get_results(self, qgraph: Dict[str, Any]):
        """Get results and kgraph."""
        qgraph = Graph(qgraph)
        normalize_qgraph(qgraph)
        kgraph, results = await self.lookup(qgraph)
        return kgraph, results
