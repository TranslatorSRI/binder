"""Planning."""
from functools import cache

from .graph import Graph


@cache
def get_plan(qgraph: Graph):
    """Get a traversal plan.

    The plan will be in the form of a map from qnode ids to orderd lists of qedge ids to traverse.
    """
    plan = []
    if not qgraph["edges"]:
        return plan

    # find pinned node to start from
    initial_qnode_id = next(
        qnode_id
        for qnode_id, qnode in qgraph["nodes"].items()
        if qnode.get("id")
    )

    # find qedges attached to initial qnode
    outgoing, incoming = qgraph.connected_edges(initial_qnode_id)

    # traverse them in order
    qedge_ids = outgoing + incoming

    for qedge_id in qedge_ids:
        qedge = qgraph["edges"].pop(qedge_id, None)
        if qedge is None:
            continue
        next_qnode_id = (
            qedge["subject"]
            if qedge["object"] == initial_qnode_id else
            qedge["object"]
        )
        qgraph["nodes"][next_qnode_id]["id"] = True
        qgraph.remove_orphaned()
        plan += [(initial_qnode_id, qedge_id)] + get_plan(qgraph)

    return plan
