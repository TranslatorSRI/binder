import json


class Graph(dict):
    """Graph."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        super().__init__(*args, **kwargs)
    
    def __hash__(self):
        """Compute hash."""
        return hash(json.dumps(self, sort_keys=True))

    def connected_edges(self, node_id):
        """Find edges connected to node."""
        outgoing = []
        incoming = []
        for edge_id, edge in self["edges"].items():
            if node_id == edge["subject"]:
                outgoing.append(edge_id)
            if node_id == edge["object"]:
                incoming.append(edge_id)
        return outgoing, incoming

    def remove_orphaned(self):
        """Remove nodes with degree 0."""
        self["nodes"] = {
            node_id: node
            for node_id, node in self["nodes"].items()
            if any(self.connected_edges(node_id))
        }


class QGraph(Graph):
    def traversable_edges(self):
        return (
            edge_id
            for edge_id, edge in self["edges"].items()
            if (
                self["nodes"][edge["subject"]].get("ids", None)
                or self["nodes"][edge["object"]].get("ids", None)
            )
        )
    
    def onehop_from(self, qedge_id):
        qedge = self["edges"][qedge_id]
        return {
            "nodes": {
                key: value
                for key, value in self["nodes"].items()
                if key in (qedge["subject"], qedge["object"])
            }, 
            "edges": {
                qedge_id: qedge
            }
        }
