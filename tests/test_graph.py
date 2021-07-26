from binder.graph import Graph


def test_graph():
    graph = Graph(
        nodes={"n0": {}, "n1": {}},
        edges={"e01": {
            "subject": "n0",
            "object": "n1",
        }},
    )
    assert graph.connected_edges("n0") == (["e01"], [])
