from simple_kp.graph import Graph
from simple_kp.planning import get_plan


def test_loop():
    qgraph = {
        "nodes": {
            "n0": {
                "category": "biolink:Disease",
                "id": "MONDO:0005148",
            },
            "n1": {
                "category": "biolink:ChemicalSubstance",
            },
            "n2": {
                "category": "biolink:Gene",
            },
        },
        "edges": {
            "e10": {
                "subject": "n1",
                "object": "n0",
                "predicate": "biolink:treats",
            },
            "e21": {
                "subject": "n2",
                "object": "n1",
                "predicate": "biolink:affected_by",
            },
            "e02": {
                "subject": "n0",
                "object": "n2",
                "predicate": "biolink:affected_by",
            },
        },
    }
    plan = get_plan(Graph(qgraph))
    assert len(plan) == 3


def test_branch():
    qgraph = {
        "nodes": {
            "disease": {
                "category": "biolink:Disease",
                "id": "MONDO:0005148",
            },
            "gene": {
                "category": "biolink:Gene",
            },
            "drug": {
                "category": "biolink:ChemicalSubstance",
            },
            "cell": {
                "category": "biolink:Cell",
            },
        },
        "edges": {
            "drug_gene": {
                "subject": "drug",
                "object": "gene",
                "predicate": "biolink:affects",
            },
            "disease_gene": {
                "subject": "disease",
                "object": "gene",
                "predicate": "biolink:affected_by",
            },
            "gene_cell": {
                "subject": "gene",
                "object": "cell",
                "predicate": "biolink:affected_by",
            },
        },
    }
    plan = get_plan(Graph(qgraph))
    assert len(plan) == 3
    assert plan[0] == ("disease", "disease_gene")
