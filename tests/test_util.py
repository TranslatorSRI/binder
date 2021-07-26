"""Test utilities."""
from binder.util import get_subcategories, get_subpredicates
from binder.engine import normalize_qgraph


def test_expand_category():
    """Test get_subcategories()."""
    categories = get_subcategories("biolink:DiseaseOrPhenotypicFeature")
    assert isinstance(categories, list)
    assert len(categories) >= 3
    categories = get_subcategories("biolink:Disease")
    assert isinstance(categories, list)
    assert len(categories) == 1


def test_expand_predicate():
    """Test get_subpredicates()."""
    predicates = get_subpredicates("biolink:affects_expression_of")
    assert isinstance(predicates, list)
    assert len(predicates) == 3


def test_normalize_qgraph():
    """Test normalize_qgraph()."""
    qgraph = {
        "nodes": {
            "something": {},
        },
        "edges": {},
    }
    normalize_qgraph(qgraph)
    assert qgraph["nodes"]["something"]["categories"]
