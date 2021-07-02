"""Query graph utilities."""
import re

from bmt import Toolkit

BMT = Toolkit(
    schema="https://raw.githubusercontent.com/biolink/biolink-model/1.8.2/biolink-model.yaml",
)


def get_subcategories(category):
    """Get sub-categories, according to the Biolink model."""
    categories = BMT.get_descendants(category, formatted=True, reflexive=True) or [category]
    return [
        category.replace("_", "")
        for category in categories
    ]


def camelcase_to_snakecase(string):
    """Convert CamelCase to snake_case."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", string).lower()


def get_subpredicates(predicate):
    """Get sub-predicates, according to the Biolink model."""
    curies = BMT.get_descendants(predicate, formatted=True, reflexive=True) or [predicate]
    return [
        "biolink:" + camelcase_to_snakecase(curie[8:])
        for curie in curies
    ]


def to_list(scalar_or_list):
    """Enclose in list if necessary."""
    if not isinstance(scalar_or_list, list):
        return [scalar_or_list]
    return scalar_or_list


class NoAnswersException(Exception):
    """No answers to question."""
