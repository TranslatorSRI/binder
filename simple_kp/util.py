"""Query graph utilities."""

def to_list(scalar_or_list):
    """Enclose in list if necessary."""
    if not isinstance(scalar_or_list, list):
        return [scalar_or_list]
    return scalar_or_list


class NoAnswersException(Exception):
    """No answers to question."""
