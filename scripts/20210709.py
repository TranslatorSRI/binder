"""Experiment.

MATCH (disease:disease)
CALL{
    WITH disease
    MATCH (drug:chemical_substance)-[:treats]->(disease)
    RETURN drug.id as drug_id, disease.id as disease_id
    LIMIT 1
}
RETURN drug_id, disease_id
LIMIT 10

CALL{
    MATCH (drug:chemical_substance)
    RETURN drug LIMIT 1000
}
WITH collect(drug) AS drugs
CALL{
    MATCH (disease:disease)
    RETURN disease LIMIT 1000
}
WITH drugs, collect(disease) AS diseases
WITH [i IN range(0, 999) | [drugs[i], diseases[i]]] AS pairs
UNWIND pairs as pair
WITH pair[0] as drug, pair[1] as disease
WHERE NOT (drug)-[:treats]->(disease)
RETURN drug.id, disease.id
LIMIT 100
"""
import asyncio
import json
import logging
import os
from pathlib import Path

from simple_kp.async_engine import AsyncBinder

FILE_DIR = Path(__file__).parent


class ColoredFormatter(logging.Formatter):
    """Colored formatter."""

    prefix = "[%(asctime)s: %(levelname)s/%(name)s]:"
    default = f"{prefix} %(message)s"
    error_fmt = f"\x1b[31m{prefix}\x1b[0m %(message)s"
    warning_fmt = f"\x1b[33m{prefix}\x1b[0m %(message)s"
    info_fmt = f"\x1b[32m{prefix}\x1b[0m %(message)s"
    debug_fmt = f"\x1b[34m{prefix}\x1b[0m %(message)s"

    def __init__(self, fmt=default):
        """Initialize."""
        logging.Formatter.__init__(self, fmt)

    def format(self, record):
        """Format record."""
        format_orig = self._style._fmt
        if record.levelno == logging.DEBUG:
            self._style._fmt = ColoredFormatter.debug_fmt
        elif record.levelno == logging.INFO:
            self._style._fmt = ColoredFormatter.info_fmt
        elif record.levelno == logging.WARNING:
            self._style._fmt = ColoredFormatter.warning_fmt
        elif record.levelno == logging.ERROR:
            self._style._fmt = ColoredFormatter.error_fmt
        result = logging.Formatter.format(self, record)
        self._style._fmt = format_orig
        return result


async def run_query(qgraph, outdir: Path, logger=None):
    """Run query."""

    kp = AsyncBinder(
        "https://automat.renci.org/robokopkg/1.1/query",
        num_workers=1,
        logger=logger,
    )
    message = {
        "query_graph": qgraph,
    }

    await kp.put(message)
    await kp.run(
        outdir,
        wait=True,
    )


async def main():
    """Do the thing."""
    # with open(FILE_DIR / "pairs.json", "r") as stream:
    with open(FILE_DIR / "no_treats_pairs.json", "r") as stream:
        pairs = json.load(stream)

    for idx, (drug, disease) in enumerate(pairs):
        # idx += 59
        print(idx)
        outdir = FILE_DIR / f"{idx:05d}"
        outdir.mkdir(exist_ok=True)

        logdir = outdir / "log.txt"

        # clear log
        logdir.unlink(missing_ok=True)

        # create logger
        logger = logging.getLogger(f"{__name__}_{idx:05d}")
        # sh = logging.StreamHandler()
        # sh.setFormatter(ColoredFormatter())
        # logger.addHandler(sh)
        fh = logging.FileHandler(logdir)
        fh.setFormatter(ColoredFormatter())
        logger.addHandler(fh)
        logger.setLevel(logging.DEBUG)

        qgraph = {
            "nodes": {
                "drug": {
                    "ids": [drug],
                    "categories": ["biolink:ChemicalSubstance"],
                },
                "stop1": {},
                # "stop2": {},
                "disease": {
                    "ids": [disease],
                    "categories": ["biolink:Disease"],
                },
            },
            "edges": {
                "leg1": {
                    "subject": "drug",
                    "object": "stop1",
                },
                "leg2": {
                    "subject": "stop1",
                    "object": "disease",
                },
                # "leg2": {
                #     "subject": "stop1",
                #     "object": "stop2",
                # },
                # "leg3": {
                #     "subject": "stop2",
                #     "object": "disease",
                # },
            }
        }
        await run_query(qgraph, outdir, logger=logger)


if __name__ == "__main__":
    asyncio.run(main())
