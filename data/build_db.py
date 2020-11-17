#!/usr/bin/env python
"""Data I/O."""
import argparse
import asyncio
import csv

import aiosqlite

from small_kg import nodes_file, edges_file, synonyms_file
from simple_kp.types import CURIEMap


async def add_data(
        connection: aiosqlite.Connection,
        origin: str = '',
        curie_prefixes: CURIEMap = None,
):
    """Add data to SQLite database."""
    with open(nodes_file, newline='') as csvfile:
        reader = csv.DictReader(
            csvfile,
            delimiter=',',
        )
        nodes = list(reader)
    with open(edges_file, newline='') as csvfile:
        reader = csv.DictReader(
            csvfile,
            delimiter=',',
        )
        edges = list(reader)
    edges = [
        {
            **edge,
            "id": idx
        }
        for idx, edge in enumerate(edges)
        if edge["origin"].startswith(origin)
    ]

    if curie_prefixes is not None:
        # map to synonyms with prefix
        with open(synonyms_file, newline="") as csvfile:
            reader = csv.reader(
                csvfile,
                delimiter=',',
            )
            synsets = list(reader)
        synset_map = {
            term: synset
            for synset in synsets
            for term in synset
        }
        node_map = dict()
        for node in nodes:
            # get preferred CURIE
            for curie_prefix in curie_prefixes[node["category"]]:
                # get CURIE with prefix, if one exists
                try:
                    node_map[node["id"]] = next(
                        curie
                        for curie in synset_map[node["id"]]
                        if curie.startswith(curie_prefix + ":")
                    )
                    break
                except StopIteration:
                    continue
        for node in nodes:
            node["id"] = node_map.get(node["id"], node["id"])
        for edge in edges:
            edge["subject"] = node_map.get(edge["subject"], edge["subject"])
            edge["object"] = node_map.get(edge["object"], edge["object"])

    await connection.execute('CREATE TABLE nodes ({0})'.format(
        ', '.join([f'{val} text' for val in nodes[0]])
    ))
    await connection.executemany('INSERT INTO nodes VALUES ({0})'.format(
        ', '.join(['?' for _ in nodes[0]])
    ), [list(node.values()) for node in nodes])
    await connection.execute('CREATE TABLE edges ({0})'.format(
        ', '.join([f'{val} text' for val in edges[0]])
    ))
    await connection.executemany('INSERT INTO edges VALUES ({0})'.format(
        ', '.join(['?' for _ in edges[0]])
    ), [list(edge.values()) for edge in edges])
    await connection.commit()


async def main(filename, origin=''):
    """Load data from CSV files."""
    connection = await aiosqlite.connect(filename)
    await add_data(connection, origin=origin)
    await connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Build SQLite database from CSV files.',
    )

    parser.add_argument('filename', type=str, help='database file')
    parser.add_argument('--origin', type=str, default='', help='origin prefix')

    args = parser.parse_args()
    asyncio.run(main(args.filename, origin=args.origin))
