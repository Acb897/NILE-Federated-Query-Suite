"""Harvesting one source and evaluating locally gives the complete answer."""

import pytest

import nile.scarab.scarab_harvester as s
from support.data import QUERIES, PREFIXES, oracle, normalise


@pytest.mark.parametrize("kind", ["tpf", "sparql", "dump"])
@pytest.mark.parametrize("name", sorted(QUERIES))
def test_single_source_answer_is_complete(dataset, sources, local_store, kind, name):
    _, store = dataset
    got = normalise(s.answer_query(PREFIXES + QUERIES[name], [sources[kind]]))
    assert got == oracle(store, QUERIES[name])
