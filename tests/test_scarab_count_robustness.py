"""Behaviour against a fragments server whose counts are placeholders."""

import threading

import nile.scarab.scarab_harvester as s
import support.servers as servers
from support.data import QUERIES, PREFIXES, oracle, normalise


def test_identical_concurrent_requests_are_coalesced(dataset, local_store):
    _, store = dataset
    with servers.FragmentsServer(store, latency=0.05) as tpf:
        source = s.TPFDataSource(f"{tpf.base}/f")
        pat = {"subject": "?a", "predicate": "http://example.org/knows", "object": "?b", "graph": None}
        s.page_cache.clear()
        before = tpf.requests
        threads = [threading.Thread(target=source.count, args=(dict(pat),)) for _ in range(8)]
        [t.start() for t in threads]
        [t.join() for t in threads]
        assert tpf.requests - before == 1


def test_placeholder_counts_do_not_make_a_source_unreachable(dataset, local_store):
    """Every count is the LDF server's 1e9 placeholder: the source is still harvested."""
    _, store = dataset
    with servers.FragmentsServer(store, reported_total=1_000_000_000) as tpf:
        query = QUERIES["chain"]
        got = normalise(s.answer_query(PREFIXES + query, [f"{tpf.base}/f"]))
    assert got == oracle(store, query)
    assert s.last_run_stats()[0]["triples_stored"] > 0
