"""Publication, export, statistics, SPHINX's strict mode and isolated harvesting."""

import glob
import os
import tempfile

import pyoxigraph as ox

import nile.scarab.scarab_harvester as s
from support.data import QUERIES, PREFIXES, oracle, normalise, make_store
from support.servers import FragmentsServer, SparqlServer


def test_remote_publication_and_export(dataset, sources):
    _, store = dataset
    published = ox.Store()
    export_dir = tempfile.mkdtemp()
    with SparqlServer(published) as remote:
        s.configure(store_kind="remote", store_query_url=remote.query_url,
                    store_statements_url=remote.statements_url, export_dir=export_dir)
        try:
            query = PREFIXES + QUERIES["optional_nested"]
            got = normalise(s.answer_query(query, [sources["tpf"]]))
        finally:
            s.configure(export_dir=None, store_kind="local")
    assert got == oracle(store, QUERIES["optional_nested"])
    suffixes = {g.value.rsplit("#", 1)[-1] for g in published.named_graphs()}
    assert suffixes == {"Head", "assertion", "provenance", "pubinfo"}      # no "paths" graph
    files = glob.glob(os.path.join(export_dir, "*.trig"))
    assert len(files) == 1
    assert normalise(s.evaluate_harvest_files(query, files)) == got


def test_statistics_are_recorded(sources, local_store):
    s.answer_query(PREFIXES + QUERIES["chain"], [sources["tpf"]])
    stats = s.last_run_stats()[0]
    assert stats["requests"] > 0 and stats["triples_stored"] > 0
    assert {p["decision"] for p in stats["patterns"]} <= {"full", "full+filter", "bind", "empty"}


def test_run_query_strict_returns_triples_without_publishing(sources, local_store):
    before = len(s.local_store())
    triples = s.run_query_strict("SELECT * WHERE { ?s a <http://example.org/Province> . }",
                                 [sources["tpf"]])
    assert len(triples) == 8 and all(t[0]["type"] == "uri" for t in triples)
    assert len(s.local_store()) == before


def test_isolated_sources_with_disjoint_vocabularies_are_complete(dataset, local_store):
    quads, store = dataset
    first = ("wasCollectedIn", "storedAt", "hasPhoto", "accessionNumber", "height",
             "status", "collectedBy", "type")
    a = make_store([q for q in quads if q.predicate.value.endswith(first)])
    b = make_store([q for q in quads if not q.predicate.value.endswith(first)])
    with FragmentsServer(a) as fa, FragmentsServer(b) as fb:
        for name, query in QUERIES.items():
            got = normalise(s.answer_query(PREFIXES + query, [f"{fa.base}/f", f"{fb.base}/f"]))
            assert got == oracle(store, query), name
