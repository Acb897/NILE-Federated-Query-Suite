"""SPHINX's dump adapter reads every graph of a file, whatever the format."""

import os
import tempfile

import pyoxigraph as ox
import pytest

from nile.sphinx.adapters.dump_adapter import RDFDumpAdapter
from nile.sphinx.adapters.tpf_adapter import TPFAdapter

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
FORMATS = {
    "trig": ox.RdfFormat.TRIG,
    "nq": ox.RdfFormat.N_QUADS,
    "ttl": ox.RdfFormat.TURTLE,
    "nt": ox.RdfFormat.N_TRIPLES,
}


def _write(quads, extension):
    path = os.path.join(tempfile.mkdtemp(), f"data.{extension}")
    if FORMATS[extension] in (ox.RdfFormat.TURTLE, ox.RdfFormat.N_TRIPLES):
        quads = [ox.Triple(q.subject, q.predicate, q.object) for q in quads]
    ox.serialize(quads, output=path, format=FORMATS[extension])
    return path


@pytest.mark.parametrize("extension", sorted(FORMATS))
def test_all_triples_are_indexed(dataset, extension):
    quads, _ = dataset
    adapter = RDFDumpAdapter(_write(quads, extension))
    distinct = {(q.subject, q.predicate, q.object) for q in quads}
    indexed = sum(len(pairs) for pairs in adapter.spo_index.values())
    assert indexed == len(distinct)


def test_named_graph_predicates_are_found():
    quads = [
        ox.Quad(ox.NamedNode("http://e/a"), ox.NamedNode(RDF_TYPE), ox.NamedNode("http://e/Plant"),
                ox.NamedNode("http://e/record1")),
        ox.Quad(ox.NamedNode("http://e/a"), ox.NamedNode("http://e/hasPhoto"), ox.Literal("p.jpg"),
                ox.NamedNode("http://e/record1")),
    ]
    adapter = RDFDumpAdapter(_write(quads, "nq"))
    assert adapter.exploratory_types() == ["http://e/Plant"]
    predicates = {r["predicate"]["value"] for r in adapter.outgoing_patterns("http://e/Plant")}
    assert "http://e/hasPhoto" in predicates


def test_dump_and_tpf_adapters_agree(dataset, sources):
    quads, _ = dataset
    dump = RDFDumpAdapter(_write(quads, "trig"))
    tpf = TPFAdapter(sources["tpf"])

    def outgoing(adapter, cls):
        # rdf:type is dropped by the Engine whichever adapter reports it.
        return {(r["predicate"]["value"], (r.get("object_type") or {}).get("value"))
                for r in adapter.outgoing_patterns(cls) if r["predicate"]["value"] != RDF_TYPE}

    def incoming(adapter, cls):
        return {(r["predicate"]["value"], r["subject_type"]["value"])
                for r in adapter.incoming_patterns(cls)}

    classes = sorted(dump.exploratory_types())
    assert classes == sorted(tpf.exploratory_types())
    for cls in classes:
        assert outgoing(dump, cls) == outgoing(tpf, cls), cls
        assert incoming(dump, cls) == incoming(tpf, cls), cls
