"""Make the package importable without installing it, and share fixtures."""

import os
import sys
import tempfile

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for path in (os.path.join(ROOT, "src"), os.path.join(ROOT, "tests")):
    if path not in sys.path:
        sys.path.insert(0, path)

import pyoxigraph as ox  # noqa: E402

from support.data import generate, make_store  # noqa: E402
from support.servers import FragmentsServer, SparqlServer  # noqa: E402


@pytest.fixture(scope="session")
def dataset():
    quads = generate()
    return quads, make_store(quads)


@pytest.fixture(scope="session")
def sources(dataset):
    """One fragments server, one SPARQL endpoint and one dump, same data."""
    quads, store = dataset
    dump = os.path.join(tempfile.mkdtemp(), "data.trig")
    ox.serialize(quads, output=dump, format=ox.RdfFormat.TRIG)
    tpf = FragmentsServer(store, page_size=100).start()
    sparql = SparqlServer(store, union_default_graph=False).start()
    yield {
        "tpf": f"{tpf.base}/fragments",
        "sparql": f"sparql:{sparql.base}/sparql",
        "dump": f"dump:{dump}",
    }
    tpf.stop()
    sparql.stop()


@pytest.fixture
def local_store():
    """Publish into a fresh in-memory store for the duration of a test."""
    import nile.scarab.scarab_harvester as s
    s.configure(store_kind="local", local_store_path="memory", export_dir=None)
    s._LOCAL_STORE = None
    yield s
    s._LOCAL_STORE = None
