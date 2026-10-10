"""A fragment retrieved in full is not retrieved again for another pattern.

SP2Bench q6 and q7 repeat `?doc rdf:type ?class` in their OPTIONALs. Where
`?class rdfs:subClassOf foaf:Document` matches nothing, each copy used to be
retrieved in full, and the page cache cannot help once a fragment has more
pages than it holds.
"""

import pyoxigraph as ox
import pytest

import nile.scarab.scarab_harvester as s
from support.data import normalise
from support.servers import FragmentsServer

N = ox.NamedNode
TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
SUB = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
NAME = "http://ex/name"


def typed_store(n=1000):
    st = ox.Store()
    for i in range(n):
        st.add(ox.Quad(N(f"http://ex/thing{i}"), N(TYPE), N(f"http://ex/Class{i % 7}")))
        st.add(ox.Quad(N(f"http://ex/thing{i}"), N(NAME), ox.Literal(f"thing {i}")))
    return st


@pytest.fixture
def small_cache(local_store):
    s.configure(page_cache_size=2)        # far fewer pages than the fragment has
    yield s
    s.configure(page_cache_size=2048)


def decisions():
    return [p["decision"] for p in s.last_run_stats()[0]["patterns"]]


Q7_LIKE = f"""SELECT ?doc WHERE {{
  ?class <{SUB}> <http://ex/Document> . ?doc <{TYPE}> ?class .
  OPTIONAL {{ ?class3 <{SUB}> <http://ex/Document> . ?doc3 <{TYPE}> ?class3 .
    OPTIONAL {{ ?class4 <{SUB}> <http://ex/Document> . ?doc4 <{TYPE}> ?class4 }} }} }}"""


def test_repeated_unrestricted_fragment_is_retrieved_once(small_cache):
    st = typed_store()
    with FragmentsServer(st) as tpf:
        got = normalise(s.answer_query(Q7_LIKE, [f"{tpf.base}/f"]))
        requests = tpf.requests
    assert got == normalise(st.query(Q7_LIKE))
    assert decisions().count("full") == 1 and decisions().count("reused") == 2
    pages = -(-1000 // 100)
    assert requests <= pages + 6          # one walk, plus one count request per pattern


def test_reuse_gives_the_same_answers_as_retrieval(small_cache):
    query = f"""SELECT * WHERE {{ {{ ?a <{TYPE}> ?b }} UNION
        {{ ?x <{TYPE}> <http://ex/Class3> . ?x <{NAME}> ?n }} UNION {{ ?y <{TYPE}> ?y }} }}"""
    st = typed_store()
    with FragmentsServer(st) as tpf:
        got = normalise(s.answer_query(query, [f"{tpf.base}/f"]))
    assert got == normalise(st.query(query))


def test_coverage_rules():
    def pat(s_, p, o, g=None):
        return {"subject": s_, "predicate": p, "object": o, "graph": g}
    full = pat("?a", TYPE, "?b")
    assert s.fragment_covers(full, pat("?x", TYPE, "?y"))                 # renamed
    assert s.fragment_covers(full, pat("?x", TYPE, "http://ex/Class3"))   # more bound
    assert s.fragment_covers(full, pat("?x", TYPE, "?x"))                 # repeated variable
    assert not s.fragment_covers(full, pat("?x", NAME, "?y"))             # other predicate
    assert not s.fragment_covers(pat("?x", TYPE, "?x"), full)             # a filtered fragment covers nothing
    assert not s.fragment_covers(pat("?a", TYPE, "http://ex/Class3"), full)
    assert not s.fragment_covers(full, pat("?x", TYPE, "?y", "http://ex/g"))   # another graph


def test_incomplete_fragment_is_not_reused(local_store):
    st = typed_store(300)

    def garbage_on_page_2(page, content_type, body):
        return b"not RDF {{{" if page == 2 else body

    query = f"SELECT * WHERE {{ {{ ?a <{TYPE}> ?b }} UNION {{ ?x <{TYPE}> ?y }} }}"
    with FragmentsServer(st, body_filter=garbage_on_page_2, serve_nquads=False) as tpf:
        s.answer_query(query, [f"{tpf.base}/f"])
    assert "reused" not in decisions()
