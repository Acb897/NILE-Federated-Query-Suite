"""The forced-dump guard: what it skips, what it costs, and how it is declared."""

import pyoxigraph as ox
import pytest

import nile.scarab.scarab_harvester as s
from support.data import normalise
from support.servers import FragmentsServer

SAME_AS = "http://www.w3.org/2002/07/owl#sameAs"
N = ox.NamedNode


def store(triples):
    st = ox.Store()
    st.extend(ox.Quad(N(a), N(b), c if isinstance(c, ox.Literal) else N(c)) for a, b, c in triples)
    return st


NYT = store([
    ("http://nyt/obama", SAME_AS, "http://dbp/Obama"),
    ("http://nyt/obama", "http://nyt/topicPage", ox.Literal("obama.html")),
])
DBPEDIA = store([("http://dbp/Obama", "http://dbp/party", "http://dbp/Democrats"),
                 ("http://dbp/Caffeine", "http://dbp/formula", ox.Literal("C8H10N4O2"))]
                + [(f"http://dbp/thing{i}", "http://dbp/label", ox.Literal(f"thing {i}")) for i in range(1500)])
DRUGBANK = store([
    ("http://db/DB00201", SAME_AS, "http://dbp/Caffeine"),
    ("http://db/DB00201", "http://db/name", ox.Literal("Caffeine")),
])
UNION = store([(q.subject.value, q.predicate.value, q.object if isinstance(q.object, ox.Literal) else q.object.value)
               for st in (NYT, DBPEDIA, DRUGBANK) for q in st])

S1 = f"""SELECT ?predicate ?object WHERE {{
  {{ <http://dbp/Obama> ?predicate ?object }} UNION
  {{ ?subject <{SAME_AS}> <http://dbp/Obama> . ?subject ?predicate ?object }} }}"""
S9 = f"""SELECT ?predicate ?object WHERE {{
  {{ <http://db/DB00201> ?predicate ?object }} UNION
  {{ <http://db/DB00201> <{SAME_AS}> ?caff . ?caff ?predicate ?object }} }}"""


@pytest.fixture(scope="module")
def servers():
    with FragmentsServer(NYT) as a, FragmentsServer(DBPEDIA) as b, FragmentsServer(DRUGBANK) as c:
        yield {"nyt": f"{a.base}/f", "dbpedia": f"{b.base}/f", "drugbank": f"{c.base}/f"}


@pytest.fixture
def guard(local_store):
    yield s
    s.configure(skip_forced_dumps=True)


def expected(query):
    return normalise(UNION.query(query))


def declared_skips():
    rows = s.local_store().query(f"""SELECT ?pattern ?source WHERE {{ GRAPH ?g {{
        ?pattern <{s.SCARAB_VOCAB.notRetrievedFrom}> ?source }} }}""")
    return {sol["source"].value for sol in rows}


def test_s1_skipping_the_dumps_loses_nothing(servers, guard):
    sources = [servers["nyt"], servers["dbpedia"], servers["drugbank"]]
    got = normalise(s.answer_query(S1, sources))
    assert got == expected(S1)
    stats = {st["source"].split(": ", 1)[1]: st for st in s.last_run_stats()}
    dbpedia = stats[servers["dbpedia"]]
    assert dbpedia["declared_incomplete"] and dbpedia["requests"] < 20       # no 1,500-triple dump
    assert "skipped-dump" in {p["decision"] for p in dbpedia["patterns"]}
    assert not stats[servers["nyt"]]["declared_incomplete"]                  # restricted, not skipped
    assert declared_skips() == {servers["dbpedia"], servers["drugbank"]}


def test_s9_loses_cross_repository_answers_and_declares_it(servers, guard):
    sources = [servers["drugbank"], servers["dbpedia"]]
    got = normalise(s.answer_query(S9, sources))
    want = expected(S9)
    missing = set(want) - set(got)
    assert missing == {(("object", "C8H10N4O2"), ("predicate", "http://dbp/formula"))}
    assert set(got) <= set(want)
    assert any(st["declared_incomplete"] for st in s.last_run_stats())

    s.configure(skip_forced_dumps=False)                                    # the old behaviour
    assert normalise(s.answer_query(S9, sources)) == want


def test_unrestricted_all_variable_pattern_is_still_retrieved(servers, guard):
    query = "SELECT * WHERE { ?s ?p ?o }"
    assert normalise(s.answer_query(query, [servers["nyt"]])) == normalise(NYT.query(query))
    assert not s.last_run_stats()[0]["declared_incomplete"]


def test_is_all_variable():
    assert s.is_all_variable({"subject": "?a", "predicate": "?b", "object": "?a"})
    assert not s.is_all_variable({"subject": "?a", "predicate": "http://p", "object": "?b"})
