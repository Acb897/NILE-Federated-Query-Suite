"""Fragments containing invalid IRIs, and pages that cannot be read at all.

LinkedMDB, as served by the Linked Data Fragments server, contains IRIs with
code points RFC 3987 forbids (controls, spaces). A strict parser rejects the
whole page holding one, which used to end the walk of the fragment silently.
"""

import pyoxigraph as ox
import pytest

import nile.scarab.scarab_harvester as s
from support.data import normalise
from support.servers import FragmentsServer

N = ox.NamedNode
SAME_AS = "http://www.w3.org/2002/07/owl#sameAs"
GENRE = "http://lmdb/genre"
MARK = "_BAD_"                 # stands for the invalid code point in the stored data


def lmdb_store(n=250, bad_every=40):
    """Films linked to external IRIs; every `bad_every`-th link is malformed."""
    st = ox.Store()
    for i in range(n):
        target = f"http://freebase/{MARK}{i}" if i % bad_every == 0 else f"http://dbp/film{i}"
        st.add(ox.Quad(N(f"http://lmdb/film{i}"), N(SAME_AS), N(target)))
        st.add(ox.Quad(N(target), N(GENRE), ox.Literal(f"genre {i % 5}")))
    return st


def break_iris(char):
    def body_filter(page, content_type, body):
        return body.replace(MARK.encode(), char.encode())
    return body_filter


def unbreak(char):
    return lambda value: value.replace(char, MARK)


def assertion_flags():
    rows = s.local_store().query(f"""SELECT ?p ?o WHERE {{ GRAPH ?g {{ ?x ?p ?o
        FILTER(?p IN (<{s.SCARAB_VOCAB.declaredIncomplete}>, <{s.SCARAB_VOCAB.rewrittenIRIs}>,
                      <{s.SCARAB_VOCAB.incompletelyRetrievedFrom}>)) }} }}""")
    return {sol["p"].value.rsplit("#", 1)[1]: sol["o"].value for sol in rows}


QUERY = f"SELECT ?film ?x WHERE {{ ?film <{SAME_AS}> ?x }}"
JOIN = f"SELECT ?film ?genre WHERE {{ ?film <{SAME_AS}> ?x . ?x <{GENRE}> ?genre }}"


@pytest.mark.parametrize("char", ["\x00", " ", "|"])
def test_invalid_iris_are_rewritten_not_lost(local_store, char):
    st = lmdb_store()
    with FragmentsServer(st, body_filter=break_iris(char), request_filter=unbreak(char)) as tpf:
        got = normalise(s.answer_query(QUERY, [f"{tpf.base}/f"]))
    stats = s.last_run_stats()[0]
    assert len(got) == 250                                  # every page was read
    assert stats["errors"] == 0 and not stats["declared_incomplete"]
    assert stats["iris_rewritten"] > 0
    encoded = "".join(f"%{b:02X}" for b in char.encode())
    assert (("film", "http://lmdb/film0"), ("x", f"http://freebase/{encoded}0")) in got
    assert int(assertion_flags()["rewrittenIRIs"]) == stats["iris_rewritten"]


def test_rewritten_iris_are_sent_back_as_the_source_spells_them(local_store):
    """A bind join on a rewritten IRI asks the source for the original IRI."""
    st = lmdb_store()
    want = normalise(st.query(JOIN))
    with FragmentsServer(st, body_filter=break_iris(" "), request_filter=unbreak(" ")) as tpf:
        got = normalise(s.answer_query(JOIN, [f"{tpf.base}/f"]))
    assert got == want                                      # ?film and ?genre carry no broken IRI
    decisions = {p["pattern"].split()[1]: p["decision"] for p in s.last_run_stats()[0]["patterns"]}
    assert decisions[GENRE] in ("bind", "full", "full+filter")


def test_unreadable_statement_costs_only_itself_and_is_declared(local_store):
    """A backslash defeats even lenient parsing; the N-Quads retry drops one line."""
    st = lmdb_store()
    with FragmentsServer(st, body_filter=break_iris("\\"), request_filter=unbreak("\\")) as tpf:
        got = normalise(s.answer_query(QUERY, [f"{tpf.base}/f"]))
    stats = s.last_run_stats()[0]
    bad = len(range(0, 250, 40))
    assert len(got) == 250 - bad
    assert stats["triples_dropped"] == bad and stats["declared_incomplete"]
    flags = assertion_flags()
    assert flags["declaredIncomplete"] == "true" and "incompletelyRetrievedFrom" in flags


def test_page_that_cannot_be_read_is_declared_not_hidden(local_store):
    st = lmdb_store()

    def garbage_on_page_2(page, content_type, body):
        return b"this is not RDF {{{" if page == 2 else body

    with FragmentsServer(st, body_filter=garbage_on_page_2) as tpf:
        got = normalise(s.answer_query(QUERY, [f"{tpf.base}/f"]))
    stats = s.last_run_stats()[0]
    assert len(got) == 100                                   # the walk stops at page 2 ...
    assert stats["declared_incomplete"]                      # ... and says so
    pattern = stats["patterns"][0]
    assert "incomplete" in pattern
    assert assertion_flags()["declaredIncomplete"] == "true"


def test_valid_pages_are_untouched(local_store):
    st = lmdb_store()
    with FragmentsServer(st) as tpf:
        got = normalise(s.answer_query(QUERY, [f"{tpf.base}/f"]))
    stats = s.last_run_stats()[0]
    assert got == normalise(st.query(QUERY))
    assert stats["iris_rewritten"] == 0 and not stats["declared_incomplete"]
    assert "declaredIncomplete" not in assertion_flags()


def test_failed_page_without_a_line_based_fallback_is_an_error_and_declared(local_store):
    st = lmdb_store()

    def garbage_on_page_2(page, content_type, body):
        return b"this is not RDF {{{" if page == 2 else body

    with FragmentsServer(st, body_filter=garbage_on_page_2, serve_nquads=False) as tpf:
        got = normalise(s.answer_query(QUERY, [f"{tpf.base}/f"]))
    stats = s.last_run_stats()[0]
    assert len(got) == 100
    assert stats["errors"] >= 1 and stats["declared_incomplete"]
    assert "request(s) failed" in stats["patterns"][0]["incomplete"]
