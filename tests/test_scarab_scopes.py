"""Which harvested patterns may restrict which (no network involved)."""

import nile.scarab.scarab_harvester as s

P = "PREFIX ex: <http://example.org/>\n"


def idx(bgp, local_name):
    return next(i for i, p in enumerate(bgp) if p["predicate"].endswith("/" + local_name))


def plan_vars(bgp, t, harvested):
    plan = s.restriction_plan(t, bgp, harvested)
    return None if plan is None else plan.variables


def test_optional_restricted_by_required_but_not_reverse():
    bgp = s.transform(P + "SELECT * WHERE { ?p ex:coll ?m . OPTIONAL { ?p ex:photo ?ph } }")
    coll, photo = idx(bgp, "coll"), idx(bgp, "photo")
    assert plan_vars(bgp, photo, [coll]) == ["p"]
    assert plan_vars(bgp, coll, [photo]) is None


def test_exists_and_not_exists_patterns_are_extracted_and_one_directional():
    bgp = s.transform(P + "SELECT * WHERE { ?a ex:p ?b . FILTER EXISTS { ?b ex:q ?c } "
                          "FILTER NOT EXISTS { ?b ex:r ?d } }")
    assert {p["predicate"].rsplit("/", 1)[-1] for p in bgp} == {"p", "q", "r"}
    outer, inner = idx(bgp, "p"), idx(bgp, "r")
    assert plan_vars(bgp, inner, [outer]) == ["b"]
    assert plan_vars(bgp, outer, [inner]) is None


def test_minus_right_restricted_by_left_only():
    bgp = s.transform(P + "SELECT * WHERE { ?p ex:a ?x . MINUS { ?p ex:b ?y } }")
    left, right = idx(bgp, "a"), idx(bgp, "b")
    assert plan_vars(bgp, right, [left]) == ["p"]
    assert plan_vars(bgp, left, [right]) is None


def test_union_branches_do_not_restrict_each_other():
    bgp = s.transform(P + "SELECT * WHERE { ?x ex:p ?y . { ?y ex:a ?z } UNION { ?y ex:b ?z } }")
    outer, a, b = idx(bgp, "p"), idx(bgp, "a"), idx(bgp, "b")
    assert plan_vars(bgp, b, [a]) is None
    assert plan_vars(bgp, a, [outer]) == ["y"]


def test_subquery_is_a_barrier_both_ways():
    bgp = s.transform(P + "SELECT * WHERE { ?x ex:p ?y . { SELECT ?y WHERE { ?y ex:s ?z } LIMIT 5 } }")
    outer, inner = idx(bgp, "p"), idx(bgp, "s")
    assert plan_vars(bgp, inner, [outer]) is None
    assert plan_vars(bgp, outer, [inner]) is None


def test_outer_pattern_restricts_optional_only_through_required_variables():
    # ?y is bound outside and inside the OPTIONAL, but not by its required side:
    # restricting the optional side on ?y could create spurious solutions.
    bgp = s.transform(P + "SELECT * WHERE { ?x ex:r ?y . { ?x ex:a ?z OPTIONAL { ?x ex:b ?y } } }")
    assert plan_vars(bgp, idx(bgp, "b"), [idx(bgp, "r"), idx(bgp, "a")]) == ["x"]


def test_binding_query_has_no_cartesian_product():
    bgp = s.transform(P + "SELECT * WHERE { ?p ex:coll ?m . ?m ex:loc ?v . ?b ex:name ?n . ?b ex:holds ?a }")
    loc = idx(bgp, "loc")
    plan = s.restriction_plan(loc, bgp, [i for i in range(len(bgp)) if i != loc])
    query = s.build_binding_query(plan, bgp, "urn:g#assertion", "urn:g#paths")
    assert "coll" in query and "name" not in query and "holds" not in query


def test_query_blank_nodes_become_variables():
    bgp = s.transform(P + "SELECT * WHERE { ?a ex:p [ ex:q ?c ] }")
    assert all(not p[f].startswith("_:") for p in bgp for f in ("subject", "object"))


def test_service_patterns_are_not_harvested():
    bgp = s.transform(P + "SELECT * WHERE { ?a ex:p ?b SERVICE <http://x/sparql> { ?b ex:q ?c } }")
    assert [p["predicate"].rsplit("/", 1)[-1] for p in bgp] == ["p"]


def test_repeated_pattern_in_union_is_requested_once_and_never_restricted():
    bgp = s.transform(P + "SELECT * WHERE { ?x ex:p ?y . { ?y ex:t ?z } UNION { ?y ex:t ?z . ?z ex:u ?w } }")
    t = idx(bgp, "t")
    assert len(bgp[t]["scopes"]) == 2
    assert plan_vars(bgp, t, [idx(bgp, "p")]) is None
