"""A small, deterministic, germplasm-shaped dataset and queries over it.

The queries cover every construct whose handling by SCARAB matters for
correctness: chains and stars, OPTIONAL (including nested, filtered and
non-well-designed ones), MINUS, UNION, FILTER EXISTS / NOT EXISTS,
subqueries with ORDER BY and LIMIT, aggregates, VALUES and BIND, property
paths, blank nodes, literal constants, variable predicates and repeated
variables. Evaluated over the harvest of a single source, each must give
exactly the answer it gives over the source's complete data.
"""

import random

import pyoxigraph as ox

EX = "http://example.org/"
RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
XSD = "http://www.w3.org/2001/XMLSchema#"
PREFIXES = "PREFIX ex: <http://example.org/>\nPREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"


def _n(local):
    return ox.NamedNode(EX + local)


def generate(seed=7, plants=300, municipalities=120, provinces=8, people=30, banks=4):
    """Generate the dataset as a list of pyoxigraph Quads."""
    rnd = random.Random(seed)
    rdf_type = ox.NamedNode(RDF_TYPE)
    photos_graph = _n("graphs/photos")
    quads = []

    def add(s, p, o, g=None):
        quads.append(ox.Quad(s, p, o, g if g is not None else ox.DefaultGraph()))

    for c in range(2):
        add(_n(f"country{c}"), rdf_type, _n("Country"))
    for v in range(provinces):
        add(_n(f"prov{v}"), rdf_type, _n("Province"))
        add(_n(f"prov{v}"), _n("partOf"), _n(f"country{v % 2}"))
    for m in range(municipalities):
        mun = _n(f"mun{m}")
        add(mun, rdf_type, _n("Municipality"))
        add(mun, _n("name"), ox.Literal(f"Municipality {m}", language="en"))
        if m % 3 == 0:
            add(mun, _n("name"), ox.Literal(f"Municipio {m}", language="es"))
        if rnd.random() < 0.85:
            add(mun, _n("locatedIn"), _n(f"prov{rnd.randrange(provinces)}"))
    for b in range(banks):
        add(_n(f"bank{b}"), rdf_type, _n("Genebank"))
        add(_n(f"bank{b}"), _n("label"), ox.Literal(f"Genebank {b}"))
    for c in range(people):
        person = _n(f"person{c}")
        add(person, rdf_type, _n("Person"))
        add(person, _n("name"), ox.Literal(f"Collector {c}", language="en"))
        for _ in range(rnd.randrange(3)):
            add(person, _n("knows"), _n(f"person{rnd.randrange(people)}"))
        if c % 10 == 0:
            add(person, _n("knows"), person)
    for i in range(plants):
        plant = _n(f"plant{i}")
        add(plant, rdf_type, _n("Plant"))
        add(plant, _n("storedAt"), _n(f"bank{rnd.randrange(banks)}"))
        add(plant, _n("accessionNumber"), ox.Literal(str(1000 + i), datatype=ox.NamedNode(XSD + "integer")))
        add(plant, _n("height"), ox.Literal(f"{rnd.uniform(0.1, 3.0):.2f}", datatype=ox.NamedNode(XSD + "decimal")))
        if rnd.random() < 0.9:
            add(plant, _n("wasCollectedIn"), _n(f"mun{rnd.randrange(municipalities)}"))
        if rnd.random() < 0.4:
            add(plant, _n("hasPhoto"), ox.Literal(f"photo-{i}.jpg"), photos_graph)
        if rnd.random() < 0.6:
            add(plant, _n("collectedBy"), _n(f"person{rnd.randrange(people)}"))
        if rnd.random() < 0.1:
            add(plant, _n("status"), ox.Literal("extinct"))
    return quads


def make_store(quads):
    store = ox.Store()
    store.extend(quads)
    return store


QUERIES = {
    "chain": """SELECT ?p ?m ?prov WHERE {
        ?p ex:wasCollectedIn ?m . ?m ex:locatedIn ?prov . }""",
    "star_constant": """SELECT ?p ?n WHERE {
        ?p ex:storedAt ex:bank1 . ?p ex:accessionNumber ?n . ?p a ex:Plant . }""",
    "optional": """SELECT ?p ?photo WHERE {
        ?p ex:wasCollectedIn ex:mun3 . OPTIONAL { ?p ex:hasPhoto ?photo } }""",
    "optional_large_required": """SELECT ?p ?m ?photo WHERE {
        ?p ex:wasCollectedIn ?m . OPTIONAL { ?p ex:hasPhoto ?photo } }""",
    "optional_filtered": """SELECT ?p ?photo WHERE {
        ?p ex:storedAt ex:bank2 . OPTIONAL { ?p ex:hasPhoto ?photo FILTER(CONTAINS(?photo, "1")) } }""",
    "optional_nested": """SELECT ?p ?c ?cn WHERE {
        ?p ex:storedAt ex:bank0 . OPTIONAL { ?p ex:collectedBy ?c OPTIONAL { ?c ex:name ?cn } } }""",
    "optional_not_well_designed": """SELECT ?p ?m ?prov WHERE {
        ?m ex:locatedIn ?prov . { ?p ex:status "extinct" OPTIONAL { ?p ex:wasCollectedIn ?m } } }""",
    "optional_with_exists": """SELECT ?p ?m WHERE {
        ?p ex:storedAt ex:bank3 .
        OPTIONAL { ?p ex:wasCollectedIn ?m FILTER EXISTS { ?m ex:locatedIn ex:prov1 } } }""",
    "minus": """SELECT ?p WHERE {
        ?p ex:storedAt ex:bank1 . MINUS { ?p ex:status "extinct" } }""",
    "minus_no_shared": """SELECT ?p WHERE {
        ?p ex:storedAt ex:bank1 . MINUS { ?x ex:status "extinct" } }""",
    "not_exists": """SELECT ?m WHERE {
        ?m a ex:Municipality . FILTER NOT EXISTS { ?p ex:wasCollectedIn ?m } }""",
    "exists": """SELECT DISTINCT ?c WHERE {
        ?c a ex:Person . FILTER EXISTS { ?p ex:collectedBy ?c . ?p ex:storedAt ex:bank2 } }""",
    "union": """SELECT ?x ?n WHERE {
        { ?x a ex:Municipality } UNION { ?x a ex:Person } ?x ex:name ?n . }""",
    "union_restricted_by_outer": """SELECT ?p ?y WHERE {
        ?p ex:wasCollectedIn ex:mun5 . { ?p ex:hasPhoto ?y } UNION { ?p ex:collectedBy ?y } }""",
    "subquery_limit": """SELECT ?p ?m WHERE {
        { SELECT ?p WHERE { ?p ex:height ?h } ORDER BY DESC(?h) ?p LIMIT 5 }
        ?p ex:wasCollectedIn ?m . }""",
    "aggregate": """SELECT ?prov (COUNT(?p) AS ?n) WHERE {
        ?p ex:wasCollectedIn ?m . ?m ex:locatedIn ?prov . } GROUP BY ?prov""",
    "values_bind": """SELECT ?p ?g ?s WHERE {
        VALUES ?g { ex:bank1 ex:bank2 } ?p ex:storedAt ?g . ?p ex:status ?st . BIND(STR(?p) AS ?s) }""",
    "path_sequence": """SELECT ?p ?country WHERE {
        ?p ex:collectedBy ex:person4 . ?p ex:wasCollectedIn/ex:locatedIn/ex:partOf ?country . }""",
    "path_plus_constant": """SELECT ?b WHERE { ex:person1 ex:knows+ ?b . }""",
    "path_restricts": """SELECT ?b ?n WHERE { ex:person2 ex:knows+ ?b . ?b ex:name ?n . }""",
    "path_star": """SELECT ?b WHERE { ex:person3 ex:knows* ?b . ?b a ex:Person . }""",
    "path_inverse_alt": """SELECT ?c ?x WHERE {
        ?c a ex:Person . ?c ^ex:collectedBy|ex:knows ?x . ?x ex:storedAt ex:bank0 . }""",
    "blank_node": """SELECT ?p WHERE { ?p ex:wasCollectedIn [ ex:locatedIn ex:prov3 ] . }""",
    "literal_lang": """SELECT ?m ?p WHERE { ?m ex:name "Municipality 7"@en . ?p ex:wasCollectedIn ?m . }""",
    "literal_typed": """SELECT ?p ?m WHERE { ?p ex:accessionNumber 1042 . OPTIONAL { ?p ex:wasCollectedIn ?m } }""",
    "variable_predicate": """SELECT ?pred ?o WHERE { ex:plant7 ?pred ?o . }""",
    "repeated_variable": """SELECT ?x WHERE { ?x ex:knows ?x . }""",
    "empty_pattern": """SELECT ?p ?z WHERE { ?p ex:wasCollectedIn ?m . ?m ex:nonexistent ?z . }""",
    "ask": """ASK { ?p ex:status "extinct" . ?p ex:wasCollectedIn ?m . }""",
}


def oracle(store, query):
    """Evaluate a query over the complete data (all graphs merged)."""
    return normalise(store.query(PREFIXES + query, use_default_graph_as_union=True))


def normalise(result):
    """Turn a pyoxigraph result or SCARAB rows into a comparable value."""
    if isinstance(result, ox.QueryBoolean):
        return bool(result)
    if isinstance(result, list):
        if len(result) == 1 and set(result[0]) == {"boolean"}:
            return bool(result[0]["boolean"])
        return sorted(tuple(sorted((k, str(v)) for k, v in row.items())) for row in result)
    names = [v.value for v in result.variables]
    return sorted(tuple(sorted((n, sol[n].value) for n in names if sol[n] is not None)) for sol in result)
