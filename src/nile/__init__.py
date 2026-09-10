"""NILE: a federated query suite for distributed RDF repositories.

NILE answers SPARQL queries that no single repository can answer on its
own. It works in two phases, which may be run by different people at
different times.

Offline, `nile.sphinx` visits each repository and writes down a compact
description of how its data is shaped -- which classes it holds, and
which predicates link them -- as a SHACL file. No instance data is
copied.

Online, `nile.riddle` compares an incoming query against those
descriptions to decide which repositories are worth asking, and
`nile.scarab` retrieves the matching triples from the ones that are,
loads them into a local triplestore, and runs the original query over
the combined result.

A repository may publish its data as a SPARQL endpoint, as a Triple or
Quad Pattern Fragments server, or as a downloadable RDF file. All three
are supported throughout and may be mixed freely within a single query.

A whole workflow, end to end::

    from nile.sphinx.sphinx import Engine
    from nile.riddle.riddle import shacl_validator
    from nile.scarab.scarab_harvester import FindBGPPriority

    # Offline: index each repository once, and again when it changes.
    engine = Engine()
    index = engine.extract_patterns(["http://example.org/sparql"])
    engine.shacl_generator(index, "./shacl_output")

    # Online: once per query.
    responsive = shacl_validator(query, "./shacl_output",
                                 identify_by="source")
    FindBGPPriority(query, [f"sparql:{url}" for url in responsive])

The "sparql:" prefix in the last line is worth noting. SCARAB reads a
bare http(s) location as a fragments server, so a SPARQL endpoint has to
say so explicitly. See `nile.scarab.scarab_harvester.make_datasource`.
"""

