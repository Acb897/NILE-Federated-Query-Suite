"""SCARAB: retrieving the data and answering the query.

SCARAB takes a query and a set of repositories, pulls the triples
matching each of the query's patterns out of each repository, loads
everything into one local triplestore, and leaves the original query to
be evaluated there over the combined result.

Retrieval and evaluation are kept apart on purpose. Everything retrieved
is kept, whether or not the repository it came from can answer the rest
of the query, so an answer may be assembled from triples that no single
repository held together. This is the point of the module: engines that
evaluate as they retrieve discard exactly those partial contributions.

Repositories may be fragments servers, SPARQL endpoints or RDF dump
files. All three are asked for one triple pattern at a time and are
interchangeable as far as the harvesting algorithm is concerned -- a
SPARQL endpoint is never handed the query as a whole.

Each run also records where its data came from, as one nanopublication
per repository, stored alongside the harvested triples.

The entry point is `nile.scarab.scarab_harvester.FindBGPPriority`.
"""

