"""SPHINX: offline structural indexing of RDF repositories.

SPHINX visits a repository and records how its data is shaped rather
than what it contains: which classes are present, and which predicates
link them to one another. The result is written as one SHACL file per
repository, which RIDDLE later matches incoming queries against.

Because only the shape is recorded, the index stays small however large
the repository is, and two repositories following the same data model
produce near-identical indexes even when they share no data at all.

Indexing is meant to be run once per repository and repeated only when
its contents change, so that query time involves no contact with the
repositories being chosen between.

The entry point is `nile.sphinx.sphinx.Engine`. Repositories are reached
through the adapters in `nile.sphinx.adapters`, which cover SPARQL
endpoints, RDF dump files and Triple Pattern Fragments servers.

One caveat worth being aware of before publishing an index: it does not
disclose individual records, but it does list the repository's full
class and predicate vocabulary. In a sensitive domain, the presence of a
rare or highly specific class can itself be revealing.
"""

