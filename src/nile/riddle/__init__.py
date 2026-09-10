"""RIDDLE: deciding which repositories are worth asking.

RIDDLE takes a SPARQL query and the SHACL indexes SPHINX produced, and
returns the repositories whose data is shaped in a way that could
contribute to answering it.

It contacts no repository to do this. Both the query and each index are
reduced to the same thing -- a set of (subject class, predicate, object
class) triples -- and the two sets are compared. One compatible triple
is enough for a repository to be kept.

The comparison is deliberately generous. Missing type information on
either side is read as "unknown", not as "does not occur", so a
repository is kept whenever it might contribute rather than only when it
could answer the query single-handedly. A false positive costs one
wasted request during harvesting; a false negative would lose data
without anyone noticing.

The entry point is `nile.riddle.riddle.shacl_validator`.
"""

