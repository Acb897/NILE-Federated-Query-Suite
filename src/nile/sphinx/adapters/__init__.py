"""Source adapters: one way in for each kind of repository.

An adapter hides how a repository is reached, so that the indexing
algorithm in `nile.sphinx.sphinx.Engine` does not have to care. Three
are provided -- `SPARQLAdapter`, `RDFDumpAdapter` and `TPFAdapter` --
and `AdapterFactory` chooses between them.

Every adapter exposes the same three methods:

    exploratory_types()
        List the class IRIs the repository holds, as plain strings.

    outgoing_patterns(type_)
        List the predicates that instances of `type_` point out along,
        together with the class of whatever they point at.

    incoming_patterns(type_)
        List the predicates that point at instances of `type_`,
        together with the class of whatever does the pointing.

The last two return a list of solution dicts in SPARQL-JSON form, so
that results from a dump file or a fragments server are
indistinguishable from results from a real endpoint::

    {
        "predicate":    {"type": "uri", "value": <predicate IRI>},
        "object_type":  {"type": "uri", "value": <class IRI>},
        "subject_type": {"type": "uri", "value": <class IRI>},
        "g":            {"type": "uri", "value": <graph IRI>},
    }

`outgoing_patterns` fills in "object_type" and `incoming_patterns` fills
in "subject_type". Either may be an empty dict, which the Engine reads
as "the class is unknown" rather than "there is no relationship", so the
predicate is still recorded.

The "type" key is what lets the Engine tell an IRI apart from a blank
node or a literal, and a class is only accepted when it is "uri".
Adapters that do not speak SPARQL-JSON natively therefore have to set
the key themselves rather than leaving callers to guess from the string.

Adding a fourth adapter means implementing those three methods and
registering it with `AdapterFactory`; nothing in the Engine needs to
change.
"""

