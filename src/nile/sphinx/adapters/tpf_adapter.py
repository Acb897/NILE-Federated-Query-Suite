"""Indexing a repository through its fragments interface.

A Triple Pattern Fragments server answers requests for one triple
pattern at a time and does nothing else, so the exploration queries the
other adapters issue have to be taken apart, requested pattern by
pattern, and reassembled locally. That work is done by SCARAB's
harvester, which this module drives through `run_query_strict` -- one
piece of retrieval machinery serves both modules.

Fragments responses carry control metadata describing the fragment
itself alongside the data. Anything in the Hydra, VoID or SPARQL service
description vocabularies is therefore filtered out, so that a server's
own bookkeeping does not end up in the index as if it were repository
content.
"""

# B.2.7: single shared back-end. This previously imported from a copy of
# the harvester that lived alongside the indexer and had diverged from the
# SCARAB copy (it carried execute_sparql_query(include_types=...) which
# _is_uri_term below depends on, but lacked the property-path support).
# Both modules now import the same file; the indexer-side copy is deleted.
from nile.scarab.scarab_harvester import run_query_strict

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

# Metadata predicates that should never appear as shape properties
METADATA_PREFIXES = (
    "http://www.w3.org/ns/hydra/core#",
    "http://rdfs.org/ns/void#",
    "http://www.w3.org/ns/sparql-service-description#",
)

def _is_metadata_predicate(p: str) -> bool:
    """Return True for a predicate belonging to fragment control metadata."""
    return any(p.startswith(ns) for ns in METADATA_PREFIXES)

def _is_metadata_class(c: str) -> bool:
    """Return True for a class belonging to fragment control metadata."""
    return any(c.startswith(ns) for ns in METADATA_PREFIXES)


def _term_value(term):
    """Return the plain string value of a term.

    Args:
        term: A SPARQL-JSON term dict, or already a plain string.

    Returns:
        The term's lexical value, or "" if there is none.
    """
    if isinstance(term, dict):
        return term.get("value", "")
    return str(term) if term is not None else ""


def _is_uri_term(term) -> bool:
    """Return True only for a term dict denoting an IRI.

    Blank nodes and literals are rejected on the strength of the term's
    declared type, not on what its string happens to look like.
    """
    return isinstance(term, dict) and term.get("type") == "uri"


class TPFAdapter:
    """Explores a repository through a fragments server.

    Each exploration step is written as a SPARQL query for readability,
    but it is never sent anywhere as one. `run_query_strict` breaks it
    into triple pattern requests, harvests the fragments into the local
    store and hands back the triples; this adapter then works out the
    classes and relationships from those triples itself.

    Expect this to be slower than asking a SPARQL endpoint the same
    question, since the work an endpoint would have done is being done
    here instead.

    See `nile.sphinx.adapters` for the contract the adapters share.
    """

    def __init__(self, endpoint):
        """Bind the adapter to one fragments server.

        Args:
            endpoint: Base URL of the TPF or QPF server.
        """
        self.endpoint = endpoint

    @staticmethod
    def normalize_iri(value: str) -> str:
        """Strip the angle brackets from an IRI, if it has any.

        Args:
            value: An IRI, with or without surrounding angle brackets.

        Returns:
            The bare IRI, with surrounding whitespace removed.
        """
        value = value.strip()
        if value.startswith("<") and value.endswith(">"):
            return value[1:-1]
        return value

    # ------------------------------------------
    # Shared helper: build a type-index from raw (typed) triples
    # ------------------------------------------
    @staticmethod
    def _build_indices(repo):
        """Sort harvested triples into type assertions and everything else.

        Args:
            repo: Triples as returned by `run_query_strict`, each a
                tuple of three SPARQL-JSON term dicts.

        Returns:
            A (type_of, data) pair. `type_of` maps an entity IRI to the
            set of classes it is typed with, admitting only classes that
            are IRIs, so a blank-node or literal "class" is dropped
            rather than indexed. `data` holds the remaining triples as
            plain strings, with control metadata removed.
        """
        type_of = {}     # entity → {class, ...}
        data = []

        for s, p, o in repo:
            s_val = _term_value(s)
            p_val = _term_value(p)
            o_val = _term_value(o)

            if p_val == RDF_TYPE:
                if _is_uri_term(o):
                    type_of.setdefault(s_val, set()).add(o_val)
                # else: blank-node/literal rdf:type object -- not a
                # resolvable class, dropped rather than indexed.
            else:
                if not _is_metadata_predicate(p_val):
                    data.append((s_val, p_val, o_val))

        return type_of, data

    # ------------------------------------------
    def exploratory_types(self):
        """List the classes the repository holds.

        Returns:
            Class IRIs as plain strings, in no particular order. Blank
            nodes, literals, malformed IRIs and the server's own
            metadata classes are all left out.
        """
        query = "SELECT DISTINCT ?type WHERE { ?s a ?type . }"
        repo = run_query_strict(query, [self.endpoint])

        def is_valid_class(term):
            """Return True for a term usable as a class IRI in the index."""
            if not _is_uri_term(term):
                return False
            iri = _term_value(term).strip()
            return (
                iri != ""
                and "<>" not in iri
                and " " not in iri
                and not _is_metadata_class(iri)
            )

        return list(set(
            self.normalize_iri(_term_value(o))
            for s, p, o in repo
            if _term_value(p) == RDF_TYPE and is_valid_class(o)
        ))

    # ------------------------------------------
    def outgoing_patterns(self, type_):
        """List the predicates leading out of instances of `type_`.

        Args:
            type_: IRI of the class to explore. Angle brackets, if
                present, are stripped.

        Returns:
            One solution dict per distinct (predicate, object class)
            pair. "object_type" is an empty dict wherever the object
            carries no explicit class.
        """
        type_ = self.normalize_iri(type_)
        query = f"""
        SELECT ?subject ?predicate ?object WHERE {{
            ?subject a <{type_}> .
            ?subject ?predicate ?object .
            OPTIONAL {{ ?object a ?objectType . }}
        }}
        """
        repo = run_query_strict(query, [self.endpoint])
        type_of, data = self._build_indices(repo)

        # Identify which subjects are actually of type_
        instances = {
            s for s, types in type_of.items()
            if type_ in types
        }

        results = []
        seen = set()

        for s, p, o in data:
            if s not in instances:
                continue

            # Resolve the object's classes (may be empty → one result with None)
            obj_classes = type_of.get(o) or {None}

            for obj_class in obj_classes:
                # Skip metadata classes leaking through
                if obj_class and _is_metadata_class(obj_class):
                    continue

                key = (p, obj_class)
                if key in seen:
                    continue
                seen.add(key)

                results.append({
                    "predicate":    {"type": "uri", "value": p},
                    "object_type":  (
                        {"type": "uri", "value": obj_class}
                        if obj_class is not None else {}
                    ),
                    "g":            {"type": "uri", "value": "urn:default-graph"},
                    "subject_type": {"type": "uri", "value": type_},
                })

        return results

    # ------------------------------------------
    def incoming_patterns(self, type_):
        """List the predicates leading into instances of `type_`.

        Args:
            type_: IRI of the class to explore. Angle brackets, if
                present, are stripped.

        Returns:
            One solution dict per distinct (subject class, predicate)
            pair. A subject carrying no explicit class contributes
            nothing, since an incoming relationship needs a named class
            at its far end to be matchable later.
        """
        type_ = self.normalize_iri(type_)
        query = f"""
        SELECT ?subject ?predicate ?object WHERE {{
            ?subject ?predicate ?object .
            ?object a <{type_}> .
            OPTIONAL {{ ?subject a ?subjectType . }}
        }}
        """
        repo = run_query_strict(query, [self.endpoint])
        type_of, data = self._build_indices(repo)

        # Identify which objects are actually of type_
        targets = {
            s for s, types in type_of.items()
            if type_ in types
        }

        results = []
        seen = set()

        for s, p, o in data:
            if o not in targets:
                continue

            # Resolve the subject's classes (already URI-filtered by
            # _build_indices; an unresolved/blank-node subject type is
            # simply absent from type_of and therefore skipped below)
            subj_classes = type_of.get(s) or set()

            for subj_class in subj_classes:
                if _is_metadata_class(subj_class):
                    continue

                key = (subj_class, p)
                if key in seen:
                    continue
                seen.add(key)

                results.append({
                    "predicate":    {"type": "uri", "value": p},
                    "subject_type": {"type": "uri", "value": subj_class},
                    "g":            {"type": "uri", "value": "urn:default-graph"},
                })

        return results
