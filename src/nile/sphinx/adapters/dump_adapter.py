"""Indexing a repository from a downloadable RDF file."""

from collections import defaultdict
from rdflib import Graph, URIRef



class RDFDumpAdapter:
    """Explores a repository published as an RDF file.

    Lets a repository be indexed even when it runs no query service at
    all: a file is enough. The file is parsed into memory once, when the
    adapter is built, and three lookup tables are prepared from it so
    that the exploration methods can answer without re-scanning the
    graph each time.

    Because everything is held in memory, the practical limit is the
    size of the file rather than the time exploration takes.

    Everything the file contains is reported as living in the default
    graph. Named graphs in a quad-based file are not distinguished --
    that distinction only matters for SPARQL endpoints, which
    `SPARQLAdapter` handles.

    See `nile.sphinx.adapters` for the contract the adapters share.

    Attributes:
        graph: The parsed rdflib Graph.
        type_index: Entity IRI -> the set of classes it is typed with.
            Only classes that are themselves IRIs are recorded.
        spo_index: Subject -> list of (predicate, object) pairs.
        pos_index: Object -> list of (subject, predicate) pairs.
    """

    RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    def __init__(self, file_path):
        """Parse the file and build the lookup tables.

        Args:
            file_path: Path or URL of the RDF file. rdflib guesses the
                serialisation, normally from the extension.

        Raises:
            Exception: Whatever rdflib raises when the file cannot be
                read or parsed. Nothing is caught here, so an
                unreadable file stops the run rather than quietly
                producing an empty index.
        """

        self.graph = Graph()
        self.graph.parse(file_path)

        # type_index: entity IRI -> set of class IRIs (rdf:type objects
        # that are themselves URIRefs; blank-node classes are excluded,
        # see note below)
        self.type_index = defaultdict(set)
        self.spo_index = defaultdict(list)
        self.pos_index = defaultdict(list)

        for s, p, o in self.graph:

            # Decide URI-vs-blank-node/literal from the actual rdflib term
            # *before* stringifying, so a blank node is never mistaken for
            # a resolvable IRI just because it stringifies without an
            # "http" prefix check. Non-URIRef subjects/objects are still
            # indexed for SPO/POS traversal (a blank-node subject can
            # still carry outgoing data properties), but are never
            # eligible to populate type_index as a *class*.
            s_is_uri = isinstance(s, URIRef)
            o_is_uri = isinstance(o, URIRef)

            s_str = str(s)
            p_str = str(p)
            o_str = str(o)

            self.spo_index[s_str].append((p_str, o_str))
            self.pos_index[o_str].append((s_str, p_str))

            if p_str == self.RDF_TYPE and o_is_uri:
                self.type_index[s_str].add(o_str)

    # ------------------------------------------

    def exploratory_types(self):
        # Already URI-filtered at index-build time (see __init__): only
        # rdf:type objects that were URIRef instances were admitted into
        # type_index, so nothing further to check here. Returned as plain
        # class-IRI strings, matching the SPARQLAdapter contract used by
        # Engine.process_type.
        """List the classes the file holds.

        Returns:
            Class IRIs as plain strings, in no particular order.
            Blank-node classes were already excluded when the lookup
            tables were built.
        """
        all_types = set()
        for t in self.type_index.values():
            all_types.update(t)
        return list(all_types)

    # ------------------------------------------

    def outgoing_patterns(self, type_):
        """List the predicates leading out of instances of `type_`.

        Args:
            type_: IRI of the class to explore.

        Returns:
            One solution dict per (predicate, object class) pair
            observed. "object_type" is an empty dict wherever the object
            carries no explicit class, which the Engine reads as
            "unknown" rather than "absent".

        Note:
            Unlike `incoming_patterns`, repeats are not filtered out
            here. The Engine discards them as it records them, so the
            index is unaffected; only the size of this list is.
        """

        results = []

        for s, types in self.type_index.items():

            if type_ not in types:
                continue

            for p, o in self.spo_index[s]:

                obj_types = self.type_index.get(o) or {None}

                for ot in obj_types:
                    results.append({
                        "predicate": {"type": "uri", "value": p},
                        "object_type": (
                            {"type": "uri", "value": ot}
                            if ot is not None else {}
                        ),
                        "g": {"type": "uri", "value": "urn:default-graph"},
                        "subject_type": {"type": "uri", "value": type_},
                    })

        return results

    # ------------------------------------------

    def incoming_patterns(self, type_):
        """List the predicates leading into instances of `type_`.

        Instances of `type_` are looked up in `type_index` first, and
        `pos_index` is then consulted once per instance. Looking `type_`
        up in `pos_index` directly would find the triples pointing at
        the class itself rather than at its instances -- almost entirely
        rdf:type assertions, and almost never what is wanted.

        Args:
            type_: IRI of the class to explore.

        Returns:
            One solution dict per distinct (subject class, predicate)
            pair. rdf:type is skipped, since it describes typing rather
            than a relationship between resources.

        Note:
            A subject carrying no explicit class contributes nothing. An
            incoming relationship has to have a named class at its far
            end to be matchable against a query later on.
        """

        results = []
        seen = set()

        # Entities explicitly typed with `type_`. type_index only ever
        # admits URIRef classes (see __init__), so a blank-node "class"
        # cannot reach this point.
        instances = [
            entity for entity, types in self.type_index.items()
            if type_ in types
        ]

        for instance in instances:

            for s, p in self.pos_index.get(instance, ()):

                # rdf:type is not a structural relationship and is
                # rejected downstream anyway; skipping it here keeps the
                # result set proportional to the number of distinct
                # (subject class, predicate) pairs, matching the
                # behaviour of the SPARQL and TPF adapters.
                if p == self.RDF_TYPE:
                    continue

                subject_types = self.type_index.get(s) or set()

                for st in subject_types:

                    # An incoming relationship cannot be attached to an
                    # unnamed neighbour class, so an untyped subject
                    # contributes nothing -- consistent with
                    # Engine.process_type, which drops such patterns.
                    key = (st, p)
                    if key in seen:
                        continue
                    seen.add(key)

                    results.append({
                        "predicate": {"type": "uri", "value": p},
                        "subject_type": {"type": "uri", "value": st},
                        "g": {"type": "uri", "value": "urn:default-graph"},
                    })

        return results
