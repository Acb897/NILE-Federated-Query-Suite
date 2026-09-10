"""The SPHINX indexing engine.

Turns a repository into a description of the shape of its data. For
every class the repository holds, the engine records which predicates
lead out of its instances and which lead in, together with the class at
the far end of each. The result is written out as SHACL, one file per
repository.

What comes out describes how information is organised, not which
information is stored: two repositories following the same data model
produce near-identical files even with no data in common.

Usage::

    from nile.sphinx.sphinx import Engine

    engine = Engine()
    index = engine.extract_patterns(["http://example.org/sparql"])
    engine.shacl_generator(index, "./shacl_output")

`extract_patterns` also takes a mode of "dump" or "tpf", which decides
how the sources are reached; see `nile.sphinx.adapters`.

Classes are explored in parallel. Set the SPHINX_MAX_WORKERS environment
variable, or pass `max_workers` to `Engine`, to change how many at a
time. How long indexing takes depends mostly on how many distinct
classes a repository holds, not on how many triples.

Progress is reported on standard output throughout.
"""

import hashlib
import os
import re
import threading
from urllib.parse import urlparse
from SPARQLWrapper import SPARQLWrapper, JSON
from nile.sphinx.adapters.factory import AdapterFactory

# ==============================
# Configuration (B.2.8)
# ==============================
# Previously hard-coded. Overridable via environment so that a deployment
# does not require editing the source.

# Number of classes explored concurrently during phase 2.
SPHINX_MAX_WORKERS = int(os.environ.get("SPHINX_MAX_WORKERS", "6"))

# ==============================
# SPO Pattern Container
# ==============================
class SPO:
    """One structural relationship, as recorded during exploration.

    A plain container: the class at the subject end, the predicate, the
    class at the object end, and the graph it was seen in.

    The object class may be an empty string, meaning it could not be
    determined -- the object was a literal, or was never typed. The
    relationship is still recorded, since the predicate is real even
    when what it points at is unclassified.

    The graph is only used to tell two observations of the same
    relationship apart while indexing. It does not reach the SHACL
    output.

    Attributes:
        SPO_Subject: IRI of the class at the subject end.
        SPO_Predicate: IRI of the predicate.
        SPO_Object: IRI of the class at the object end, or "".
        SPO_Graph: IRI of the graph it was observed in.
    """

    def __init__(self, params=None):
        """Build a relationship from a dict of field values.

        Args:
            params: Any of "SPO_Subject", "SPO_Predicate", "SPO_Object"
                and "SPO_Graph". Missing keys default to the empty
                string, except the graph, which defaults to
                "urn:default-graph".
        """
        params = params or {}
        self.SPO_Subject = params.get("SPO_Subject", "")
        self.SPO_Predicate = params.get("SPO_Predicate", "")
        self.SPO_Object = params.get("SPO_Object", "")
        self.SPO_Graph = params.get("SPO_Graph", "urn:default-graph")


# ==============================
# Engine
# ==============================
class Engine:
    """Explores repositories and writes out their descriptions.

    One engine indexes any number of repositories in sequence: call
    `extract_patterns` to explore them, then `shacl_generator` to write
    the result to disk.

    The engine keeps the working state of the exploration on itself and
    resets it at the start of each repository, so a single instance
    should not be driven from more than one thread. Within a repository,
    though, classes are explored concurrently and the shared bookkeeping
    is locked accordingly.

    Attributes:
        hashed_patterns: Fingerprints of the relationships already seen
            for the repository being explored, so none is recorded
            twice.
        patterns: Class IRI -> list of `SPO`, for the repository
            currently being explored.
        endpoint_graph_mode: Repository -> where its content lives:
            "default", "named", "mixed" or "none".
        endpoint_patterns: Repository -> its finished `patterns` dict.
            This is the index, and what `extract_patterns` returns.
        max_workers: How many classes are explored at once.
    """

    RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

    def __init__(self, max_workers=None):
        """Create an engine with empty state.

        Args:
            max_workers: How many classes to explore concurrently.
                Defaults to the SPHINX_MAX_WORKERS environment
                variable, or 6. Raising it speeds up repositories with
                many classes, at the cost of a heavier load on the
                source.
        """
        self.hashed_patterns = set()
        self.patterns = {}
        self.endpoint_graph_mode = {}
        self.endpoint_patterns = {}

        # B.2.8: `hashed_patterns` and `patterns` are mutated concurrently
        # by the worker pool in extract_patterns. `in_database` performs a
        # check-then-add which is NOT atomic under the GIL, so without this
        # lock two threads could both observe a pattern as unseen and both
        # record it. A single re-entrant lock guards both structures,
        # since add_triple_pattern is called immediately after in_database
        # and the pair must be atomic with respect to other workers.
        self._pattern_lock = threading.RLock()

        # B.2.8: pool size is configurable rather than hard-coded.
        self.max_workers = (
            max_workers if max_workers is not None else SPHINX_MAX_WORKERS
        )

    # --------------------------------------------------
    # Detect if endpoint contains named graphs, the default graph, or both
    # --------------------------------------------------
    def detect_named_graphs(self, endpoint_URL):
        """Work out where a repository keeps its content.

        Some repositories put everything in the default graph, some in
        named graphs, and some use both. Exploration queries have to be
        shaped accordingly, so this runs once per repository, before
        exploration begins, and records the answer in
        `endpoint_graph_mode`.

        Two separate ASK queries are needed. Asking only whether any
        named graph exists says nothing about whether the default graph
        is populated too, and assuming a repository is purely one or the
        other would silently drop half the content of one that is both.

        Args:
            endpoint_URL: URL of the SPARQL endpoint to test.

        Note:
            The recorded mode is "default", "named", "mixed", or "none"
            when both queries ran but found nothing. If neither query
            could be run at all -- an unreachable endpoint, say --
            "default" is recorded, that being the cheapest query shape
            to fall back to.
        """

        print(f"\n[Engine] Detecting graph mode for {endpoint_URL}...")

        def ask(query):
            """Run one ASK query and return its boolean answer."""
            sparql = SPARQLWrapper(endpoint_URL)
            sparql.setMethod("POST")
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            return sparql.query().convert()["boolean"]

        try:
            has_default = ask("ASK { ?s ?p ?o . }")
        except Exception:
            has_default = None

        try:
            has_named = ask("ASK { GRAPH ?g { ?s ?p ?o . } }")
        except Exception:
            has_named = None

        if has_default and has_named:
            mode = "mixed"
        elif has_named:
            mode = "named"
        elif has_default:
            mode = "default"
        elif has_default is None and has_named is None:
            # Both ASKs failed outright (e.g. connection error) --
            # fall back to the cheapest query shape rather than guessing.
            mode = "default"
        else:
            # Both ASKs succeeded but the endpoint is empty.
            mode = "none"

        self.endpoint_graph_mode[endpoint_URL] = mode
        print(f" → Graph mode detected: {mode}")

    # --------------------------------------------------
    # Deduplication
    # --------------------------------------------------
    def in_database(self, s, p, o, g):
        """Check whether a relationship is already known, and claim it.

        The check and the claim happen together under a lock, so two
        threads exploring different classes cannot both conclude that
        the same relationship is new.

        Args:
            s: IRI of the class at the subject end.
            p: IRI of the predicate.
            o: IRI of the class at the object end, or "".
            g: IRI of the graph it was observed in.

        Returns:
            True if the relationship was already known, in which case
            the caller should not record it again. False if it is new --
            and it has now been marked as seen.
        """

        digest = hashlib.sha256(f"{s}|{p}|{o}|{g}".encode()).hexdigest()

        # B.2.8: check-then-add must be atomic with respect to the other
        # workers, otherwise two threads exploring different classes can
        # both conclude that the same pattern is new.
        with self._pattern_lock:

            if digest in self.hashed_patterns:
                return True

            self.hashed_patterns.add(digest)
            return False

    # --------------------------------------------------
    # Add pattern
    # --------------------------------------------------
    def add_triple_pattern(self, type_, s, p, o, g):
        """Record one structural relationship for a class.

        Args:
            type_: IRI of the class this relationship is filed under.
            s: IRI of the class at the subject end.
            p: IRI of the predicate.
            o: IRI of the class at the object end, or "" if unknown.
            g: IRI of the graph it was observed in. Anything empty
                becomes "urn:default-graph".

        Note:
            Callers resolve `s` and `o` to a class IRI only when the
            underlying term really is an IRI, never a blank node or a
            literal, and pass "" otherwise. That decision is not made
            here by testing the string for an "http" prefix, because a
            class IRI need not use the http(s) scheme -- urn:, doi: and
            ark: are all legitimate.

            Relationships with no predicate or no subject class are
            dropped, as are rdf:type relationships. SHACL has no
            structural way to say "this exact value", and RIDDLE strips
            rdf:type out of queries in any case, so such a relationship
            could never be matched against.
        """

        s = str(s).strip()
        p = str(p).strip()
        o = str(o).strip()
        g = str(g).strip() if str(g).strip() else "urn:default-graph"

        if not p:
            return

        if not s:
            return

        # rdf:type patterns are never encodable as a structural (A, P, B)
        # relationship: SHACL's only vocabulary for "this exact value" is
        # sh:hasValue, an instance-specific value constraint, not a
        # structural one -- and RIDDLE's query-side abstraction already
        # strips every rdf:type triple out of a query's APB set (it
        # supplies typing information, not a data relationship), so such
        # a constraint could never be matched against anyway. Rejected
        # outright here rather than stored and filtered later.
        if p == self.RDF_TYPE:
            return

        # B.2.8: guarded because several workers may target the same
        # `type_` key (the incoming exploration of class B records under
        # B while the outgoing exploration of A also records under A),
        # and because setdefault-then-append is not atomic.
        with self._pattern_lock:

            if type_ not in self.patterns:
                self.patterns[type_] = []

            self.patterns[type_].append(
                SPO({
                    "SPO_Subject": s,
                    "SPO_Predicate": p,
                    "SPO_Object": o,
                    "SPO_Graph": g
                })
            )

    # --------------------------------------------------
    # Graph scoping helper
    # --------------------------------------------------
    def _graph_scope(self, graph_mode, content, graph_var, bind_graph_var=False):
        """Scope a query fragment so it matches whichever graph it is in.

        Args:
            graph_mode: Where the repository keeps its content, as
                established by `detect_named_graphs`.
            content: A graph-pattern fragment with no GRAPH clause of
                its own.
            graph_var: Name of the graph variable to use, without
                the "?".
            bind_graph_var: Whether to bind `graph_var` to
                "urn:default-graph" in the default-graph branch. Set
                this only for the fragment whose graph variable is
                selected; for the others it is a throwaway.

        Returns:
            The fragment scoped to the default graph, to any named
            graph, or to either, depending on `graph_mode`.

        Note:
            Each fragment of a query gets its own graph variable rather
            than the whole query being wrapped in a single GRAPH block.
            Sharing one variable would force every part of the query to
            match in the same named graph, hiding any relationship whose
            two ends were asserted in different named graphs of the same
            repository.
        """
        default_branch = content
        if bind_graph_var:
            default_branch = (
                f'{content}\nBIND(IRI("urn:default-graph") AS ?{graph_var})'
            )
        named_branch = f"GRAPH ?{graph_var} {{ {content} }}"

        if graph_mode == "default":
            return default_branch
        elif graph_mode == "named":
            return named_branch
        else:  # "mixed" or "none" -- check both, independently
            return f"{{ {default_branch} }} UNION {{ {named_branch} }}"

    # --------------------------------------------------
    # Query Builder
    # --------------------------------------------------
    def build_query(self, endpoint_URL, mode, type_=None):
        """Build the SPARQL query for one exploration step.

        Args:
            endpoint_URL: The repository being explored, used to look up
                where it keeps its content.
            mode: Which step to build. "exploratory" lists the classes,
                "fixed_subject" lists what leads out of a class, and
                "fixed_object" lists what leads into it.
            type_: IRI of the class being explored. Needed by
                "fixed_subject" and "fixed_object", unused otherwise.

        Returns:
            The query as a string, or None if `mode` is not one of the
            three.

        Note:
            Every query selects DISTINCT, so a row comes back per
            distinct combination rather than per matching triple. What
            is transferred therefore follows how varied a repository is,
            not how large.
        """

        graph_mode = self.endpoint_graph_mode.get(endpoint_URL, "mixed")

        if mode == "exploratory":

            scoped = self._graph_scope(
                graph_mode, "?subject a ?type .", "g", bind_graph_var=True
            )

            return f"""
            SELECT DISTINCT ?type ?g
            WHERE {{
              {scoped}
            }}
            """

        elif mode == "fixed_subject":

            # NOTE: `?subject a ?subject_type` was previously joined in here
            # but is never consumed downstream (process_type only reads
            # object_type for the outgoing direction) -- it only inflated
            # the result set by the number of rdf:type assertions on each
            # subject. Removed, and DISTINCT added, so the transferred
            # volume tracks the number of distinct (predicate, object
            # class) pairs -- i.e. P+(c) as defined -- rather than the
            # number of triples times the subject's type multiplicity.
            type_clause = self._graph_scope(
                graph_mode, f"?subject a <{type_}> .", "tg"
            )
            data_clause = self._graph_scope(
                graph_mode, "?subject ?predicate ?object .", "g",
                bind_graph_var=True,
            )
            object_type_clause = self._graph_scope(
                graph_mode, "?object a ?object_type .", "og"
            )

            return f"""
            SELECT DISTINCT ?predicate ?object_type ?g
            WHERE {{
              {type_clause}
              {data_clause}
              OPTIONAL {{ {object_type_clause} }}
            }}
            """

        elif mode == "fixed_object":

            object_type_clause = self._graph_scope(
                graph_mode, f"?object a <{type_}> .", "tg"
            )
            data_clause = self._graph_scope(
                graph_mode, "?subject ?predicate ?object .", "g",
                bind_graph_var=True,
            )
            subject_type_clause = self._graph_scope(
                graph_mode, "?subject a ?subject_type .", "sg"
            )

            return f"""
            SELECT DISTINCT ?subject_type ?predicate ?g
            WHERE {{
              {object_type_clause}
              {data_clause}
              OPTIONAL {{ {subject_type_clause} }}
            }}
            """

    # --------------------------------------------------
    # Execute query
    # --------------------------------------------------
    def query_endpoint(self, endpoint_URL, mode, type_=None):
        """Run one exploration query and return its solutions.

        Args:
            endpoint_URL: URL of the SPARQL endpoint to query.
            mode: Which exploration step to run; see `build_query`.
            type_: IRI of the class being explored, where the step needs
                one.

        Returns:
            Solution dicts in SPARQL-JSON form, or an empty list if the
            query failed. Failures are reported and swallowed rather
            than raised, so that one unresponsive repository does not
            abandon the whole run.
        """

        print(f" [Engine] Executing {mode} query for {type_ if type_ else 'N/A'}...")

        sparql = SPARQLWrapper(endpoint_URL)
        sparql.setMethod("POST")
        sparql.setReturnFormat(JSON)

        query = self.build_query(endpoint_URL, mode, type_)
        sparql.setQuery(query)

        try:
            results = sparql.query().convert()

            bindings = results["results"]["bindings"]
            count = len(bindings)

            print(f" → {count} row(s) received.")

            return bindings

        except Exception as e:

            print(f" [Engine] SPARQL error on {endpoint_URL}: {e}")
            return []

    # --------------------------------------------------
    # Extract patterns
    # --------------------------------------------------
    def extract_patterns(self, sources, mode="sparql"):
        """Explore repositories and build their structural descriptions.

        Repositories are processed one after another. For each, the
        engine establishes where its content lives, lists its classes,
        then explores those classes concurrently, recording what leads
        into and out of every one.

        Args:
            sources: The repositories to index -- SPARQL endpoint URLs
                for "sparql" mode, RDF file paths for "dump" mode, or
                fragments server URLs for "tpf" mode.
            mode: How to reach them: "sparql" (the default), "dump" or
                "tpf". One call uses one mode for all its sources.

        Returns:
            A dict mapping each source to its own dict of class IRI ->
            list of `SPO`. Hand it straight to `shacl_generator`.

        Note:
            Only "sparql" mode looks for named graphs; dump and TPF
            sources report everything as being in the default graph.

            A source that cannot be reached contributes an empty entry
            rather than raising, so one bad repository does not cost the
            rest of the run.
        """

        self.endpoint_patterns = {}
        print(f"\n[Engine] Starting pattern extraction")
        print(f"          → Mode: {mode.upper()}")
        print(f"          → Sources: {len(sources)} endpoint(s)")
        for src in sources:
            print(f"            - {src}")

        for source in sources:

            adapter = AdapterFactory.create(source, mode, self)

            # ⚠️ pass engine only for SPARQL
            if mode == "sparql":
                adapter.engine = self

            with self._pattern_lock:
                self.patterns = {}
                self.hashed_patterns = set()

            # ------------------------------------------------------------
            # Phase 0: graph-mode detection.
            #
            # This determines whether the source's content resides in the
            # default graph, in named graphs, or in both, and is what
            # build_query consults to decide the scoping shape of every
            # exploration query. It MUST run before any exploration query
            # is issued, otherwise build_query falls through to its
            # "mixed" default and the permissive two-branch UNION is
            # applied unconditionally.
            #
            # Only the SPARQL adapter routes its queries through
            # build_query; the TPF and dump adapters construct their own
            # requests and attribute every pattern to the default graph,
            # so the detection is neither consulted nor issued for them.
            # ------------------------------------------------------------
            if mode == "sparql":
                self.detect_named_graphs(source)
            else:
                self.endpoint_graph_mode[source] = "default"
                print(f"\n[Engine] Graph mode for {source}: "
                      f"default (not applicable in {mode.upper()} mode)")

            print("\n[Engine] Phase 1: exploratory scan...")
            types = adapter.exploratory_types()

            print(f" → Detected {len(types)} classes.")

            print("[Engine] Phase 2: expansion...")

            from concurrent.futures import ThreadPoolExecutor

            def _is_uri_binding(binding):
                """Return True only for a SPARQL-JSON binding denoting an IRI.

                Blank nodes and literals are rejected here, so a class is
                never accepted on the strength of what its string looks
                like. Adapters that do not speak SPARQL-JSON natively are
                expected to set the "type" key themselves.
                """
                return bool(binding) and binding.get("type") == "uri"

            def process_type(type_):

                # outgoing
                """Explore one class and record what it links to."""
                for sol in adapter.outgoing_patterns(type_):

                    g = sol.get("g", {}).get("value", "urn:default-graph")
                    p = sol.get("predicate", {}).get("value", "")
                    o_binding = sol.get("object_type", {})
                    o = o_binding.get("value", "") if _is_uri_binding(o_binding) else ""

                    if not self.in_database(type_, p, o, g):
                        self.add_triple_pattern(type_, type_, p, o, g)

                # incoming
                for sol in adapter.incoming_patterns(type_):

                    s_binding = sol.get("subject_type", {})
                    s = s_binding.get("value", "") if _is_uri_binding(s_binding) else ""
                    p = sol.get("predicate", {}).get("value", "")
                    g = sol.get("g", {}).get("value", "urn:default-graph")

                    if not s:
                        # Subject type unresolved or not a URI (blank node,
                        # literal-typed anomaly) -- an incoming relationship
                        # cannot be attached to an unnamed neighbour class,
                        # so it is dropped rather than silently mis-typed.
                        continue

                    if not self.in_database(s, p, type_, g):
                        self.add_triple_pattern(type_, s, p, type_, g)

            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                pool.map(process_type, types)

            with self._pattern_lock:
                self.endpoint_patterns[source] = self.patterns

        return self.endpoint_patterns

    # --------------------------------------------------
    # SHACL Generator
    # --------------------------------------------------
    def shacl_generator(self, patterns_hash, output_dir):
        """Write the structural descriptions out as SHACL files.

        Every class becomes a NodeShape targeting it, carrying one
        property shape per predicate observed. Where the class at the
        far end of a predicate is known it is recorded as sh:class;
        where it is not, the property shape carries the path alone,
        which RIDDLE reads as "unknown" rather than "no such
        relationship".

        The repository each file describes is recorded as dct:source,
        which is how RIDDLE can report the repository itself rather than
        a filename.

        No cardinality, datatype or other instance-level constraints are
        emitted. These files describe shape; although SHACL is a
        validation language, nothing here is meant to validate anything.

        Args:
            patterns_hash: The index returned by `extract_patterns`.
            output_dir: Directory to write into. Created if missing.

        Returns:
            True. Failure is not reported through the return value.

        Note:
            Filenames come from each source's host and path. If that
            name is taken already -- by a different source, or by a file
            left behind by an earlier run -- a short hash of the URL is
            appended instead. Re-indexing into a directory that already
            holds output therefore adds files rather than replacing
            them, and since RIDDLE reads every file in the directory it
            is given, clearing it first avoids matching against a stale
            copy.
        """

        print(f"\n[Engine] Generating SHACL files in {output_dir}...")

        os.makedirs(output_dir, exist_ok=True)

        for url, patterns in patterns_hash.items():

            print(f" [Engine] Building SHACL for {url}...")

            shacl = []

            shacl.append("""@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix dct:  <http://purl.org/dc/terms/> .
""")

            grouped = {}

            for _, values in patterns.items():
                for pattern in values:
                    grouped.setdefault(pattern.SPO_Subject, []).append(pattern)

            for subject, lst in grouped.items():

                shacl.append(f"<{subject}Shape>\n")
                shacl.append("  a sh:NodeShape ;\n")
                shacl.append(f"  sh:targetClass <{subject}> ;\n")
                shacl.append(f"  dct:source <{url}> ;\n")

                grouped_props = {}

                for pat in lst:
                    key = (pat.SPO_Predicate, pat.SPO_Object.strip())
                    grouped_props.setdefault(key, []).append(pat)

                items = list(grouped_props.items())

                # NOTE: predicate == rdf:type never reaches this point --
                # add_triple_pattern rejects it unconditionally, since
                # SHACL has no structural (non-instance-specific) way to
                # encode "this exact value" (only sh:hasValue, which is a
                # value constraint), and RIDDLE's query-side abstraction
                # discards rdf:type triples entirely, so such a shape
                # could never be matched against regardless of how it
                # was encoded.
                for idx, ((predicate, object_str), _) in enumerate(items):

                    shacl.append("  sh:property [\n")
                    shacl.append(f"    sh:path <{predicate}> ;\n")

                    if object_str and object_str != "urn:default-graph":
                        shacl.append(f"    sh:class <{object_str}> ;\n")

                    end = "." if idx == len(items) - 1 else ";"

                    shacl.append(f"  ]{end}\n")

                shacl.append("\n")

            uri = urlparse(url)

            host = re.sub(r"[^a-zA-Z0-9]", "_", uri.hostname or "unknown")
            path = re.sub(r"[^a-zA-Z0-9]", "_", uri.path if uri.path else "root")

            base = re.sub(r"_+", "_", f"{host}{path}").strip("_")

            filename = f"{base}.ttl"

            output_path = os.path.join(output_dir, filename)

            if os.path.exists(output_path):

                short = hashlib.sha256(url.encode()).hexdigest()[:6]

                filename = f"{base}_{short}.ttl"
                output_path = os.path.join(output_dir, filename)

            print(f" → Writing {output_path}")

            with open(output_path, "w", encoding="utf-8") as f:
                f.write("".join(shacl))

        return True