"""SCARAB: the federated harvester.

Retrieves the data a query needs from any number of repositories, loads
it into one local triplestore, and leaves the query to be evaluated
there.

The query is broken into its triple patterns and each repository is
asked for one pattern at a time. Patterns are requested smallest first,
and each is constrained by what the previous ones returned -- a bind
join -- so that little more comes back than the query can use.
Everything retrieved is kept, whether or not the repository it came from
can answer the rest of the query. That is what allows an answer to be
assembled from triples no single repository held together.

Usage::

    from nile.scarab.scarab_harvester import (
        FindBGPPriority, execute_sparql_query)

    FindBGPPriority(query, [
        "http://example.org/fragments",      # fragments server
        "sparql:http://example.org/sparql",  # SPARQL endpoint
        "dump:/data/repository.ttl",         # local RDF file
    ])
    results = execute_sparql_query(query)

Sources
    A bare http(s) location is read as a fragments server. A SPARQL
    endpoint has to be declared with a "sparql:" prefix, because the two
    cannot be told apart by their URL and mistaking one for the other
    would quietly yield nothing at all. See `make_datasource`.

Configuration
    Read from the environment at import time::

        SCARAB_STORE_BASE            local triplestore, e.g.
                                     http://localhost:7200
        SCARAB_STORE_REPOSITORY      repository within it
        SCARAB_STORE_QUERY_URL       overrides the derived query URL
        SCARAB_STORE_STATEMENTS_URL  overrides the derived statements URL
        SCARAB_MAX_THREADS           concurrent requests per bind join
        SCARAB_HARVEST_PATH_BASE_PREDICATES
                                     set to "0" to stop property paths
                                     retrieving their base predicates

    Only the SPARQL protocol and the RDF graph store protocol are used,
    so any conformant triplestore can stand in for the GraphDB
    deployment these default to.

Pattern dicts
    A query pattern travels through the module as a dict::

        {
            "subject":   <term>,
            "predicate": <term> or "__PATH__",
            "object":    <term>,
            "graph":     <term> or None,
            "predicate_path":    rdflib path, on path patterns only,
            "derived_from_path": str, on generated support patterns only,
        }

    Every position is a string in one canonical encoding, which the
    request builder and the response filter both depend on::

        variable     ?name
        IRI          http://example.org/x      (bare, any scheme)
        literal      "lex", "lex"@en, "lex"^^<datatype>   (N3 form)
        blank node   _:label
        path         the "__PATH__" sentinel

    The N3 form for literals is what the Linked Data Fragments
    specification expects in a fragment selector, so a request built
    this way is correct as well as internally consistent. Note that an
    IRI is never recognised by an "http" prefix anywhere in this module,
    since urn:, doi: and ark: are all perfectly ordinary.

Provenance
    Each run mints one nanopublication per source. The graph the
    harvested triples are written into *is* that nanopublication's
    assertion graph, so nothing is stored twice. See
    `write_nanopub_graphs`.

State
    The page cache, the ingestion queue and its daemon thread are
    module-level and shared across the process, so one harvesting run at
    a time per process. Progress is reported on standard output
    throughout.
"""

# == SCARAB: Federated Harvester with Binding Propagation & Blank Node Safety ==
#
# Python implementation of the Ruby version with the following improvements:
#
# 1. SPARQL algebra parsing via RDFLib (no regex)
# 2. Concurrent fragment requests
# 3. RDFa page caching (avoid repeated parsing)
# 4. Vectorized bind-join batches
# 5. Unified datasource abstraction (TPF/QPF / SPARQL endpoint / RDF dump)
# 6. Nanopublication provenance tracking (one nanopub per source per run)
#
# Behaviour intentionally matches the Ruby version:
# - Harvest patterns independently per source
# - Does NOT require sources to answer the whole query
# - Store harvested triples locally
#
# This module is the SINGLE harvesting back-end for the NILE suite. It is
# imported both by SCARAB's own entry points and by SPHINX's TPFAdapter
# (via run_query_strict), replacing the previously divergent copy that
# lived alongside the indexer.

import os
import re
import json
import uuid as _uuid_mod
import requests
import threading
from pathlib import Path as _FsPath
from urllib.parse import urlencode, urljoin, quote
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

from rdflib import ConjunctiveGraph, Graph, URIRef, Literal, Variable, BNode
from rdflib.namespace import Namespace, RDF, RDFS, XSD
from rdflib.util import from_n3
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parserutils import Expr
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib import paths as rdflib_paths
from rdflib.paths import (
    MulPath, SequencePath, AlternativePath,
    InvPath, NegatedPath, Path
)
import time
import queue

# -------------------------------------------------------------------------
# NAMESPACES
# -------------------------------------------------------------------------

HYDRA  = Namespace("http://www.w3.org/ns/hydra/core#")
VOID   = Namespace("http://rdfs.org/ns/void#")
PROV   = Namespace("http://www.w3.org/ns/prov#")
NP     = Namespace("http://www.nanopub.org/nschema#")
NPX    = Namespace("http://purl.org/nanopub/x/")
DCAT   = Namespace("http://www.w3.org/ns/dcat#")
DCT    = Namespace("http://purl.org/dc/terms/")
SCHEMA = Namespace("https://schema.org/")

# -------------------------------------------------------------------------
# GLOBAL SETTINGS
# -------------------------------------------------------------------------

MAX_THREADS = int(os.environ.get("SCARAB_MAX_THREADS", "3"))
BIND_BATCH_SIZE = MAX_THREADS * 5
MAX_BUFFER_BYTES   = 10_000_000  # ~10MB safety cap
FLUSH_INTERVAL     = 5           # seconds
_cache_lock = threading.Lock()
page_cache = {}
_repo_lock  = threading.Lock()
_parse_lock = threading.Lock()

BUFFER_QUEUE_MAXSIZE = 100_000  # prevents memory explosion
_ingest_queue = queue.Queue(maxsize=BUFFER_QUEUE_MAXSIZE)

INDEXING_MODE = False
ALLOW_PARTIAL = True

# B.1.3: when True, the base predicates referenced by a property path are
# scheduled as ordinary triple patterns so that the local closure has
# triples to traverse. Retrieving the full extent of a base predicate can
# be expensive; setting this to False restores the previous behaviour, in
# which a path is evaluated only over whatever the rest of the query
# happened to materialize.
HARVEST_PATH_BASE_PREDICATES = os.environ.get(
    "SCARAB_HARVEST_PATH_BASE_PREDICATES", "1"
) not in ("0", "false", "False")

# -------------------------------------------------------------------------
# LOCAL STORE CONFIGURATION (B.2.8)
# -------------------------------------------------------------------------
# The address of the local triplestore was previously hard-coded at four
# separate points in this module. It is now derived once, from environment
# variables, so that a deployment does not require editing the source and
# so that the query and statements URLs cannot drift apart.
#
#   SCARAB_STORE_BASE        e.g. http://localhost:7200
#   SCARAB_STORE_REPOSITORY  e.g. test1
#   SCARAB_STORE_QUERY_URL       (optional, overrides the derived value)
#   SCARAB_STORE_STATEMENTS_URL  (optional, overrides the derived value)
#
# Only the SPARQL protocol and the RDF graph store protocol are used, so
# any conformant store may be substituted for the reference GraphDB
# deployment.

STORE_BASE = os.environ.get("SCARAB_STORE_BASE", "http://acb8computer:7200")
STORE_REPOSITORY = os.environ.get("SCARAB_STORE_REPOSITORY", "test1")

STORE_QUERY_URL = os.environ.get(
    "SCARAB_STORE_QUERY_URL",
    f"{STORE_BASE}/repositories/{STORE_REPOSITORY}",
)
STORE_STATEMENTS_URL = os.environ.get(
    "SCARAB_STORE_STATEMENTS_URL",
    f"{STORE_QUERY_URL}/statements",
)

# B.2.8: the ingestion daemon was previously started on every call to
# FindBGPPriority, spawning one additional consumer thread per invocation.
# It is now started exactly once, under a lock.
_flusher_lock = threading.Lock()
_flusher_started = False


def _ensure_flusher_running():
    """Start the ingestion daemon, once per process.

    Safe to call repeatedly; only the first call starts a thread.
    """
    global _flusher_started
    with _flusher_lock:
        if _flusher_started:
            return
        threading.Thread(target=buffer_flusher_daemon, daemon=True).start()
        _flusher_started = True

# -------------------------------------------------------------------------
# SCARAB SOFTWARE IDENTITY (used in nanopub provenance)
# -------------------------------------------------------------------------

SCARAB_CODEBASE_URI = URIRef("https://github.com/Acb897/NILE-Federated-Query-Suite/tree/main/src/nile/scarab")
SCARAB_VERSION      = "1.0.0"
SCARAB_DOWNLOAD_URI = URIRef(
    f"https://github.com/Acb897/NILE-Federated-Query-Suite/tree/main/src/nile/scarab/releases/tag/v{SCARAB_VERSION}"
)

# -------------------------------------------------------------------------
# NANOPUB URI MINTING
# -------------------------------------------------------------------------

def mint_nanopub_uri(run_id: str, endpoint_index: int) -> str:
    """Mint the nanopublication base URI for one run and source.

    Derived from the run identifier and the source's position using UUID
    v5, so it is stable: the same run and source always produce the same
    URI, and separate runs never overwrite one another's records.

    Args:
        run_id: Identifier for the harvesting run.
        endpoint_index: 1-based position of the source within the run.

    Returns:
        The base URI. The four graphs of the nanopublication are named
        by appending "#Head", "#assertion", "#provenance" and
        "#pubinfo".
    """
    slug = _uuid_mod.uuid5(
        _uuid_mod.NAMESPACE_URL,
        f"{run_id}/endpoint{endpoint_index}"
    )
    return f"urn:tpf:nanopub:{slug}"


# -------------------------------------------------------------------------
# CANONICAL PATTERN TERM ENCODING (B.2.1, B.2.4)
# -------------------------------------------------------------------------
# A pattern position is carried through the module as a string. Previously
# two mutually incompatible encodings were in use: transform() emitted the
# bare lexical form of a literal (discarding its datatype and language
# tag), while fetch_binding().concretize() emitted the N3 form. The
# request builder and the response filter therefore disagreed about what a
# literal looked like, and IRIs were distinguished from literals by testing
# for an "http" prefix, which misclassifies every other scheme (urn:, doi:,
# ark:, info:, ...).
#
# One encoding is now used everywhere:
#
#   variable   ->  "?name"
#   IRI        ->  "http://example.org/x"   (bare, any scheme)
#   literal    ->  '"lex"', '"lex"@en', '"lex"^^<datatype>'   (N3)
#   blank node ->  "_:label"
#   path       ->  "__PATH__" sentinel
#
# The N3 form for literals is also what the Linked Data Fragments
# specification expects in the `object` selector, so the request sent to a
# TPF/QPF server is correct as well as internally consistent.

_PATH_SENTINEL = "__PATH__"


def term_to_pattern_str(term):
    """Encode an rdflib term as a canonical pattern position.

    Args:
        term: An rdflib term, an already-encoded string, or None.

    Returns:
        The encoding described in the module docstring, or None for
        None. Anything unrecognised becomes the "__PATH__" sentinel.
    """
    if term is None:
        return None
    if isinstance(term, Variable):
        return f"?{term}"
    if isinstance(term, Literal):
        return term.n3()
    if isinstance(term, URIRef):
        return str(term)
    if isinstance(term, BNode):
        return f"_:{term}"
    if isinstance(term, str):
        return term
    return _PATH_SENTINEL


def parse_pattern_term(text):
    """Decode a canonical pattern position back into an rdflib term.

    The inverse of `term_to_pattern_str`.

    Args:
        text: An encoded pattern position.

    Returns:
        The rdflib term, or None where the position is unbound and so
        restricts nothing -- a variable, the path sentinel, or nothing
        at all.

    Note:
        Anything that is not a variable, a blank node, an
        angle-bracketed IRI or an N3 literal is read as a bare IRI,
        whatever its scheme.
    """
    if text is None or text == _PATH_SENTINEL:
        return None
    if not isinstance(text, str):
        return term_to_pattern_str(text) and text
    if text.startswith("?"):
        return None
    if text.startswith("_:"):
        return BNode(text[2:])
    if text.startswith("<") and text.endswith(">"):
        return URIRef(text[1:-1])
    if text.startswith('"') or text.startswith("'"):
        # N3 literal, possibly with a language tag or datatype
        try:
            return from_n3(text)
        except Exception:
            return Literal(text.strip('"\''))
    # Anything else is an IRI. NOTE: no scheme test is applied, so urn:,
    # doi:, ark: and friends are handled correctly (B.2.4).
    return URIRef(text)


def pattern_term_matches(req, value):
    """Return True when an RDF term satisfies a pattern position.

    Args:
        req: The encoded pattern position. An unbound one matches
            anything.
        value: The rdflib term to test.

    Returns:
        True if the term satisfies the position.

    Note:
        Comparison is by RDF term equality, not string equality, so a
        plain literal is not conflated with an equally-spelled typed or
        language-tagged one. IRIs get a lenient fallback on their string
        form, for servers that echo an IRI back in a different but
        equivalent shape. Literals do not, since their datatype and
        language are significant.
    """
    expected = parse_pattern_term(req)
    if expected is None:
        return True
    if value == expected:
        return True
    # Lenient fallback for IRI positions only: some servers echo an IRI in
    # a different but equivalent lexical form. Literals are compared
    # strictly, since datatype and language are significant.
    if isinstance(expected, URIRef) and not isinstance(value, Literal):
        return str(value) == str(expected)
    return False


def sparql_term(text):
    """Render a canonical pattern position for use in a SPARQL query.

    Args:
        text: An encoded pattern position.

    Returns:
        Variables, blank nodes, literals and already-bracketed IRIs
        unchanged; anything else angle-bracketed as an IRI, whatever its
        scheme. None for None.
    """
    if text is None:
        return None
    if text.startswith("?"):
        return text
    if text.startswith("_:"):
        return text
    if text.startswith("<") and text.endswith(">"):
        return text
    if text.startswith('"') or text.startswith("'"):
        return text
    return f"<{text}>"


def term_from_sparql_json(binding):
    """Rebuild an rdflib term from a SPARQL-JSON binding.

    Lets bindings read back out of the local store keep their node kind,
    datatype and language tag rather than being flattened to a bare
    string -- which is exactly the information the request encoding
    depends on.

    Args:
        binding: A SPARQL-JSON binding dict.

    Returns:
        The rdflib term. Anything that is not a dict becomes a plain
        literal of its string form.
    """
    if not isinstance(binding, dict):
        return Literal(str(binding))

    kind = binding.get("type")
    value = binding.get("value", "")

    if kind == "uri":
        return URIRef(value)
    if kind == "bnode":
        return BNode(value)
    if binding.get("xml:lang"):
        return Literal(value, lang=binding["xml:lang"])
    if binding.get("datatype"):
        return Literal(value, datatype=URIRef(binding["datatype"]))
    return Literal(value)


# -------------------------------------------------------------------------
# SPARQL ALGEBRA PARSING
# -------------------------------------------------------------------------

def extract_all_patterns(node, patterns=None, graph_term=None):
    """Collect every triple pattern in a parsed query, with its graph.

    Walks an rdflib SPARQL algebra tree, descending through OPTIONAL,
    UNION, MINUS, FILTER and GRAPH alike. Each pattern is recorded
    together with the graph term of the GRAPH clause it sits under, if
    any, so patterns come back as quads rather than triples.

    Args:
        node: An algebra node, normally the root of a translated query.
        patterns: Accumulator for the recursion. Leave unset.
        graph_term: The graph in force at this point. Leave unset.

    Returns:
        A list of pattern dicts holding rdflib terms, not yet encoded. A
        pattern whose predicate is a property path carries the
        "__PATH__" sentinel, with the expression preserved under
        "predicate_path".
    """
    if patterns is None:
        patterns = []
    if node is None:
        return patterns

    node_name = getattr(node, "name", None)

    if node_name == "BGP":
        for s, p, o in node.triples:
            if isinstance(p, (MulPath, SequencePath, AlternativePath,
                               InvPath, NegatedPath)):
                patterns.append({
                    "subject":        s,
                    "predicate":      "__PATH__",
                    "predicate_path": p,
                    "object":         o,
                    "graph":          graph_term,
                })
            else:
                patterns.append({
                    "subject":   s,
                    "predicate": p,
                    "object":    o,
                    "graph":     graph_term,
                })
        return patterns

    # Safety net for RDFLib versions that do produce a Path algebra node
    if node_name == "Path":
        patterns.append({
            "subject":        node.s,
            "predicate":      "__PATH__",
            "predicate_path": node.p,
            "object":         node.o,
            "graph":          graph_term,
        })
        return patterns

    if node_name == "Graph":
        inner_graph_term = getattr(node, "term", None)
        if hasattr(node, "p"):
            extract_all_patterns(node.p, patterns, graph_term=inner_graph_term)
        return patterns

    for attr in ["p", "p1", "p2", "args", "expr",
                 "BGP", "Join", "LeftJoin", "Union", "Project", "Filter"]:
        if hasattr(node, attr):
            child = getattr(node, attr)
            if isinstance(child, list):
                for c in child:
                    extract_all_patterns(c, patterns, graph_term)
            elif child is not None:
                extract_all_patterns(child, patterns, graph_term)

    return patterns


def transform(query: str):
    """Turn a query into the pattern dicts the harvester works from.

    Parses the query, collects its patterns, encodes every position
    canonically, and collapses duplicates -- so a pattern repeated
    across two branches of a UNION is requested only once.

    Args:
        query: The SPARQL query.

    Returns:
        A list of pattern dicts. Empty if the query could not be parsed;
        the error is printed rather than raised.

    Note:
        A pattern's graph term is part of its identity, so the same
        triple pattern under two different GRAPH clauses stays two
        patterns. So is the serialised path expression, so that two
        distinct paths sharing a subject and an object are not collapsed
        into one by the sentinel they share.
    """
    with _parse_lock:
        try:
            parsed = parseQuery(query)
            algebra = translateQuery(parsed).algebra
            raw_patterns = extract_all_patterns(algebra)
        except Exception as e:
            print(f"SPARQL parse error: {e}")
            raw_patterns = []

    # B.2.1: term_to_pattern_str replaces the previous str(term), which
    # discarded the quoting, datatype and language tag of a literal and so
    # produced an encoding incompatible with the one used at bind-join
    # time. Blank nodes and non-http IRI schemes are also handled.
    def term_to_str(term):
        """Encode one term, falling back to the path sentinel."""
        if term is None:
            return None
        if isinstance(term, (URIRef, Literal, Variable, BNode, str)):
            return term_to_pattern_str(term)
        return _PATH_SENTINEL

    seen = set()
    bgp = []

    for pat in raw_patterns:
        entry = {
            "subject":   term_to_str(pat.get("subject")),
            "predicate": term_to_str(pat.get("predicate")),
            "object":    term_to_str(pat.get("object")),
            "graph":     term_to_str(pat.get("graph")),
        }

        if "predicate_path" in pat:
            entry["predicate_path"] = pat["predicate_path"]

        # B.2.3: every path pattern carries the same "__PATH__" sentinel in
        # its predicate position, so a key built from the predicate alone
        # collapsed two structurally distinct paths sharing a subject and
        # an object (e.g. ?a ex:p1* ?b and ?a ex:p2+ ?b) into one entry and
        # silently dropped the second. The serialized path expression is
        # therefore part of the key.
        if "predicate_path" in entry:
            predicate_key = f"{_PATH_SENTINEL}:{path_to_str(entry['predicate_path'])}"
        else:
            predicate_key = entry["predicate"]

        key = (entry["subject"], predicate_key, entry["object"], entry["graph"])
        if key not in seen:
            seen.add(key)
            bgp.append(entry)

    print("DEBUG: extracted quad patterns:", bgp)
    return bgp

def path_to_str(path) -> str:
    """Serialise a property path expression to a string.

    Args:
        path: An rdflib path expression, or a plain IRI.

    Returns:
        A readable form: "(p)*" for a modifier, "a/b" for a sequence,
        "a|b" for an alternative, "^p" for an inverse.
    """
    if isinstance(path, URIRef):
        return str(path)
    if isinstance(path, MulPath):
        base = path_to_str(path.path)
        return f"({base}){path.mod}"
    if isinstance(path, SequencePath):
        return "/".join(path_to_str(a) for a in path.args)
    if isinstance(path, AlternativePath):
        return "|".join(path_to_str(a) for a in path.args)
    if isinstance(path, InvPath):
        return f"^{path_to_str(path.arg)}"
    return repr(path)


def is_path_pattern(pat: dict) -> bool:
    """Return True if a pattern's predicate is a property path expression."""
    return pat.get("predicate") == "__PATH__"


def synthetic_path_iri(path_obj) -> URIRef:
    """Build the reserved IRI a locally-evaluated path stores results under.

    Evaluating a property path produces pairs of resources, and those
    pairs have to stay available to the patterns that join with the
    path. Each is materialised as a triple using this predicate, which
    makes the result reachable through ordinary SPARQL while the
    reserved namespace keeps it from being mistaken for a predicate any
    repository actually asserted.

    Args:
        path_obj: The path expression.

    Returns:
        The IRI. The serialised expression is percent-encoded, since "^"
        and "|" are not legal in an IRI, and the encoding stays
        one-to-one so two distinct paths still get two distinct
        predicates.

    Note:
        Both the writer and the reader of these triples call this
        function, so the two cannot drift apart.
    """
    return URIRef("urn:tpf:path:" + quote(path_to_str(path_obj), safe=""))


def extract_base_iris_from_path(path) -> list:
    """Collect the concrete predicates a path expression references.

    Args:
        path: The path expression.

    Returns:
        The IRIs, in the order met, repeats included. Empty for a
        negated property set, which names the predicates that must *not*
        occur and so cannot be enumerated from a fragments interface at
        all.
    """
    if isinstance(path, URIRef):
        return [path]
    if isinstance(path, (MulPath, InvPath)):
        inner = path.path if isinstance(path, MulPath) else path.arg
        return extract_base_iris_from_path(inner)
    if isinstance(path, (SequencePath, AlternativePath)):
        iris = []
        for arg in path.args:
            iris.extend(extract_base_iris_from_path(arg))
        return iris
    if isinstance(path, NegatedPath):
        # A negated property set names the predicates that must NOT occur;
        # its extension cannot be enumerated from a fragment interface, so
        # no support pattern can be derived for it.
        return []
    return []


def path_support_patterns(pat, path_index):
    """Derive the patterns that must be harvested before a path can run.

    A property path cannot be requested from a fragments server: a
    server will return the triples for a given predicate, but not the
    pairs of resources joined by an arbitrary-length chain of them.
    SCARAB therefore evaluates paths locally, over triples already in
    the store, which means the base predicates have to be fetched first.

    Args:
        pat: The path pattern.
        path_index: Its position in the query, used to name variables.

    Returns:
        One unconstrained pattern per distinct base predicate, using
        freshly-named variables so a derived pattern cannot join with
        anything in the original query by accident. Each carries
        "derived_from_path", so provenance can tell it apart from a
        pattern of the query proper.

    Note:
        These patterns are unconstrained deliberately. A transitive path
        may travel through intermediate resources that nothing in the
        query constrains, so retrieving the full extent of the base
        predicates is what makes the local evaluation complete with
        respect to what was harvested. It can also be expensive -- see
        SCARAB_HARVEST_PATH_BASE_PREDICATES.
    """
    derived = []
    seen = set()

    for n, iri in enumerate(extract_base_iris_from_path(pat.get("predicate_path"))):
        iri_str = str(iri)
        if iri_str in seen:
            continue
        seen.add(iri_str)

        derived.append({
            "subject":   f"?__path{path_index}_{n}_s",
            "predicate": iri_str,
            "object":    f"?__path{path_index}_{n}_o",
            "graph":     pat.get("graph"),
            "derived_from_path": path_to_str(pat["predicate_path"]),
        })

    return derived


def augment_bgp_with_path_support(bgp):
    """Add the support patterns every property path in a query needs.

    Args:
        bgp: The query's pattern dicts.

    Returns:
        The list, extended. A support pattern is left out where the
        query already requests the same predicate unconstrained, so
        nothing is fetched twice.

    Note:
        Support patterns are ordinary patterns and are scheduled by the
        ordinary rule, which puts them before the paths -- paths are
        always scheduled last.

        Returns `bgp` untouched when SCARAB_HARVEST_PATH_BASE_PREDICATES
        is disabled, warning first if the query contains paths, since
        the failure mode there is a path quietly producing nothing.
    """
    if not HARVEST_PATH_BASE_PREDICATES:
        # Silent under-retrieval is the failure mode this toggle
        # introduces, so it is announced rather than left to be inferred
        # from an empty result. With support harvesting disabled, a path
        # is evaluated only over whatever the rest of the query happened
        # to materialize; a path over a predicate that appears nowhere
        # else in the query therefore yields no bindings at all.
        path_count = sum(1 for p in bgp if is_path_pattern(p))
        if path_count:
            print(
                f"  [Path] WARNING: {path_count} property path pattern(s) "
                "present but SCARAB_HARVEST_PATH_BASE_PREDICATES is "
                "disabled. Base predicates will not be retrieved, and any "
                "path over a predicate not otherwise requested by the "
                "query will produce no bindings."
            )
        return bgp

    existing_predicates = {
        p["predicate"] for p in bgp if not is_path_pattern(p)
    }

    augmented = list(bgp)

    for k, pat in enumerate(bgp):
        if not is_path_pattern(pat):
            continue
        for support in path_support_patterns(pat, k):
            if support["predicate"] in existing_predicates:
                print(f"  [Path] base predicate {support['predicate']} "
                      "already requested by the query — no support pattern added")
                continue
            existing_predicates.add(support["predicate"])
            augmented.append(support)
            print(f"  [Path] added support pattern for base predicate "
                  f"{support['predicate']}")

    return augmented


# -------------------------------------------------------------------------
# PATTERN HELPERS
# -------------------------------------------------------------------------

def extract_vars_from_pattern(pat):
    """List the variable names a pattern uses, without their "?".

    Args:
        pat: A pattern dict.

    Returns:
        The names, in subject, predicate, object, graph order.
    """
    vars_ = []
    for field in ("subject", "predicate", "object", "graph"):
        val = pat.get(field)
        if val and val != "__PATH__" and val.startswith("?"):
            vars_.append(val[1:])
    return vars_

def shares_variable(pat, processed_patterns):
    """Return True if a pattern shares a variable with any of the others.

    Args:
        pat: The pattern to test.
        processed_patterns: The patterns to test it against.
    """
    vars = set(extract_vars_from_pattern(pat))
    for p in processed_patterns:
        if vars.intersection(extract_vars_from_pattern(p)):
            return True
    return False


def evaluate_path_locally(pat: dict, named_graph: str) -> list[dict]:
    """Evaluate a property path over what is already in the local store.

    The triples for the path's base predicates are pulled out of the
    store into an in-memory graph, and the expression is evaluated
    against that: outwards from a fixed subject, backwards towards a
    fixed object, or over every reachable pair when both ends are
    variables.

    Args:
        pat: The path pattern.
        named_graph: Graph to draw triples from, matched by prefix.

    Returns:
        One dict of variable name -> rdflib term per distinct pair
        found. Empty if the path references no concrete predicate, or if
        nothing has been materialised for it.

    Note:
        Because the closure is computed over materialised triples only,
        an empty local graph always means no bindings. A warning is
        printed in that case, since "the base predicates were never
        retrieved" and "the repository holds no such triples" are
        otherwise indistinguishable from the output.
    """
    path_obj  = pat["predicate_path"]
    subj_term = pat["subject"]
    obj_term  = pat["object"]

    subj_is_var = subj_term.startswith("?")
    obj_is_var  = obj_term.startswith("?")

    base_iris = extract_base_iris_from_path(path_obj)
    if not base_iris:
        return []

    values_clause = ", ".join(f"<{iri}>" for iri in base_iris)
    graph_filter  = (
        f'FILTER(STRSTARTS(STR(?g), "{named_graph}"))' if named_graph else ""
    )

    pull_query = f"""
    SELECT ?s ?p ?o WHERE {{
      GRAPH ?g {{ ?s ?p ?o . }}
      FILTER(?p IN ({values_clause}))
      {graph_filter}
    }}
    """
    # B.2.4: typed bindings, so that a term's node kind comes from the
    # store rather than from testing the string for an "http" prefix,
    # which misclassified every urn:, doi: and ark: IRI as a literal.
    rows = execute_sparql_query(pull_query, include_types=True) or []

    local_g = Graph()
    for row in rows:
        try:
            s_node = term_from_sparql_json(row["s"])
            p_node = term_from_sparql_json(row["p"])
            o_node = term_from_sparql_json(row["o"])
            local_g.add((s_node, p_node, o_node))
        except Exception:
            continue

    print(f"  [Path eval] local graph has {len(local_g)} triples "
          f"for path {path_to_str(path_obj)}")

    if len(local_g) == 0:
        # The closure is computed over materialized triples only, so an
        # empty local graph always means zero bindings. Distinguishing
        # "the base predicates were never retrieved" from "the repository
        # holds no such triples" is otherwise impossible from the output.
        print(
            "  [Path eval] WARNING: no triples materialized for the base "
            f"predicates {[str(i) for i in base_iris]}. "
            + ("The path will produce no bindings; check that the source "
               "holds these predicates."
               if HARVEST_PATH_BASE_PREDICATES else
               "SCARAB_HARVEST_PATH_BASE_PREDICATES is disabled, so they "
               "were never requested.")
        )

    if not subj_is_var and obj_is_var:
        # B.2.4: parse_pattern_term honours every IRI scheme, and returns
        # a properly typed literal when the anchor is one.
        anchor = parse_pattern_term(subj_term)
        pairs  = [(anchor, o) for o in local_g.objects(anchor, path_obj)]
    elif subj_is_var and not obj_is_var:
        anchor = parse_pattern_term(obj_term)
        pairs  = [(s, anchor) for s in local_g.subjects(path_obj, anchor)]
    else:
        pairs = list(local_g.subject_objects(path_obj))

    bindings = []
    for s_res, o_res in pairs:
        sol = {}
        if subj_is_var:
            sol[subj_term[1:]] = s_res
        if obj_is_var:
            sol[obj_term[1:]] = o_res
        if sol:
            bindings.append(sol)

    seen: set = set()
    unique = []
    for b in bindings:
        key = tuple(sorted((k, str(v)) for k, v in b.items()))
        if key not in seen:
            seen.add(key)
            unique.append(b)

    print(f"  [Path eval] produced {len(unique)} bindings")
    return unique

# -------------------------------------------------------------------------
# TPF URL BUILDER
# -------------------------------------------------------------------------

def triple_matches_request(s, p, o, req_s, req_p, req_o):
    """Return True when a retrieved triple satisfies the pattern requested.

    A fragments response carries the fragment's own control metadata
    alongside its data, so a page is filtered against the request before
    anything is kept.

    Args:
        s: Subject of the retrieved triple.
        p: Predicate of the retrieved triple.
        o: Object of the retrieved triple.
        req_s: Encoded subject position requested.
        req_p: Encoded predicate position requested.
        req_o: Encoded object position requested.

    Returns:
        True if the triple matches.
    """
    return (
        pattern_term_matches(req_s, s) and
        pattern_term_matches(req_p, p) and
        pattern_term_matches(req_o, o)
    )


def tpf_uri_request_builder(control_uri, subject, predicate, object_, graph=None):
    """Build the request URL for one pattern.

    Variables and unset positions are left out, so an all-variable
    pattern asks for the whole dataset and a fully-bound one asks
    whether a single triple is present.

    Args:
        control_uri: Base URL of the fragments server.
        subject: Encoded subject position.
        predicate: Encoded predicate position.
        object_: Encoded object position.
        graph: Encoded graph position. Including it makes this a Quad
            Pattern Fragments request; leaving it out keeps the request
            indistinguishable from an ordinary triple-based one, so a
            server implementing only the latter is unaffected.

    Returns:
        The URL.
    """
    params = {}

    if subject is not None and not subject.startswith("?"):
        params["subject"] = subject

    if predicate is not None and not predicate.startswith("?"):
        params["predicate"] = predicate

    if object_ is not None and not object_.startswith("?"):
        params["object"] = object_

    if graph is not None and not graph.startswith("?"):
        params["graph"] = graph

    if params:
        return f"{control_uri}?{urlencode(params)}"
    return control_uri


# -------------------------------------------------------------------------
# CARDINALITY ESTIMATION
# -------------------------------------------------------------------------

def heuristic_cardinality(html):
    """Guess a fragment's size from the response body.

    A fallback for servers whose control metadata is missing or cannot
    be read. It distinguishes only three cases: an empty fragment, one
    advertising a further page, and one that does not.

    Args:
        html: The response body.

    Returns:
        A rough estimate. These numbers are ordering hints rather than
        counts; they only have to rank patterns against one another.
    """
    if re.search(r'no\s*triples', html, re.I):
        return 0
    if 'rel="next"' in html:
        return 10000
    triple_count = len(re.findall(r'property=|typeof=', html))
    if triple_count > 0:
        return triple_count
    return 5000


def get_pattern_count(control_uri, subject, predicate, object_, graph=None):
    """Estimate how many triples a fragments server holds for a pattern.

    One request is issued and the estimate read from the fragment's
    control metadata -- hydra:totalItems or void:triples -- falling back
    to `heuristic_cardinality` where the server publishes neither.

    Args:
        control_uri: Base URL of the fragments server.
        subject: Encoded subject position.
        predicate: Encoded predicate position.
        object_: Encoded object position.
        graph: Encoded graph position, for a QPF request.

    Returns:
        The estimate. A server that cannot be reached yields a very
        large number rather than an error, which schedules its pattern
        last and lets the run go on: across independently administered
        repositories, one being unavailable should cost some
        completeness, not the whole answer.
    """
    url = tpf_uri_request_builder(control_uri, subject, predicate, object_, graph)

    try:
        response = requests.get(url)
        html = response.text
        content_type = response.headers.get("content-type", "")
    except Exception as e:
        print("Connection failed:", url, e)
        return 999_999_999

    if "html" not in content_type and "<html" not in html:
        return heuristic_cardinality(html)

    g = Graph()
    count = None

    try:
        g.parse(data=html, format="rdfa")
    except:
        return heuristic_cardinality(html)

    for pred in [HYDRA.totalItems, VOID.triples]:
        for s, p, o in g:
            if p == pred:
                raw = re.sub(r'[,±~]', '', str(o).strip())
                if raw.isdigit():
                    count = int(raw)
                    break
        if count:
            break

    return count if count else heuristic_cardinality(html)

# -------------------------------------------------------------------------
# PAGE FETCHING (WITH CACHE)
# -------------------------------------------------------------------------

FOAF_PRIMARY_TOPIC = URIRef("http://xmlns.com/foaf/0.1/primaryTopic")
METADATA_NAMESPACES = (
    "http://www.w3.org/ns/hydra/core#",
    "http://rdfs.org/ns/void#",
)

def _is_metadata_predicate(p):
    """Return True for a predicate belonging to fragment control metadata."""
    return any(str(p).startswith(ns) for ns in METADATA_NAMESPACES)


def fetch_tpf_page(url):
    """Fetch and parse one page of a fragment.

    Pages are cached by URL for the life of the process, so the same
    page is not fetched twice when two bindings of a bind join lead to
    the same request.

    TriG is preferred and Turtle accepted, but the body is parsed by
    trying the supported serialisations in turn, so a server answering
    in N-Triples, RDF/XML, JSON-LD or RDFa-annotated HTML stays usable.

    Args:
        url: The page URL.

    Returns:
        A ConjunctiveGraph holding the page: its data triples together
        with the control metadata describing the fragment. Empty if the
        page could not be fetched or parsed, the error being printed
        rather than raised.
    """
    with _cache_lock:
        if url in page_cache:
            return page_cache[url]

    headers = {"Accept": "application/trig, text/turtle;q=0.9"}
    cg = ConjunctiveGraph()

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        text = resp.text

        print(f"[FETCH] {url} → {resp.status_code}, {len(text)} bytes")

        for fmt in ["trig", "turtle", "nt", "xml", "json-ld", "rdfa"]:
            try:
                cg.parse(data=text, format=fmt)
                break
            except Exception:
                continue

    except Exception as e:
        print(f"  Fetch/parse error: {e}")

    with _cache_lock:
        page_cache[url] = cg

    return cg


def harvest_pattern_into_repo(url, named_graph,
                              subject=None, predicate=None, object_=None):
    """Follow a fragment to its end, buffering the triples that match.

    Pages are followed through their hydra:next links. Each page is
    filtered against the requested pattern before anything is buffered,
    since a response carries the fragment's own control metadata
    alongside its data.

    Args:
        url: URL of the first page.
        named_graph: Graph to write the triples into.
        subject: Encoded subject position, for filtering.
        predicate: Encoded predicate position, for filtering.
        object_: Encoded object position, for filtering.

    Note:
        The walk stops when a page contributes nothing, when a page
        holds fewer triples than the server's advertised page size
        (marking it the last), or when the next link repeats the current
        URL -- a guard against servers whose pagination fails to
        advance.

        Correctness depends on successive pages covering the fragment
        exactly once, which requires the server to paginate
        deterministically. A server backed by a store that returns
        solutions in a different order each time will silently skip some
        triples and repeat others.
    """
    print("Harvesting URL:", url)

    current_url = url
    page_count = 0

    while current_url:
        page_count += 1
        print(f" Page {page_count}: {current_url}")

        cg = fetch_tpf_page(current_url)

        next_url = None
        items_per_page = None

        for s, p, o in cg:
            if p == HYDRA.next or p == HYDRA.nextPage:
                next_url = str(o)
            if p == HYDRA.itemsPerPage:
                try:
                    items_per_page = int(str(o))
                except ValueError:
                    pass

        data_triples = 0

        for s, p, o, ctx in cg.quads((None, None, None, None)):
            if triple_matches_request(s, p, o, subject, predicate, object_):
                add_to_buffer((s, p, o), named_graph)
                data_triples += 1

        print(f"  Buffered {data_triples} matching triples")

        if data_triples == 0:
            print("  → No matching triples → end")
            break

        if items_per_page is not None and data_triples < items_per_page:
            print(f"  → Partial page → last page")
            break

        if next_url == current_url:
            print("  WARNING: next URL equals current URL, stopping.")
            break

        current_url = next_url


# -------------------------------------------------------------------------
# DATASOURCE ABSTRACTION (TPF/QPF, SPARQL endpoint, RDF dump)
# -------------------------------------------------------------------------
# Retrieval was previously bound to the fragment protocol at three points:
# the encoding of a pattern as a fragment selector, the derivation of a
# cardinality estimate from hydra/void control metadata, and the traversal
# of a fragment through its pagination links. Each of those is now a method
# on a DataSource, and the harvesting algorithm above them is unchanged:
# the same cardinality-and-connectivity ordering, the same bind join, the
# same ingestion queue and the same nanopublication wrapper apply to all
# three source types.
#
# A source therefore only has to answer two questions:
#
#   count(pat)                  -> an estimate of |F(t, E)|
#   harvest(pat, named_graph)   -> buffer every triple of F(t, E)
#
# Note that the fragment interface remains the only one that constrains
# what a source must expose. A SPARQL endpoint is asked only for single
# triple patterns, never for the query as a whole, so the federation
# semantics (partial contributions retained, evaluation deferred to the
# local store) are identical whichever source type is used.


class DataSource:
    """What SCARAB needs from a repository, and nothing more.

    A source has to answer two questions: how many triples it holds for
    a given pattern, and which triples those are. Everything above this
    -- the scheduling, the bind join, the ingestion queue, the
    nanopublication wrapper -- is identical whichever kind of repository
    lies behind it.

    The interface is this narrow because the fragments interface is the
    most restrictive of the three supported, and writing to it means the
    other two need no special handling: a SPARQL endpoint answers a
    pattern request with a SELECT over that one pattern, and a dump
    answers it by scanning the parsed graph.

    Attributes:
        kind: Short name of the source type, used in labels and
            provenance.
        location: Where the repository lives.
    """

    kind = "abstract"

    def __init__(self, location):
        """Bind to one location.

        Args:
            location: URL or file path of the repository.
        """
        self.location = location

    # -- provenance -------------------------------------------------------
    def identity_uri(self):
        """Return the IRI denoting this source in the provenance graph."""
        return URIRef(self.location)

    def provenance_type(self):
        """Return the type and access property describing this source.

        Returns:
            An (rdf:type, access property) pair.
        """
        return DCAT.DataService, DCAT.endpointURL

    def label(self):
        """Return a short human-readable description of the source."""
        return f"{self.kind}: {self.location}"

    # -- retrieval --------------------------------------------------------
    def count(self, pat):
        """Estimate how many triples this source holds for a pattern.

        Args:
            pat: The pattern dict.

        Returns:
            The estimate, used only to rank patterns against one
            another.

        Raises:
            NotImplementedError: Always. Subclasses provide this.
        """
        raise NotImplementedError

    def harvest(self, pat, named_graph):
        """Buffer every triple this source holds for a pattern.

        Args:
            pat: The pattern dict.
            named_graph: Graph to write the triples into.

        Raises:
            NotImplementedError: Always. Subclasses provide this.
        """
        raise NotImplementedError


class TPFDataSource(DataSource):
    """A Triple or Quad Pattern Fragments server.

    The interface the rest of the design is written against. A pattern
    becomes a fragment selector, the size estimate comes from the
    fragment's control metadata, and the fragment is walked page by
    page.

    A fragments server is cheap to put in front of an existing
    repository and exposes far less than a SPARQL endpoint does, which
    matters wherever a data custodian is willing to publish the shape of
    their data but not an unrestricted query interface.
    """

    kind = "tpf"

    def count(self, pat):
        """Read the fragment's size estimate from its control metadata."""
        return get_pattern_count(
            self.location,
            pat["subject"],
            pat["predicate"],
            pat["object"],
            pat.get("graph"),
        )

    def harvest(self, pat, named_graph):
        """Walk the fragment for a pattern, buffering what matches."""
        url = tpf_uri_request_builder(
            self.location,
            pat["subject"],
            pat["predicate"],
            pat["object"],
            pat.get("graph"),
        )
        harvest_pattern_into_repo(
            url,
            named_graph,
            pat["subject"],
            pat["predicate"],
            pat["object"],
        )


class SPARQLDataSource(DataSource):
    """A remote SPARQL endpoint, asked one pattern at a time.

    The endpoint is never handed the query. It is asked for one triple
    pattern at a time, exactly as a fragments server would be, which
    keeps retrieval separate from evaluation whichever kind of source is
    involved and means an endpoint able to answer only part of a query
    still contributes everything it holds for the rest.

    Graphs are handled as SPHINX handles them: a pattern carrying a
    concrete graph IRI is scoped with GRAPH, and one carrying none is
    matched against the default graph or any named graph, so content is
    found wherever the repository chose to put it.
    """

    kind = "sparql"

    def _where_clause(self, pat):
        """Build the WHERE clause matching one pattern, correctly scoped.

        Args:
            pat: The pattern dict.

        Returns:
            The clause.
        """
        s = sparql_term(pat["subject"])
        p = sparql_term(pat["predicate"])
        o = sparql_term(pat["object"])
        graph = pat.get("graph")

        core = f"{s} {p} {o} ."

        if graph is not None and not graph.startswith("?"):
            return f"GRAPH {sparql_term(graph)} {{ {core} }}"

        return f"{{ {core} }} UNION {{ GRAPH ?__g {{ {core} }} }}"

    def _projection(self, pat):
        """Return the pattern's three positions, keyed by role.

        Args:
            pat: The pattern dict.

        Returns:
            A dict of "subject", "predicate" and "object".

        Note:
            Not used by `harvest`, which builds its projection inline.
        """
        return {
            "subject":   pat["subject"],
            "predicate": pat["predicate"],
            "object":    pat["object"],
        }

    def count(self, pat):
        """Count the triples the endpoint holds for a pattern.

        Args:
            pat: The pattern dict.

        Returns:
            The count, or a very large number where the endpoint could
            not be reached or answered unusably -- which schedules the
            pattern last and lets the run go on, exactly as an
            unreachable fragments server is treated.
        """
        query = f"""
SELECT (COUNT(*) AS ?__count) WHERE {{
  {self._where_clause(pat)}
}}
"""
        rows = execute_sparql_query(query, endpoint=self.location)
        if not rows:
            # Unreachable or failing endpoint: schedule last, exactly as
            # an uncontactable TPF server is treated.
            return 999_999_999
        try:
            return int(rows[0].get("__count", 0))
        except (TypeError, ValueError):
            return 999_999_999

    def harvest(self, pat, named_graph):
        """Select the triples matching a pattern and buffer them.

        Args:
            pat: The pattern dict.
            named_graph: Graph to write the triples into.

        Note:
            Blank nodes are skipped. One retrieved from a remote source
            has no identity outside the document that produced it, so it
            could not be joined against anything afterwards.
        """
        s_var = pat["subject"].startswith("?")
        p_var = pat["predicate"].startswith("?")
        o_var = pat["object"].startswith("?")

        select_vars = []
        if s_var:
            select_vars.append(pat["subject"])
        if p_var:
            select_vars.append(pat["predicate"])
        if o_var:
            select_vars.append(pat["object"])

        projection = " ".join(dict.fromkeys(select_vars)) if select_vars else "*"

        query = f"""
SELECT DISTINCT {projection} WHERE {{
  {self._where_clause(pat)}
}}
"""
        print(f"Harvesting SPARQL source: {self.location}")
        rows = execute_sparql_query(
            query, include_types=True, endpoint=self.location
        )

        if not rows:
            print("  → No rows returned → end")
            return

        fixed_s = parse_pattern_term(pat["subject"])
        fixed_p = parse_pattern_term(pat["predicate"])
        fixed_o = parse_pattern_term(pat["object"])

        buffered = 0
        for row in rows:
            try:
                s = (term_from_sparql_json(row[pat["subject"][1:]])
                     if s_var else fixed_s)
                p = (term_from_sparql_json(row[pat["predicate"][1:]])
                     if p_var else fixed_p)
                o = (term_from_sparql_json(row[pat["object"][1:]])
                     if o_var else fixed_o)
            except KeyError:
                continue

            if s is None or p is None or o is None:
                continue

            # A blank node retrieved from a remote source has no identity
            # outside the document that produced it, so it is skipped for
            # the same reason SPHINX excludes blank-node classes.
            if isinstance(s, BNode) or isinstance(o, BNode):
                continue

            add_to_buffer((s, p, o), named_graph)
            buffered += 1

        print(f"  Buffered {buffered} matching triples")


class DumpDataSource(DataSource):
    """A local RDF file.

    Parsed once, on first use, into an in-memory ConjunctiveGraph, so a
    quad-based serialisation keeps its named graphs and can be addressed
    by a pattern carrying a graph term, exactly as the QPF interface
    allows.

    Counting a pattern scans the whole graph, so a large file is better
    loaded into a triplestore and declared as a SPARQL source.
    """

    kind = "dump"

    def __init__(self, location, rdf_format=None):
        """Bind to one file, without reading it yet.

        Args:
            location: Path or URL of the RDF file.
            rdf_format: Serialisation name for rdflib. Guessed if
                omitted.
        """
        super().__init__(location)
        self.rdf_format = rdf_format
        self._graph = None
        self._load_lock = threading.Lock()

    def identity_uri(self):
        """Return the file's IRI, for provenance.

        Returns:
            An absolute file:// URI where the location resolves to a
            path on disk, and the location unchanged where it does not.
        """
        try:
            return URIRef(_FsPath(self.location).resolve().as_uri())
        except Exception:
            # Already a URL, or a path that cannot be resolved
            return URIRef(str(self.location))

    def provenance_type(self):
        # A dump is a distribution, not a service.
        """Describe a dump as a dataset rather than as a service."""
        return DCAT.Dataset, DCAT.downloadURL

    def _ensure_loaded(self):
        """Parse the file, once.

        Returns:
            The parsed graph. A file that cannot be parsed yields an
            empty graph and a printed error, so one bad dump does not
            cost the other sources of the run.
        """
        with self._load_lock:
            if self._graph is not None:
                return self._graph
            cg = ConjunctiveGraph()
            try:
                if self.rdf_format:
                    cg.parse(self.location, format=self.rdf_format)
                else:
                    cg.parse(self.location)
                print(f"[DUMP] Loaded {len(cg)} triples from {self.location}")
            except Exception as e:
                print(f"[DUMP] Parse error for {self.location}: {e}")
            self._graph = cg
            return self._graph

    def _matching(self, pat):
        """Yield the triples matching a pattern.

        Args:
            pat: The pattern dict. A concrete graph term restricts the
                scan to that named graph.

        Yields:
            Matching (subject, predicate, object) triples.
        """
        cg = self._ensure_loaded()

        graph = pat.get("graph")
        graph_term = None
        if graph is not None and not graph.startswith("?"):
            graph_term = parse_pattern_term(graph)

        for s, p, o, ctx in cg.quads((None, None, None, None)):
            if graph_term is not None and ctx.identifier != graph_term:
                continue
            if not triple_matches_request(
                s, p, o, pat["subject"], pat["predicate"], pat["object"]
            ):
                continue
            yield s, p, o

    def count(self, pat):
        """Count the matching triples, by scanning the whole graph."""
        return sum(1 for _ in self._matching(pat))

    def harvest(self, pat, named_graph):
        """Buffer every triple matching a pattern.

        Args:
            pat: The pattern dict.
            named_graph: Graph to write the triples into.

        Note:
            Blank nodes are skipped, as for every other source type.
        """
        print(f"Harvesting dump source: {self.location}")
        buffered = 0
        for s, p, o in self._matching(pat):
            if isinstance(s, BNode) or isinstance(o, BNode):
                continue
            add_to_buffer((s, p, o), named_graph)
            buffered += 1
        print(f"  Buffered {buffered} matching triples")


# Extensions recognised as RDF dumps when a source is given as a bare string.
_DUMP_EXTENSIONS = (
    ".ttl", ".turtle", ".nt", ".ntriples", ".nq", ".nquads",
    ".trig", ".rdf", ".owl", ".n3", ".jsonld", ".json-ld", ".xml",
)


def make_datasource(spec):
    """Build a DataSource from a caller's description of a repository.

    Args:
        spec: One of

            - a DataSource, returned unchanged
            - {"type": "tpf"|"sparql"|"dump", "location": ...,
              "format": ...}
            - "tpf:<url>" or "qpf:<url>", a fragments server
            - "sparql:<url>", a SPARQL endpoint
            - "dump:<path>", an RDF file
            - a bare path or URL, inferred

    Returns:
        The DataSource.

    Raises:
        ValueError: If a dict names an unknown type, or if `spec` is
            neither a string nor a dict.

    Note:
        Inference is deliberately cautious. Anything with a recognised
        RDF extension, a file:// URL, or a path that exists on disk is
        taken for a dump; anything else http(s) for a fragments server.
        A SPARQL endpoint therefore has to declare itself, since it
        cannot be told from a fragments server by its URL alone and
        mistaking one for the other would produce empty fragments rather
        than an error.
    """
    if isinstance(spec, DataSource):
        return spec

    if isinstance(spec, dict):
        kind = (spec.get("type") or "tpf").lower()
        location = spec.get("location") or spec.get("url") or spec.get("path")
        if kind == "sparql":
            return SPARQLDataSource(location)
        if kind == "dump":
            return DumpDataSource(location, spec.get("format"))
        if kind in ("tpf", "qpf"):
            return TPFDataSource(location)
        raise ValueError(f"Unknown datasource type: {kind}")

    if not isinstance(spec, str):
        raise ValueError(f"Cannot build a datasource from {spec!r}")

    lowered = spec.lower()

    for prefix, cls in (("tpf:", TPFDataSource),
                        ("qpf:", TPFDataSource),
                        ("sparql:", SPARQLDataSource),
                        ("dump:", DumpDataSource)):
        # Guard against a bare "http://..." being read as scheme "http:"
        if lowered.startswith(prefix):
            return cls(spec[len(prefix):])

    if lowered.startswith("file://"):
        return DumpDataSource(spec)

    if any(lowered.endswith(ext) for ext in _DUMP_EXTENSIONS):
        return DumpDataSource(spec)

    if not lowered.startswith(("http://", "https://")) and os.path.exists(spec):
        return DumpDataSource(spec)

    return TPFDataSource(spec)


# -------------------------------------------------------------------------
# VECTORISED BIND JOIN
# -------------------------------------------------------------------------

def concretize_pattern(pat, binding):
    """Substitute a binding into a pattern.

    Args:
        pat: The pattern dict.
        binding: Variable name -> rdflib term.

    Returns:
        A new pattern dict with the bound variables replaced. Variables
        the binding says nothing about are left alone.

    Note:
        Every position goes through the canonical encoder, so a bound
        literal is rendered identically in the request sent to the
        source and in the filter applied to its response. That they are
        the same string is what makes the round trip work.
    """
    def concretize(term):
        """Replace one position if the binding covers it."""
        if term is None or not term.startswith("?"):
            return term
        var_name = term[1:]
        if var_name not in binding:
            return term
        return term_to_pattern_str(binding[var_name])

    bound = dict(pat)
    bound["subject"]   = concretize(pat["subject"])
    bound["predicate"] = concretize(pat["predicate"])
    bound["object"]    = concretize(pat["object"])
    bound["graph"]     = concretize(pat.get("graph"))
    return bound


def fetch_binding(binding, pat, source, named_graph):
    """Harvest one pattern under one binding.

    Args:
        binding: Variable name -> rdflib term.
        pat: The pattern dict.
        source: The DataSource to ask.
        named_graph: Graph to write the triples into.
    """
    source.harvest(concretize_pattern(pat, binding), named_graph)


def fetch_binding_batch(batch, pat, source, named_graph):
    """Harvest one pattern under a batch of bindings, concurrently.

    Requests run on a bounded pool sized by SCARAB_MAX_THREADS. The
    batch size is a multiple of the pool size, so the pool stays busy
    for the whole batch.

    Args:
        batch: The bindings.
        pat: The pattern dict.
        source: The DataSource to ask.
        named_graph: Graph to write the triples into.

    Note:
        Blocks until every request in the batch has finished. An
        exception raised by any of them propagates.
    """
    with ThreadPoolExecutor(MAX_THREADS) as pool:
        futures = []
        for binding in batch:
            futures.append(
                pool.submit(fetch_binding, binding, pat, source, named_graph)
            )
        for f in futures:
            f.result()


# -------------------------------------------------------------------------
# BINDING EXTRACTION
# -------------------------------------------------------------------------

def term_matches(pattern_term, triple_term):
    # B.2.1/B.2.4: shares the canonical comparison used by the fragment
    # filter, so the in-memory binding extractor cannot disagree with the
    # store-backed one about what a literal or a non-http IRI looks like.
    """Return True when a term satisfies a pattern position.

    Shares the canonical comparison the fragment filter uses, so the
    in-memory binding extractor cannot disagree with the store-backed
    one about what a literal or a non-http IRI looks like.
    """
    return pattern_term_matches(pattern_term, triple_term)

def triple_matches_pattern(triple, pat):
    """Return True when a triple satisfies every position of a pattern.

    Args:
        triple: The (subject, predicate, object) terms.
        pat: The pattern dict.
    """
    s, p, o = triple
    if not term_matches(pat["subject"], s):
        return False
    if not term_matches(pat["predicate"], p):
        return False
    if not term_matches(pat["object"], o):
        return False
    return True

def extract_upstream_bindings(repo, current_idx, harvested, bgp):
    """Read join bindings out of an in-memory collection of triples.

    Args:
        repo: The triples to read from.
        current_idx: Index of the pattern about to be harvested.
        harvested: Indices of the patterns harvested already.
        bgp: All of the query's pattern dicts.

    Returns:
        One dict of variable name -> rdflib term per distinct binding.
        Empty when nothing has been harvested yet, or when the pattern
        shares no variable with what has.

    Note:
        The harvesting path uses `extract_upstream_bindings_graphdb`
        instead, which asks the local store the same question. This
        variant is kept for callers holding their triples in memory.
    """
    if not harvested:
        return []

    current_pat = bgp[current_idx]
    current_vars = set(extract_vars_from_pattern(current_pat))

    upstream_vars = set()
    prev_patterns = [bgp[i] for i in harvested]

    for p in prev_patterns:
        upstream_vars.update(extract_vars_from_pattern(p))

    join_vars = current_vars.intersection(upstream_vars)

    print("\n--- Binding Extraction ---")
    print("Current pattern:", current_pat)
    print("Join variables:", join_vars)
    print("Repo size:", len(repo))

    if not join_vars:
        return []

    bindings = []

    for triple in repo:
        s, p, o = triple
        for pat in prev_patterns:
            if not triple_matches_pattern(triple, pat):
                continue
            sol = {}
            if pat["subject"].startswith("?"):
                var = pat["subject"][1:]
                if var in join_vars:
                    sol[var] = s
            if pat["predicate"].startswith("?"):
                var = pat["predicate"][1:]
                if var in join_vars:
                    sol[var] = p
            if pat["object"].startswith("?"):
                var = pat["object"][1:]
                if var in join_vars:
                    sol[var] = o
            if sol:
                bindings.append(sol)

    unique = []
    seen = set()
    for b in bindings:
        key = tuple(sorted((k, str(v)) for k, v in b.items()))
        if key not in seen:
            seen.add(key)
            unique.append(b)

    print("Bindings extracted:", len(unique))
    if unique:
        print("Sample bindings:", unique[:5])
    print("--------------------------\n")

    return unique


def extract_upstream_bindings_graphdb(current_idx, harvested, bgp, graph_iri):
    """Read join bindings for the next pattern out of the local store.

    Before a pattern is requested, the store is asked what the
    already-harvested patterns bind its shared variables to. Those
    bindings are what turn an unconstrained request into a bind join.

    Args:
        current_idx: Index of the pattern about to be harvested.
        harvested: Indices of the patterns harvested already.
        bgp: All of the query's pattern dicts.
        graph_iri: Graph to read from, which confines the bindings to
            one source's contribution. A falsy value reads from
            everywhere.

    Returns:
        One dict of variable name -> rdflib term per row. Empty when
        nothing has been harvested yet, when the pattern shares no
        variable with what has, or when the query failed -- in which
        case the failure is printed and the pattern is then requested
        unconstrained.

    Note:
        Bindings come back typed, so a literal keeps its datatype and
        language tag and can be re-encoded correctly for the next
        request.

        A property path pattern already harvested is read through the
        synthetic predicate its results were stored under.
    """
    if not harvested:
        return []

    current_pat = bgp[current_idx]
    current_vars = set(extract_vars_from_pattern(current_pat))

    upstream_vars = set()
    prev_patterns = [bgp[i] for i in harvested]

    for p in prev_patterns:
        upstream_vars.update(extract_vars_from_pattern(p))

    join_vars = current_vars.intersection(upstream_vars)

    if not join_vars:
        return []

    # B.2.4: sparql_term angle-brackets any IRI regardless of scheme. The
    # previous `safe` quoted every term that did not begin with "http" as
    # a literal, so a urn:, doi: or ark: identifier was written into the
    # binding query as a string and could never match the IRI it denoted.
    safe = sparql_term

    query = "SELECT " + " ".join("?" + v for v in join_vars) + " WHERE {\n"

    if graph_iri:
        query += f" GRAPH <{graph_iri}> {{\n"

    for pat in prev_patterns:
        if is_path_pattern(pat):
            # B.2.2: the same percent-encoded IRI the writer used.
            synthetic_p = synthetic_path_iri(pat["predicate_path"])
            s = safe(pat["subject"])
            o = safe(pat["object"])
            query += f" {s} <{synthetic_p}> {o} .\n"
        else:
            s = safe(pat["subject"])
            p = safe(pat["predicate"])
            o = safe(pat["object"])
            query += f" {s} {p} {o} .\n"

    if graph_iri:
        query += " }\n"

    query += "}"

    print("DEBUG GraphDB binding query:")
    print(query)

    try:
        # B.2.1: typed bindings, so that a literal read back out of the
        # store retains its datatype and language tag and can be
        # re-serialized correctly when it is substituted into the next
        # request. Flattening to a bare string discarded exactly the
        # information the request encoding depends on.
        results = execute_sparql_query(query, include_types=True)
        bindings = []
        for row in results:
            sol = {}
            for v in join_vars:
                if v in row:
                    sol[v] = term_from_sparql_json(row[v])
            if sol:
                bindings.append(sol)
        print("Bindings extracted from local store:", len(bindings))
        return bindings
    except Exception as e:
        print("Local store binding extraction failed:", e)
        return []


# -------------------------------------------------------------------------
# INSERT INTO LOCAL TRIPLESTORE
# -------------------------------------------------------------------------

def build_query(statements, named_graph=None):
    """Build an INSERT DATA update for a batch of triples.

    Args:
        statements: The (subject, predicate, object) triples.
        named_graph: Graph to insert into. Omitted for the default
            graph.

    Returns:
        The update, as a string.

    Note:
        Ingestion itself goes through the queue and
        `buffer_flusher_daemon`, which posts N-Quads to the graph store
        endpoint rather than issuing updates. This is here for callers
        that want an update instead.
    """
    triples = "\n".join(
        f"{s.n3()} {p.n3()} {o.n3()} ." for s, p, o in statements
    )
    if named_graph:
        return f"""
INSERT DATA {{
 GRAPH <{named_graph}> {{
 {triples}
 }}
}}
"""
    else:
        return f"""
INSERT DATA {{
{triples}
}}
"""


def insert_triples_stream(statements, named_graph=None):
    """Post a batch of triples straight to the local store.

    A synchronous alternative to the ingestion queue, bypassing the
    buffer entirely.

    Args:
        statements: The (subject, predicate, object) triples.
        named_graph: Graph to write into. Omitted for the default graph.

    Note:
        Failures are printed, not raised. The harvesting path uses
        `add_to_buffer` instead, so this is for callers writing outside
        a run.
    """
    endpoint = STORE_STATEMENTS_URL  # B.2.8: configured, not hard-coded
    lines = []
    for s, p, o in statements:
        triple = f"{s.n3()} {p.n3()} {o.n3()} ."
        if named_graph:
            triple = f"{s.n3()} {p.n3()} {o.n3()} <{named_graph}> ."
        lines.append(triple)

    payload = "\n".join(lines)
    headers = {
        "Content-Type": "application/n-quads" if named_graph else "application/n-triples"
    }

    try:
        r = requests.post(endpoint, data=payload, headers=headers)
        if r.status_code in (200, 204):
            print(f"Stream insert OK ({len(statements)} triples)")
        else:
            print("Stream insert failed:", r.status_code, r.text)
        time.sleep(0.1)
    except Exception as e:
        print("Streaming insert error:", e)


# -------------------------------------------------------------------------
# BUFFERED INGESTION
# -------------------------------------------------------------------------

def add_to_buffer(triple, named_graph=None):
    """Queue one triple for ingestion.

    Args:
        triple: The (subject, predicate, object) terms.
        named_graph: Graph to write it into.

    Note:
        Blocks when the queue is full, which is the point: it puts
        backpressure on the harvesting threads rather than letting an
        unbounded buffer grow in memory. That matters for the
        unconstrained fragments a federated query can produce.
    """
    s, p, o = triple

    if named_graph:
        line = f"{s.n3()} {p.n3()} {o.n3()} <{named_graph}> .\n"
    else:
        line = f"{s.n3()} {p.n3()} {o.n3()} .\n"

    _ingest_queue.put(line)


def buffer_flusher_daemon():
    """Drain the ingestion queue into the local store, forever.

    Runs on a daemon thread, started once per process by
    `_ensure_flusher_running`. Triples come off the queue in batches and
    are posted as N-Quads, retrying with a growing delay when a post
    fails.

    Note:
        A batch still failing after its retries is dropped and the loss
        reported. Nothing is raised, there being no caller to raise to.

        Callers needing their triples to be visible should join the
        queue rather than sleep. The harvester does this between
        patterns, so that every triple harvested for one pattern is
        visible to the bindings extracted for the next.
    """
    endpoint = STORE_STATEMENTS_URL  # B.2.8: configured, not hard-coded
    BATCH_SIZE = 500
    MAX_RETRIES = 3

    while True:
        batch = []
        try:
            line = _ingest_queue.get(timeout=1)
            batch.append(line)

            while len(batch) < BATCH_SIZE:
                try:
                    batch.append(_ingest_queue.get_nowait())
                except queue.Empty:
                    break

            payload = "".join(batch)
            success = False

            for attempt in range(MAX_RETRIES):
                try:
                    r = requests.post(
                        endpoint,
                        data=payload,
                        headers={"Content-Type": "application/n-quads"},
                        timeout=30
                    )
                    if r.status_code in (200, 204):
                        success = True
                        break
                    else:
                        print(f"[FLUSH ERROR] attempt {attempt+1}: {r.status_code} {r.text[:200]}")
                except requests.RequestException as e:
                    print(f"[FLUSH ERROR] attempt {attempt+1}: {e}")
                time.sleep(0.5 * (attempt + 1))

            if not success:
                print(f"[DATA LOSS] Failed to insert {len(batch)} triples after {MAX_RETRIES} retries")

            for _ in batch:
                _ingest_queue.task_done()

            time.sleep(0.05)

        except queue.Empty:
            pass


# -------------------------------------------------------------------------
# NANOPUB PROVENANCE GRAPHS
# -------------------------------------------------------------------------

def write_nanopub_graphs(
    nanopub_base: str,
    source,
    bgp: list,
    started_at: str,
    ended_at: str,
):
    """Write the provenance wrapper around one source's harvest.

    A nanopublication is four named graphs sharing a base IRI: a head
    naming the other three, an assertion holding the content, a
    provenance describing how the assertion came about, and a pubinfo
    holding administrative metadata. This writes three of them. The
    assertion graph is the graph the harvested triples were already
    written into, so nothing is copied and one graph serves at once as
    the triples' location and as the nanopublication's assertion.

    The provenance records the source, the activity that read it, the
    patterns that directed the reading, and SCARAB itself at two levels:
    the codebase as a software agent, and the running instance as an
    agent attributed to it.

    Args:
        nanopub_base: Base IRI, from `mint_nanopub_uri`.
        source: The DataSource harvested. A fragments server or SPARQL
            endpoint is described as a service identified by its
            endpoint URL, a dump as a dataset identified by its download
            URL -- so the record says not only which source contributed
            what, but through what kind of interface.
        bgp: The query's pattern dicts. Support patterns generated for a
            property path are labelled as such, so provenance does not
            present them as patterns of the query.
        started_at: When the harvest began, as an xsd:dateTime string.
        ended_at: When it finished.

    Note:
        Call only once the ingestion queue has drained, so that the
        assertion graph is complete before anything claims to describe
        it. All the graphs go to the store in a single request.
    """

    source = make_datasource(source)

    this        = URIRef(nanopub_base)
    head_g      = URIRef(f"{nanopub_base}#Head")
    assertion_g = URIRef(f"{nanopub_base}#assertion")
    prov_g      = URIRef(f"{nanopub_base}#provenance")
    pubinfo_g   = URIRef(f"{nanopub_base}#pubinfo")

    activity    = URIRef(f"{nanopub_base}#harvestActivity")
    instance    = URIRef(f"{nanopub_base}#scarabInstance")
    endpoint    = source.identity_uri()
    source_type, access_property = source.provenance_type()

    # Each quad is (subject, predicate, object, graph_uri)
    quads = []

    # ── Head graph ────────────────────────────────────────────────────────
    quads += [
        (this, RDF.type,                NP.Nanopublication, head_g),
        (this, NP.hasAssertion,         assertion_g,        head_g),
        (this, NP.hasProvenance,        prov_g,             head_g),
        (this, NP.hasPublicationInfo,   pubinfo_g,          head_g),
    ]

    # ── Provenance graph ──────────────────────────────────────────────────

    # assertion graph: output dataset
    quads += [
        (assertion_g, RDF.type,              DCAT.Dataset,  prov_g),
        (assertion_g, PROV.wasGeneratedBy,   activity,      prov_g),
        (assertion_g, PROV.wasDerivedFrom,   endpoint,      prov_g),
    ]

    # source: dcat:DataService for a TPF/QPF server or SPARQL endpoint,
    # dcat:Dataset for an RDF dump, with the corresponding access property
    quads += [
        (endpoint, RDF.type,         source_type, prov_g),
        (endpoint, access_property,  endpoint,    prov_g),
    ]

    # harvest activity
    quads += [
        (activity, RDF.type,               PROV.Activity,                    prov_g),
        (activity, RDFS.label,
                   Literal(f"SCARAB {source.kind} bind-join harvest"),        prov_g),
        (activity, PROV.wasAssociatedWith,  instance,                         prov_g),
        (activity, PROV.used,              endpoint,                          prov_g),
        (activity, PROV.startedAtTime,
                   Literal(started_at, datatype=XSD.dateTime),                prov_g),
        (activity, PROV.endedAtTime,
                   Literal(ended_at,   datatype=XSD.dateTime),                prov_g),
    ]

    # triple patterns consumed during the harvest
    for i, pat in enumerate(bgp):
        pat_node = URIRef(f"{nanopub_base}#pattern{i}")
        if is_path_pattern(pat):
            comment = (f"{pat['subject']} "
                       f"{path_to_str(pat['predicate_path'])} "
                       f"{pat['object']}")
        else:
            comment = f"{pat['subject']} {pat['predicate']} {pat['object']}"

        # B.1.3: a support pattern is not part of T(Q). It is recorded,
        # because it did direct retrieval, but labelled so that the
        # provenance does not misrepresent it as a pattern of the query.
        derived = pat.get("derived_from_path")
        if derived:
            label = f"Support pattern {i} (property path base predicate)"
            comment = f"{comment}  [derived from property path: {derived}]"
        else:
            label = f"Triple pattern {i}"

        quads += [
            (activity,  PROV.used,    pat_node,                         prov_g),
            (pat_node,  RDF.type,     PROV.Entity,                      prov_g),
            (pat_node,  RDFS.label,   Literal(label),                   prov_g),
            (pat_node,  RDFS.comment, Literal(comment),                  prov_g),
        ]

    # SCARAB codebase (software agent)
    quads += [
        (SCARAB_CODEBASE_URI, RDF.type,
                              PROV.SoftwareAgent,                        prov_g),
        (SCARAB_CODEBASE_URI, RDFS.label,
                              Literal("SCARAB TPF Federated Harvester"), prov_g),
        (SCARAB_CODEBASE_URI, SCHEMA.softwareVersion,
                              Literal(SCARAB_VERSION),                   prov_g),
        (SCARAB_CODEBASE_URI, DCAT.downloadURL,
                              SCARAB_DOWNLOAD_URI,                       prov_g),
    ]

    # SCARAB running instance
    quads += [
        (instance, RDF.type,               PROV.Agent,              prov_g),
        (instance, RDFS.label,             Literal("SCARAB instance"), prov_g),
        (instance, PROV.wasAttributedTo,   SCARAB_CODEBASE_URI,     prov_g),
        (instance, SCHEMA.softwareVersion, Literal(SCARAB_VERSION),  prov_g),
    ]

    # ── Pubinfo graph ─────────────────────────────────────────────────────
    quads += [
        (this, DCT.created,
               Literal(ended_at, datatype=XSD.dateTime), pubinfo_g),
        (this, DCT.creator,        instance,             pubinfo_g),
        (this, NPX.hasNanopubType, NPX.ProvenanceRecord, pubinfo_g),
    ]

    # ── Serialise to n-quads and POST ─────────────────────────────────────
    lines = [
        f"{s.n3()} {p.n3()} {o.n3()} <{g}> ."
        for s, p, o, g in quads
    ]
    payload = "\n".join(lines)

    graphdb_endpoint = STORE_STATEMENTS_URL  # B.2.8: configured, not hard-coded
    try:
        r = requests.post(
            graphdb_endpoint,
            data=payload,
            headers={"Content-Type": "application/n-quads"},
            timeout=30,
        )
        if r.status_code in (200, 204):
            print(f"[NANOPUB] Provenance graphs written for {nanopub_base}")
        else:
            print(f"[NANOPUB] Write failed: {r.status_code} {r.text[:200]}")
    except Exception as e:
        print(f"[NANOPUB] Write error: {e}")


# -------------------------------------------------------------------------
# HARVEST EXECUTION
# -------------------------------------------------------------------------

def harvest_endpoint_optimized(source, bgp, nanopub_base):
    """Harvest every pattern of a query from one source.

    Patterns are ordered before anything is requested. The one estimated
    smallest goes first; each subsequent step prefers a pattern sharing
    a variable with those already scheduled, taking the smallest among
    them, and lifts that restriction only when nothing left is
    connected. The order therefore favours both selectivity and
    connectivity, so small fragments arrive first and the bindings they
    yield constrain what follows. Property paths are always last, since
    they are evaluated over whatever the rest of the query materialised.

    Each pattern is then requested: unconstrained if nothing binds its
    variables yet, or once per binding if something does -- a bind join,
    issued in concurrent batches. The ingestion queue is drained between
    patterns, so each pattern's triples are visible to the next one's
    bindings.

    Args:
        source: The DataSource, or anything `make_datasource` accepts.
        bgp: The query's pattern dicts.
        nanopub_base: Base IRI for this run and source. The assertion
            graph is this plus "#assertion", and is where the harvested
            triples go.

    Returns:
        None. What the call produces is the triples now in the local
        store and the provenance wrapper written around them.

    Note:
        The module-level INDEXING_MODE turns on a strict mode, in which
        a pattern sharing variables with earlier ones but for which no
        binding could be found is skipped rather than harvested in full.
        It is meant for exploratory indexing, where the cost of an
        unconstrained fragment is not worth paying. It is off for
        ordinary federated querying, and `run_query_strict` enables it
        around its own harvest.
    """

    source = make_datasource(source)

    # Derive the assertion graph IRI from the nanopub base URI
    named_graph = f"{nanopub_base}#assertion"
    started_at  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    print("Harvesting:", source.label())
    print("Assertion graph:", named_graph)

    harvested = set()
    counts = {}

    # Bug 1 fix: never call count() on a path pattern
    for i, pat in enumerate(bgp):
        if is_path_pattern(pat):
            counts[i] = 1
        else:
            counts[i] = source.count(pat)

    simple_indices = [i for i, p in enumerate(bgp) if not is_path_pattern(p)]
    path_indices   = [i for i, p in enumerate(bgp) if is_path_pattern(p)]

    if simple_indices:
        remaining = simple_indices[:]
        first = min(remaining, key=lambda i: counts[i])
        remaining.remove(first)
        execution_order = [first]

        while remaining:
            connected = [
                idx for idx in remaining
                if shares_variable(bgp[idx], [bgp[i] for i in execution_order])
            ]
            next_idx = min(connected if connected else remaining,
                           key=lambda i: counts[i])
            execution_order.append(next_idx)
            remaining.remove(next_idx)
    else:
        execution_order = []

    execution_order.extend(path_indices)

    print("Execution order:", execution_order)

    for idx in execution_order:
        pat = bgp[idx]
        print("Processing pattern", idx, pat)

        if is_path_pattern(pat):
            print(f"  [Path] Evaluating '{path_to_str(pat['predicate_path'])}' locally")
            _ingest_queue.join()
            path_bindings = evaluate_path_locally(pat, named_graph)
            print(f"  [Path] {len(path_bindings)} bindings — "
                  "storing synthetic triples for join propagation")

            s_field = pat["subject"]
            o_field = pat["object"]
            # B.2.2: percent-encoded synthetic predicate. The previous
            # concatenation produced an IRI containing '^' or '|' for an
            # inverse or alternative path; n3() then raised and the
            # exception propagated out of the harvest, losing the source.
            synthetic_p = synthetic_path_iri(pat["predicate_path"])

            for b in path_bindings:
                # Terms are used as evaluate_path_locally produced them.
                # Coercing them through URIRef(str(...)) corrupted a
                # literal-valued path end — newly reachable now that
                # alternative paths (e.g. rdfs:label|skos:prefLabel) no
                # longer abort before this point.
                if s_field.startswith("?"):
                    s_val = b[s_field[1:]]
                else:
                    s_val = parse_pattern_term(s_field)
                if o_field.startswith("?"):
                    o_val = b[o_field[1:]]
                else:
                    o_val = parse_pattern_term(o_field)

                if s_val is None or o_val is None:
                    continue

                add_to_buffer((s_val, synthetic_p, o_val), named_graph)

            harvested.add(idx)
            _ingest_queue.join()
            print("------------------------------------")
            continue

        bindings = extract_upstream_bindings_graphdb(
            idx, harvested, bgp, named_graph
        )

        if INDEXING_MODE:
            required_vars = extract_vars_from_pattern(pat)
            if not bindings and required_vars and len(harvested) > 0:
                print("Skipping pattern (strict mode, no bindings)")
                harvested.add(idx)
                continue

        if not bindings or not extract_vars_from_pattern(pat):
            print("Full pattern download")
            source.harvest(pat, named_graph)

        else:
            print("Bind join:", len(bindings), "bindings")
            for i in range(0, len(bindings), BIND_BATCH_SIZE):
                print(f"Processing binding batch {i} → {i + BIND_BATCH_SIZE}")
                batch = bindings[i:i + BIND_BATCH_SIZE]
                fetch_binding_batch(batch, pat, source, named_graph)

        _ingest_queue.join()
        print(f"[SYNC] Queue drained after pattern {idx}")
        harvested.add(idx)
        print("------------------------------------")

    # All data triples for this source are now in the local store.
    # Write the nanopub provenance wrapper (head + provenance + pubinfo).
    ended_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    write_nanopub_graphs(nanopub_base, source, bgp, started_at, ended_at)

    return None


# -------------------------------------------------------------------------
# MAIN ENTRY POINT
# -------------------------------------------------------------------------

def FindBGPPriority(query, endpoints, base_named_graph=None):
    # B.2.8: started once per process rather than once per call.
    """Harvest a query's data from every source into the local store.

    The entry point. The query is broken into patterns, support patterns
    are added for any property paths, and each source is harvested in
    turn into its own nanopublication assertion graph.

    The query itself is never rewritten. The decomposition governs
    retrieval only, and the query's semantics -- its OPTIONAL, UNION,
    MINUS and FILTER clauses, all of which harvesting ignores -- are
    applied afterwards by the local store, over everything harvested.

    Args:
        query: The SPARQL query.
        endpoints: The sources. A single source may be given on its own.
            Each may be a URL, a prefixed string, a dict or a
            DataSource; see `make_datasource`.
        base_named_graph: Identifier for this run, which the assertion
            graph IRIs are derived from. Defaults to "urn:tpf:run", so
            passing a fresh one per run is what keeps runs apart.

    Returns:
        None. Query the local store afterwards, with
        `execute_sparql_query`, to get the answers.

    Note:
        Returns early, having done nothing, if the query cannot be
        parsed or yields no patterns.

        Each source harvests into its own assertion graph, and the
        bindings constraining its requests are read back from that graph
        alone, so a source's contribution is gathered in isolation from
        the others. They are combined only at the end, when the query is
        evaluated over everything the run gathered.
    """
    _ensure_flusher_running()

    if isinstance(endpoints, (str, dict, DataSource)):
        endpoints = [endpoints]

    # Each entry may be a TPF/QPF URL, an explicitly declared SPARQL
    # endpoint or RDF dump, or an already-constructed DataSource.
    sources = [make_datasource(spec) for spec in endpoints]

    try:
        bgp = transform(query)
    except Exception as e:
        print("ERROR inside transform:", e)
        return

    if not bgp:
        print("No triple patterns extracted from query")
        return

    # B.1.3: schedule the base predicates of every property path as
    # ordinary patterns, so that the local closure computed by
    # evaluate_path_locally has triples to traverse. Done once here rather
    # than per source, so that every source is harvested for the same set
    # of patterns and the provenance records agree.
    bgp = augment_bgp_with_path_support(bgp)

    print("Query has", len(bgp), "triple patterns")

    run_id = base_named_graph or "urn:tpf:run"

    for i, source in enumerate(sources):
        # Mint one nanopub URI per (run, source) pair.
        # The assertion graph = nanopub_base + "#assertion" replaces
        # the old {base_named_graph}/endpoint{i+1} scheme.
        nanopub_base = mint_nanopub_uri(run_id, i + 1)
        harvest_endpoint_optimized(source, bgp, nanopub_base)

    print("Waiting for ingestion queue to empty...")
    _ingest_queue.join()
    print("All data flushed.")
    print("All sources processed.")


# -------------------------------------------------------------------------
# EXECUTE SPARQL QUERY ON LOCAL GRAPHDB
# -------------------------------------------------------------------------

def execute_sparql_query(query, include_types=False, endpoint=None):
    """Run a SPARQL query and return its solutions.

    Args:
        query: The query.
        include_types: How each binding comes back. False flattens it to
            its plain string value, which is enough for callers that
            only compare or print. True keeps the full SPARQL-JSON dict,
            so a caller can tell an IRI from a blank node or a literal
            without guessing from the string -- which SPHINX's TPF
            adapter needs in order not to mistake a blank-node class for
            a real one, and which the bind join needs in order to
            re-encode a literal with its datatype intact.
        endpoint: Where to send it. Defaults to the local store; passing
            one explicitly is what lets `SPARQLDataSource` reuse this
            against a remote source.

    Returns:
        A list of solutions, or None if the query failed. That is None
        rather than an empty list, so a caller distinguishing failure
        from no results should test for it.
    """
    endpoint = endpoint or STORE_QUERY_URL  # B.2.8: configured

    try:
        r = requests.post(
            endpoint,
            data=query,
            headers={
                "Content-Type": "application/sparql-query",
                "Accept": "application/sparql-results+json"
            }
        )

        if r.status_code != 200:
            print("Query failed:", r.text)
            return None

        data = r.json()
        results = []

        for row in data["results"]["bindings"]:
            parsed = {}
            for var, val in row.items():
                parsed[var] = val if include_types else val["value"]
            results.append(parsed)

        return results

    except Exception as e:
        print("SPARQL execution error:", e)
        return None


def run_query_strict(query, endpoints, base_named_graph="urn:tpf:temp",
                     strict=True):
    """Harvest a query in strict mode and hand back the triples.

    Runs a harvest under a fresh run identifier and reads back
    everything it gathered. Used by SPHINX's TPF adapter, which wants
    the triples themselves rather than an answer to the query.

    Args:
        query: The SPARQL query.
        endpoints: The sources; see `make_datasource`.
        base_named_graph: Prefix for the run identifier.
        strict: Whether to enable INDEXING_MODE for the duration of the
            harvest, so that a pattern sharing variables with those
            already harvested, but for which no binding could be found,
            is skipped rather than retrieved in full.

    Returns:
        A list of (subject, predicate, object) triples, each term a
        SPARQL-JSON dict so that its node kind survives. Empty if no
        source was given, or if nothing was harvested.

    Note:
        Strict mode is what makes this usable for indexing. SPHINX's
        exploration queries pair a bounded pattern with an unbounded
        `?subject ?predicate ?object`, so a class that turns out to have
        no instances yields no bindings for the second pattern, and
        without strict mode the whole repository would be retrieved to
        describe a class it does not hold.

        INDEXING_MODE is module-level state, set and restored around the
        harvest. The previous value is put back even if the harvest
        raises, but as the module supports one run per process anyway,
        do not rely on two harvests with different values of `strict`
        overlapping.

        The run's assertion graphs are recomputed here rather than
        searched for, which works because `mint_nanopub_uri` is
        deterministic in the run and the source's position. It is also
        exact: it cannot pick up an assertion graph belonging to another
        run, as a substring test over a shared prefix could.
    """
    global INDEXING_MODE
    run_id = str(_uuid_mod.uuid4())
    graph_base = f"{base_named_graph}/{run_id}"

    print(f"[run_query_strict] Run ID: {graph_base}")

    # Normalized here as well as in FindBGPPriority, because the number of
    # sources is needed below in order to reconstruct the set of assertion
    # graphs this run will have minted.
    if isinstance(endpoints, (str, dict, DataSource)):
        endpoints = [endpoints]
    endpoints = list(endpoints)

    if not endpoints:
        print("[run_query_strict] No sources supplied")
        return []

    # Strict mode is enabled for the duration of the harvest only, and
    # the previous value restored afterwards even on failure, so that a
    # raising harvest cannot leave the whole process skipping patterns.
    previous_mode = INDEXING_MODE
    INDEXING_MODE = strict
    print(f"[run_query_strict] Strict mode: {strict}")
    try:
        FindBGPPriority(query, endpoints, base_named_graph=graph_base)
    finally:
        INDEXING_MODE = previous_mode

    # Enumerate the assertion graphs of this run explicitly.
    #
    # FIX: the run was previously scoped with
    #     FILTER(CONTAINS(STR(?assertionGraph), "<graph_base>"))
    # which could never match. mint_nanopub_uri hashes the run identifier
    # through UUID v5, so the assertion graph is
    #     urn:tpf:nanopub:<uuid5>#assertion
    # and does not contain the run identifier as a substring at all. The
    # query therefore returned no rows for every run, which in turn made
    # SPHINX's TPFAdapter -- the only consumer of this function -- produce
    # an empty index.
    #
    # Because mint_nanopub_uri is deterministic in (run_id, index), the
    # same URIs can simply be recomputed here and bound through VALUES.
    # This is exact rather than approximate: it cannot match an assertion
    # graph belonging to a different run, which a substring test on a
    # shared prefix could.
    assertion_graphs = [
        f"{mint_nanopub_uri(graph_base, i + 1)}#assertion"
        for i in range(len(endpoints))
    ]
    values_clause = " ".join(f"<{g}>" for g in assertion_graphs)

    # The join on np:hasAssertion is retained: it enumerates only
    # assertion graphs, excluding the head, provenance and publication
    # information graphs of the nanopublications from the result.
    wrapped_query = f"""
PREFIX np: <http://www.nanopub.org/nschema#>
SELECT ?s ?p ?o WHERE {{
  VALUES ?assertionGraph {{ {values_clause} }}
  ?np np:hasAssertion ?assertionGraph .
  GRAPH ?assertionGraph {{
    ?s ?p ?o .
  }}
}}
"""

    # B.2.7: include_types=True so downstream adapters (SPHINX's
    # TPFAdapter) can tell a URI/IRI term apart from a blank node or a
    # literal instead of guessing from the string value. This is the
    # behaviour the indexer-side copy of this module provided and which
    # the SCARAB-side copy lacked; it is now the single implementation.
    results = execute_sparql_query(wrapped_query, include_types=True)

    if not results:
        return []

    triples = []
    for row in results:
        s = row.get("s")
        p = row.get("p")
        o = row.get("o")
        if s and p and o:
            triples.append((s, p, o))

    print(f"[run_query_strict] Returned {len(triples)} triples")
    return triples
