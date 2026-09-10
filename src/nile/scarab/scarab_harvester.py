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
    """Start the ingestion daemon exactly once per process (B.2.8)."""
    global _flusher_started
    with _flusher_lock:
        if _flusher_started:
            return
        threading.Thread(target=buffer_flusher_daemon, daemon=True).start()
        _flusher_started = True

# -------------------------------------------------------------------------
# SCARAB SOFTWARE IDENTITY (used in nanopub provenance)
# -------------------------------------------------------------------------

SCARAB_CODEBASE_URI = URIRef("https://github.com/myorg/scarab")
SCARAB_VERSION      = "1.0.0"
SCARAB_DOWNLOAD_URI = URIRef(
    f"https://github.com/myorg/scarab/releases/tag/v{SCARAB_VERSION}"
)

# -------------------------------------------------------------------------
# NANOPUB URI MINTING
# -------------------------------------------------------------------------

def mint_nanopub_uri(run_id: str, endpoint_index: int) -> str:
    """
    Mint a stable nanopub base URI for one (run, endpoint) pair.
    Uses uuid5 (deterministic, name-based) so reruns produce the same URI.

    The four sub-graph IRIs are derived by appending:
      #Head, #assertion, #provenance, #pubinfo
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
    """Serialize an RDFLib term (or an already-encoded string) canonically."""
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
    """
    Inverse of term_to_pattern_str.

    Returns an RDFLib term, or None when `text` denotes a variable, the
    path sentinel, or nothing at all (i.e. an unbound position that
    imposes no restriction).
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
    """
    True when the RDF term `value` satisfies the pattern position `req`.

    Comparison is by RDF term equality, not string equality, so a plain
    literal is no longer conflated with an equally-spelled typed or
    language-tagged literal, and a quoted request term no longer fails to
    match the triple it was built from (B.2.1).
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
    """
    Render a canonical pattern term for inclusion in a SPARQL query.

    Variables pass through; literals are already in N3 and pass through;
    everything else is an IRI and is angle-bracketed regardless of scheme
    (B.2.4).
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
    """
    Rebuild an RDFLib term from a SPARQL-JSON binding dict.

    Used so that bindings read back out of the local store retain their
    node kind, datatype and language tag instead of being flattened to a
    bare string, which is what previously made a literal indistinguishable
    from an IRI at bind-join time (B.2.1, B.2.4).
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
    """Recursively serialize an RDFLib path expression to a string token."""
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
    """
    Returns True when a pattern's predicate is a property path expression
    rather than a plain IRI or variable string.
    """
    return pat.get("predicate") == "__PATH__"


def synthetic_path_iri(path_obj) -> URIRef:
    """
    Build the reserved IRI under which the result of a locally evaluated
    property path is materialized (B.2.2).

    path_to_str emits '^' for an inverse path and '|' for an alternative
    path, and neither character is legal in an IRI. Concatenating the raw
    serialization onto the urn:tpf:path: prefix therefore produced a term
    whose n3() raised, aborting the harvest for the whole source. The
    serialization is percent-encoded, which keeps the mapping injective
    (so two distinct paths still receive two distinct predicates) while
    guaranteeing a syntactically valid IRI.

    Both the writer (harvest_endpoint_optimized) and the reader
    (extract_upstream_bindings_graphdb) call this function, so the two
    cannot drift apart.
    """
    return URIRef("urn:tpf:path:" + quote(path_to_str(path_obj), safe=""))


def extract_base_iris_from_path(path) -> list:
    """
    Walk a path expression tree and collect every concrete IRI it references.
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
    """
    Derive the ordinary triple patterns that must be harvested before a
    property path can be evaluated locally (B.1.3).

    evaluate_path_locally computes the closure over triples that are
    already in the local store, filtered to the base predicates of the
    path. Nothing previously scheduled the retrieval of those triples, so
    unless a base predicate happened to appear elsewhere in the query as a
    simple pattern, the closure was computed over an empty graph and the
    path yielded no bindings.

    For each distinct base IRI of the path this returns one pattern

        ?__path{k}_{n}_s   <baseIRI>   ?__path{k}_{n}_o

    using fresh variable names so that the derived pattern cannot join
    accidentally with any variable of the original query. The pattern is
    unconstrained because a transitive or arbitrary-length path may
    traverse intermediate resources that no binding of the query
    constrains; retrieving the full extent of the base predicates is what
    makes the local closure complete with respect to the harvested data.
    Derived patterns are flagged so that provenance can distinguish them
    from the triple patterns of the query proper.
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
    """
    Return `bgp` extended with the support patterns required by every
    property path it contains (B.1.3).

    A support pattern is suppressed when the query already requests the
    same predicate as a simple unconstrained pattern, so that no fragment
    is requested twice. Support patterns are ordinary simple patterns and
    are therefore scheduled by the ordinary cardinality/connectivity rule,
    which places them before the path patterns (paths are always appended
    last).
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
    vars_ = []
    for field in ("subject", "predicate", "object", "graph"):
        val = pat.get(field)
        if val and val != "__PATH__" and val.startswith("?"):
            vars_.append(val[1:])
    return vars_

def shares_variable(pat, processed_patterns):
    vars = set(extract_vars_from_pattern(pat))
    for p in processed_patterns:
        if vars.intersection(extract_vars_from_pattern(p)):
            return True
    return False


def evaluate_path_locally(pat: dict, named_graph: str) -> list[dict]:
    """
    Resolve a property-path pattern against triples already stored in
    GraphDB, returning a list of variable-binding dicts.
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
    """
    Comunica-style triple filtering.

    B.2.1: comparison is now by RDF term equality via
    pattern_term_matches. The previous string comparison tested the
    N3-encoded request term ('"Aspirin"') against the plain lexical form
    of the retrieved literal ('Aspirin'); the two never agreed, every
    triple of the page was rejected, data_triples fell to zero and the
    traversal terminated at the first page having ingested nothing.
    """
    return (
        pattern_term_matches(req_s, s) and
        pattern_term_matches(req_p, p) and
        pattern_term_matches(req_o, o)
    )


def tpf_uri_request_builder(control_uri, subject, predicate, object_, graph=None):
    """
    Build a TPF or QPF request URL.
    Skips variables (starting with ?) and None values.
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
    if re.search(r'no\s*triples', html, re.I):
        return 0
    if 'rel="next"' in html:
        return 10000
    triple_count = len(re.findall(r'property=|typeof=', html))
    if triple_count > 0:
        return triple_count
    return 5000


def get_pattern_count(control_uri, subject, predicate, object_, graph=None):
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
    return any(str(p).startswith(ns) for ns in METADATA_NAMESPACES)


def fetch_tpf_page(url):
    """Fetch and parse a TPF/QPF page. Returns full ConjunctiveGraph."""
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
    """
    Harvest triples from a TPF/QPF endpoint for ONE triple pattern.
    Filters triples by the requested pattern before buffering.
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
    """Common interface over TPF/QPF servers, SPARQL endpoints and dumps."""

    kind = "abstract"

    def __init__(self, location):
        self.location = location

    # -- provenance -------------------------------------------------------
    def identity_uri(self):
        """The IRI by which this source is denoted in the provenance graph."""
        return URIRef(self.location)

    def provenance_type(self):
        """(rdf:type, access-property) used to describe the source."""
        return DCAT.DataService, DCAT.endpointURL

    def label(self):
        return f"{self.kind}: {self.location}"

    # -- retrieval --------------------------------------------------------
    def count(self, pat):
        raise NotImplementedError

    def harvest(self, pat, named_graph):
        raise NotImplementedError


class TPFDataSource(DataSource):
    """
    Triple/Quad Pattern Fragments server.

    Behaviour is exactly that of the previous implementation: the pattern
    is encoded as a fragment selector, the cardinality is read from the
    control metadata, and the fragment is traversed page by page.
    """

    kind = "tpf"

    def count(self, pat):
        return get_pattern_count(
            self.location,
            pat["subject"],
            pat["predicate"],
            pat["object"],
            pat.get("graph"),
        )

    def harvest(self, pat, named_graph):
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
    """
    Remote SPARQL endpoint used as a source of triple patterns.

    The endpoint is never asked to evaluate the query: it is asked for one
    triple pattern at a time, exactly as a fragment server would be. This
    keeps the retrieval-before-evaluation property of the suite intact,
    and means an endpoint that can answer only part of the query still
    contributes everything it holds for the patterns it can answer.

    The graph component is handled symmetrically with SPHINX: a pattern
    carrying a concrete graph IRI is scoped with GRAPH, and a pattern with
    no graph term is matched against the default graph OR any named graph,
    so that content is found wherever the repository chose to put it.
    """

    kind = "sparql"

    def _where_clause(self, pat):
        s = sparql_term(pat["subject"])
        p = sparql_term(pat["predicate"])
        o = sparql_term(pat["object"])
        graph = pat.get("graph")

        core = f"{s} {p} {o} ."

        if graph is not None and not graph.startswith("?"):
            return f"GRAPH {sparql_term(graph)} {{ {core} }}"

        return f"{{ {core} }} UNION {{ GRAPH ?__g {{ {core} }} }}"

    def _projection(self, pat):
        """Project the variable positions; bound positions are echoed back."""
        return {
            "subject":   pat["subject"],
            "predicate": pat["predicate"],
            "object":    pat["object"],
        }

    def count(self, pat):
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
    """
    Local RDF dump file.

    The dump is parsed once, on first use, into an in-memory
    ConjunctiveGraph so that a quad-based serialization (N-Quads, TriG)
    retains its named graphs and can be addressed by a pattern carrying a
    graph term, exactly as the QPF interface allows.
    """

    kind = "dump"

    def __init__(self, location, rdf_format=None):
        super().__init__(location)
        self.rdf_format = rdf_format
        self._graph = None
        self._load_lock = threading.Lock()

    def identity_uri(self):
        try:
            return URIRef(_FsPath(self.location).resolve().as_uri())
        except Exception:
            # Already a URL, or a path that cannot be resolved
            return URIRef(str(self.location))

    def provenance_type(self):
        # A dump is a distribution, not a service.
        return DCAT.Dataset, DCAT.downloadURL

    def _ensure_loaded(self):
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
        return sum(1 for _ in self._matching(pat))

    def harvest(self, pat, named_graph):
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
    """
    Build a DataSource from a caller-supplied specification.

    Accepted forms:

      {"type": "tpf"|"sparql"|"dump", "location": ..., "format": ...}
      "tpf:<url>"        an explicit TPF/QPF server
      "sparql:<url>"     an explicit SPARQL endpoint
      "dump:<path>"      an explicit RDF dump
      "<path-or-url>"    inferred

    Inference is deliberately conservative: a bare http(s) URL is taken to
    be a TPF/QPF server, which preserves the behaviour of every existing
    caller, and a SPARQL endpoint must therefore be declared explicitly.
    Anything carrying a recognised RDF file extension, or a file:// URL,
    or an existing path on disk, is taken to be a dump.
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
    """
    Substitute a binding into a pattern, returning a new pattern dict.

    B.2.1: the canonical encoder is used for every position, so a bound
    literal is rendered in its N3 form both in the request sent to the
    source and in the filter applied to the response. The two are the same
    string, which is what makes the round trip work.
    """
    def concretize(term):
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
    source.harvest(concretize_pattern(pat, binding), named_graph)


def fetch_binding_batch(batch, pat, source, named_graph):
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
    return pattern_term_matches(pattern_term, triple_term)

def triple_matches_pattern(triple, pat):
    s, p, o = triple
    if not term_matches(pat["subject"], s):
        return False
    if not term_matches(pat["predicate"], p):
        return False
    if not term_matches(pat["object"], o):
        return False
    return True

def extract_upstream_bindings(repo, current_idx, harvested, bgp):
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
    """Send triples directly to the local store as RDF (N-Triples or N-Quads)."""
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
    """Push triple into ingestion queue (blocking if full)."""
    s, p, o = triple

    if named_graph:
        line = f"{s.n3()} {p.n3()} {o.n3()} <{named_graph}> .\n"
    else:
        line = f"{s.n3()} {p.n3()} {o.n3()} .\n"

    _ingest_queue.put(line)


def buffer_flusher_daemon():
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
    """
    Write the three non-data nanopub graphs (Head, provenance, pubinfo)
    directly to the local store as n-quads.

    Called AFTER _ingest_queue.join() so all data triples are guaranteed
    to be in the assertion graph before provenance is written.

    The assertion graph is already populated by the buffer flusher —
    this function only adds the provenance wrapper around it.

    `source` is a DataSource. The source is described according to its
    kind: a TPF/QPF server or SPARQL endpoint is a dcat:DataService
    identified by its dcat:endpointURL, whereas an RDF dump is a
    dcat:Dataset identified by its dcat:downloadURL. In every case the
    description distinguishes the resource that was consumed from SCARAB
    itself, which is the agent that performed the harvest.
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
    """
    Harvest all BGP patterns from one source into the nanopub's assertion
    graph, then write the nanopub provenance wrapper.

    `source` is a DataSource — a TPF/QPF server, a SPARQL endpoint or an
    RDF dump. The scheduling, bind join, ingestion and provenance
    behaviour is identical for all three; only count() and harvest()
    differ. A bare string is still accepted and is resolved through
    make_datasource, so existing callers are unaffected.

    The assertion graph IRI is derived from nanopub_base:
        named_graph = nanopub_base + "#assertion"

    This replaces the old `named_graph` parameter so the assertion graph
    IS the nanopub assertion graph — no duplication, no separate copy.
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
    """
    Execute a SPARQL query and return its solutions.

    include_types=False (default): each binding is flattened to its plain
    string value, as used by the callers that only need to compare or
    print a value.

    include_types=True: each binding is kept as the full SPARQL-JSON dict
    ({"type": "uri"|"bnode"|"literal"|..., "value": ..., optional
    "datatype"/"xml:lang"}), so callers can tell a URI/IRI term from a
    blank node or a literal without guessing from the string. This is what
    SPHINX's TPFAdapter requires in order not to mistake a blank-node
    class for a resolvable class IRI, and what the bind join requires in
    order to re-serialize a literal with its datatype intact.

    B.2.7: this parameter previously existed only in the copy of this
    module that lived alongside the indexer. Merging it here is what
    allows that copy to be deleted and both modules to share one
    back-end.

    endpoint: defaults to the local store. Passing an explicit endpoint is
    what allows SPARQLDataSource to reuse this function against a remote
    source; the local store remains the default for every other caller.
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


def run_query_strict(query, endpoints, base_named_graph="urn:tpf:temp"):
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

    FindBGPPriority(query, endpoints, base_named_graph=graph_base)

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
