"""SCARAB: the federated harvester.

Retrieves the data a query needs from one repository at a time, keeps it,
and leaves the query to be evaluated over the union of what was harvested.

The unit of work is a query and a single repository. The query is broken
into its triple patterns and the repository is asked for one pattern at a
time. Patterns are requested smallest first, and each is restricted by the
values that the patterns already harvested from the same repository bind
its variables to -- a bind join -- so that little more comes back than the
query can use. Everything retrieved is kept, whether or not the repository
can answer the rest of the query, so that its contribution can later be
combined with the contributions of repositories that were harvested
elsewhere, by other parties, or at other times.

Usage::

    from nile.scarab.scarab_harvester import answer_query

    results = answer_query(query, [
        "http://example.org/fragments",      # fragments server
        "sparql:http://example.org/sparql",  # SPARQL endpoint
        "dump:/data/repository.ttl",         # local RDF file
    ])

`answer_query` harvests and then evaluates the query over the data this run
harvested, and nothing else. The two steps are also available separately:
`FindBGPPriority` harvests and returns the run's assertion graphs, and
`evaluate_over_run` answers a query over them. For harvests produced
independently (for instance, inside different institutions' networks) and
exported as files, see `load_harvest_files` and `evaluate_harvest_files`.

How a harvest works
    Each source is harvested into its own *working store*, an embedded
    pyoxigraph database (on disk by default, so memory use does not grow
    with the size of the harvest). The working store answers the bind-join
    binding queries and evaluates property paths locally. When the source
    is finished, the four graphs of its nanopublication are *published*:
    to the configured triplestore (the default), to a local pyoxigraph
    store, and/or to a TriG file. Synthetic triples produced by local
    property path evaluation stay in the working store and are never
    published.

Which patterns may restrict which
    A bind join must never remove a triple that could change the answer.
    Inside a basic graph pattern this only requires the restricting
    patterns to have been harvested. Across operators it does not:
    `transform` therefore records, for every pattern, the path of operators
    leading to it, and `restriction_plan` derives from those paths which
    harvested patterns may restrict it and through which variables. The
    required side of an OPTIONAL may restrict the optional side, the left
    side of a MINUS its right side, and a filtered group the patterns of its
    EXISTS / NOT EXISTS expressions; never the reverse, never across UNION
    branches, and never into or out of a subquery. A restriction that
    crosses an OPTIONAL, MINUS or EXISTS boundary from outside is only made
    through variables that the required side of that boundary always binds,
    which is what keeps a removed triple from turning an extended solution
    into an unextended one that would then join differently.

    The restriction uses bindings from the repository being harvested only.
    Under isolated harvesting this can omit triples that would have joined
    with another repository's contribution; see the thesis discussion of
    the completeness of bind joins under isolated harvesting.

Forced dumps
    When the patterns that may restrict a pattern yield no bindings at a
    source, the pattern is retrieved in full there, since the restricting
    data may be held by another repository. For a pattern whose subject,
    predicate and object are all variables, that means retrieving the whole
    repository. Such a *forced dump* is not performed (SCARAB_SKIP_FORCED_DUMPS):
    the pattern is skipped at that source, the skip is reported in the run
    statistics, and the nanopublication's provenance declares that the
    pattern was not retrieved from that source and that the assertion graph
    is therefore incomplete. A skipped pattern does not restrict other
    patterns, since its contents at that source are unknown. An all-variable
    pattern that nothing may restrict is still retrieved in full, since then
    the query itself asks for it.

Sources
    A bare http(s) location is read as a fragments server. A SPARQL
    endpoint has to be declared with a "sparql:" prefix, because the two
    cannot be told apart by their URL and mistaking one for the other
    would quietly yield nothing at all. See `make_datasource`.

Configuration
    Read from the environment at import time, and adjustable at run time
    with `configure`::

        SCARAB_STORE_KIND            "remote" (default) publishes to the
                                     triplestore below; "local" to an
                                     embedded pyoxigraph store; "none"
                                     only to SCARAB_EXPORT_DIR
        SCARAB_STORE_BASE            remote triplestore, e.g.
                                     http://localhost:7200
        SCARAB_STORE_REPOSITORY      repository within it
        SCARAB_STORE_QUERY_URL       overrides the derived query URL
        SCARAB_STORE_STATEMENTS_URL  overrides the derived statements URL
        SCARAB_LOCAL_STORE_PATH      directory of the local store, or
                                     "memory" (default)
        SCARAB_EXPORT_DIR            if set, each source's nanopublication
                                     is also written there as TriG
        SCARAB_WORKING_STORE_DIR     where working stores are created, or
                                     "memory"; default: system temp dir
        SCARAB_MAX_THREADS           concurrent requests per source (16)
        SCARAB_PARALLEL_SOURCES      sources harvested concurrently (4)
        SCARAB_PAGE_CACHE_SIZE       fragment pages cached per run (2048)
        SCARAB_FULL_PAGE_WEIGHT      cost of one fragment page relative to
                                     one bound request (1.0)
        SCARAB_SPARQL_VALUES_BLOCK   bindings per VALUES request to a
                                     SPARQL source (100)
        SCARAB_HTTP_TIMEOUT          seconds (60)
        SCARAB_HARVEST_PATH_BASE_PREDICATES
                                     "0" stops property paths retrieving
                                     their base predicates
        SCARAB_SKIP_FORCED_DUMPS     "0" retrieves forced dumps instead of
                                     skipping and declaring them

    Only the SPARQL protocol and the RDF graph store protocol are used to
    talk to the remote store, so any conformant triplestore can stand in
    for the GraphDB deployment these default to.

Pattern dicts
    A query pattern travels through the module as a dict::

        {
            "subject":   <term>,
            "predicate": <term> or "__PATH__",
            "object":    <term>,
            "graph":     <term> or None,
            "scopes":    [scope, ...],   # one per occurrence in the query
            "operators": [str, ...],     # innermost operator, per occurrence
            "predicate_path":    rdflib path, on path patterns only,
            "derived_from_path": str, on generated support patterns only,
        }

    Every position is a string in one canonical encoding::

        variable     ?name
        IRI          http://example.org/x      (bare, any scheme)
        literal      "lex", "lex"@en, "lex"^^<datatype>   (N3 form)
        blank node   _:label
        path         the "__PATH__" sentinel

    Blank nodes written in the query are replaced by fresh variables, since
    that is how SPARQL reads them.

Logging and statistics
    Progress is reported through the "nile.scarab" logger (configure it
    with `logging.basicConfig(level=logging.INFO)` or `configure(
    log_level="INFO")`). Per-source counts of requests, pages, bytes and
    triples for the last run are available from `last_run_stats`.
"""

import os
import re
import math
import time
import uuid as _uuid_mod
import atexit
import shutil
import logging
import tempfile
import threading
from collections import OrderedDict, namedtuple
from functools import lru_cache
from pathlib import Path as _FsPath
from urllib.parse import urlencode, quote
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pyoxigraph as ox

from rdflib import URIRef, Literal, Variable, BNode
from rdflib.namespace import Namespace, RDF, RDFS, XSD
from rdflib.util import from_n3
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery
from rdflib.plugins.sparql.parserutils import CompValue
from rdflib.paths import (
    MulPath, SequencePath, AlternativePath, InvPath, NegatedPath, Path
)

logger = logging.getLogger("nile.scarab")

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

_HYDRA_TOTAL      = "http://www.w3.org/ns/hydra/core#totalItems"
_HYDRA_NEXT       = "http://www.w3.org/ns/hydra/core#next"
_HYDRA_NEXT_PAGE  = "http://www.w3.org/ns/hydra/core#nextPage"
_HYDRA_PER_PAGE   = "http://www.w3.org/ns/hydra/core#itemsPerPage"
_VOID_TRIPLES     = "http://rdfs.org/ns/void#triples"
_CONTROL_NAMESPACES = (
    "http://www.w3.org/ns/hydra/core#",
    "http://rdfs.org/ns/void#",
)

# -------------------------------------------------------------------------
# CONFIGURATION
# -------------------------------------------------------------------------

def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def _env_float(name, default):
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


MAX_THREADS          = _env_int("SCARAB_MAX_THREADS", 16)
PARALLEL_SOURCES     = _env_int("SCARAB_PARALLEL_SOURCES", 4)
PAGE_CACHE_SIZE      = _env_int("SCARAB_PAGE_CACHE_SIZE", 2048)
FULL_PAGE_WEIGHT     = _env_float("SCARAB_FULL_PAGE_WEIGHT", 1.0)
SPARQL_VALUES_BLOCK  = _env_int("SCARAB_SPARQL_VALUES_BLOCK", 100)
SPARQL_PAGE_EQUIV    = _env_int("SCARAB_SPARQL_PAGE_EQUIV", 10_000)
HTTP_TIMEOUT         = _env_float("SCARAB_HTTP_TIMEOUT", 60)
HTTP_RETRIES         = _env_int("SCARAB_HTTP_RETRIES", 2)
PUBLISH_BATCH        = _env_int("SCARAB_PUBLISH_BATCH", 100_000)
MAX_IN_FLIGHT_FACTOR = 8      # bound requests queued per worker thread

STORE_KIND = os.environ.get("SCARAB_STORE_KIND", "remote").lower()
STORE_BASE = os.environ.get("SCARAB_STORE_BASE", "http://acb8computer:7200")
STORE_REPOSITORY = os.environ.get("SCARAB_STORE_REPOSITORY", "test1")
STORE_QUERY_URL = os.environ.get(
    "SCARAB_STORE_QUERY_URL", f"{STORE_BASE}/repositories/{STORE_REPOSITORY}"
)
STORE_STATEMENTS_URL = os.environ.get(
    "SCARAB_STORE_STATEMENTS_URL", f"{STORE_QUERY_URL}/statements"
)
LOCAL_STORE_PATH     = os.environ.get("SCARAB_LOCAL_STORE_PATH", "memory")
EXPORT_DIR           = os.environ.get("SCARAB_EXPORT_DIR", "") or None
WORKING_STORE_DIR    = os.environ.get("SCARAB_WORKING_STORE_DIR", "") or None
KEEP_WORKING_STORES  = os.environ.get("SCARAB_KEEP_WORKING_STORES", "0") in ("1", "true", "True")

# Strict mode: a pattern whose restricting patterns yield no bindings is
# skipped instead of being retrieved in full. Used by SPHINX's exploratory
# indexing through `run_query_strict`; off for federated querying.
INDEXING_MODE = False

# Skip, and declare, the retrieval of an all-variable pattern that would only
# be retrieved in full because its restricting patterns yield no bindings at a
# source (a forced dump of the whole repository). See the module docstring.
SKIP_FORCED_DUMPS = os.environ.get("SCARAB_SKIP_FORCED_DUMPS", "1") not in ("0", "false", "False")

# Skolemised blank nodes. A fragments server backed by a SPARQL store
# cannot look a blank node up by its label: it has to turn a requested
# genid IRI back into `_:label`, which SPARQL reads as a variable, so the
# request silently matches every triple with that predicate. Terms with
# these prefixes are therefore never substituted into a request; when a
# binding holds one, the fragment is retrieved once and filtered locally.
SKOLEM_PREFIXES = tuple(
    prefix for prefix in
    os.environ.get("SCARAB_SKOLEM_PREFIXES", "genid:").split(",")
    if prefix
)

# When True, the base predicates referenced by a property path are
# scheduled as ordinary triple patterns so that the local evaluation of
# the path has triples to traverse.
HARVEST_PATH_BASE_PREDICATES = os.environ.get(
    "SCARAB_HARVEST_PATH_BASE_PREDICATES", "1"
) not in ("0", "false", "False")

_CONFIGURABLE = {
    "max_threads": "MAX_THREADS",
    "parallel_sources": "PARALLEL_SOURCES",
    "page_cache_size": "PAGE_CACHE_SIZE",
    "full_page_weight": "FULL_PAGE_WEIGHT",
    "sparql_values_block": "SPARQL_VALUES_BLOCK",
    "sparql_page_equiv": "SPARQL_PAGE_EQUIV",
    "http_timeout": "HTTP_TIMEOUT",
    "http_retries": "HTTP_RETRIES",
    "publish_batch": "PUBLISH_BATCH",
    "store_kind": "STORE_KIND",
    "store_query_url": "STORE_QUERY_URL",
    "store_statements_url": "STORE_STATEMENTS_URL",
    "local_store_path": "LOCAL_STORE_PATH",
    "export_dir": "EXPORT_DIR",
    "working_store_dir": "WORKING_STORE_DIR",
    "keep_working_stores": "KEEP_WORKING_STORES",
    "harvest_path_base_predicates": "HARVEST_PATH_BASE_PREDICATES",
    "skip_forced_dumps": "SKIP_FORCED_DUMPS",
}


def configure(**settings):
    """Change configuration at run time, e.g. from a notebook.

    Accepts the lower-case names of the settings listed in the module
    docstring (``max_threads=32``, ``store_kind="local"``, ...), plus
    ``store_base`` and ``store_repository`` (which re-derive the query and
    statements URLs) and ``log_level`` (which attaches a stream handler to
    the "nile.scarab" logger if it has none).

    Raises:
        ValueError: For an unknown setting.
    """
    global STORE_QUERY_URL, STORE_STATEMENTS_URL, _LOCAL_STORE
    g = globals()
    if "store_base" in settings or "store_repository" in settings:
        base = settings.pop("store_base", STORE_BASE)
        repo = settings.pop("store_repository", STORE_REPOSITORY)
        g["STORE_BASE"], g["STORE_REPOSITORY"] = base, repo
        STORE_QUERY_URL = f"{base}/repositories/{repo}"
        STORE_STATEMENTS_URL = f"{STORE_QUERY_URL}/statements"
    level = settings.pop("log_level", None)
    if level is not None:
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            logger.addHandler(handler)
        logger.setLevel(level)
    for key, value in settings.items():
        if key not in _CONFIGURABLE:
            raise ValueError(f"Unknown SCARAB setting: {key}")
        if key == "store_kind":
            value = str(value).lower()
        if key == "local_store_path" and value != LOCAL_STORE_PATH:
            with _local_store_lock:
                _LOCAL_STORE = None
        g[_CONFIGURABLE[key]] = value


_TEMP_DIRS = set()
_temp_lock = threading.Lock()


def _register_temp(path):
    with _temp_lock:
        _TEMP_DIRS.add(path)


def _remove_temp(path):
    with _temp_lock:
        _TEMP_DIRS.discard(path)
    shutil.rmtree(path, ignore_errors=True)


@atexit.register
def _cleanup_temp_dirs():
    for path in list(_TEMP_DIRS):
        shutil.rmtree(path, ignore_errors=True)


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
        The base URI. The four graphs of the nanopublication are named by
        appending "#Head", "#assertion", "#provenance" and "#pubinfo".
    """
    slug = _uuid_mod.uuid5(_uuid_mod.NAMESPACE_URL, f"{run_id}/endpoint{endpoint_index}")
    return f"urn:tpf:nanopub:{slug}"


def run_assertion_graphs(run_id: str, n_sources: int) -> list:
    """Return the assertion graph IRIs of one run, in source order."""
    return [f"{mint_nanopub_uri(run_id, i + 1)}#assertion" for i in range(n_sources)]


# -------------------------------------------------------------------------
# CANONICAL PATTERN TERM ENCODING
# -------------------------------------------------------------------------

_PATH_SENTINEL = "__PATH__"


def term_to_pattern_str(term):
    """Encode an rdflib term as a canonical pattern position.

    Returns:
        The encoding described in the module docstring, or None for None.
        Anything unrecognised becomes the "__PATH__" sentinel.
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

    Returns:
        The rdflib term, or None where the position is unbound (a
        variable, the path sentinel, or nothing at all).
    """
    if text is None or text == _PATH_SENTINEL:
        return None
    if not isinstance(text, str):
        return text
    if text.startswith("?"):
        return None
    if text.startswith("_:"):
        return BNode(text[2:])
    if text.startswith("<") and text.endswith(">"):
        return URIRef(text[1:-1])
    if text.startswith('"') or text.startswith("'"):
        try:
            return from_n3(text)
        except Exception:
            return Literal(text.strip('"\''))
    return URIRef(text)


def sparql_term(text):
    """Render a canonical pattern position for use in a SPARQL query."""
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
    """Rebuild an rdflib term from a SPARQL-JSON binding."""
    if binding is None:
        return None
    kind = binding.get("type")
    value = binding.get("value")
    if kind == "uri":
        return URIRef(value)
    if kind == "bnode":
        return BNode(value)
    if kind in ("literal", "typed-literal"):
        lang = binding.get("xml:lang")
        dtype = binding.get("datatype")
        if lang:
            return Literal(value, lang=lang)
        if dtype:
            return Literal(value, datatype=URIRef(dtype))
        return Literal(value)
    return URIRef(value) if value is not None else None


# --- pyoxigraph side -----------------------------------------------------
# Data travels through the harvester as pyoxigraph terms; patterns travel
# as canonical strings. These convert between the two.

_XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"


@lru_cache(maxsize=65536)
def ox_from_pattern(text):
    """Decode a canonical pattern position into a pyoxigraph term.

    Returns:
        The term, or None where the position is unbound.

    Raises:
        ValueError: If the position is not a valid RDF term.
    """
    if text is None or text == _PATH_SENTINEL or text.startswith("?"):
        return None
    if text.startswith("_:"):
        return ox.BlankNode(text[2:])
    if text.startswith('"') or text.startswith("'"):
        # Turtle accepts every N3 literal form rdflib produces, including
        # triple-quoted ones, so it is used to read the literal back.
        triple = next(iter(ox.parse(f"<urn:x:s> <urn:x:p> {text} .",
                                    format=ox.RdfFormat.TURTLE)))
        return triple.object
    if text.startswith("<") and text.endswith(">"):
        return ox.NamedNode(text[1:-1])
    return ox.NamedNode(text)


def pattern_from_ox(term):
    """Encode a pyoxigraph term as a canonical pattern position."""
    if isinstance(term, ox.NamedNode):
        return term.value
    if isinstance(term, ox.BlankNode):
        return f"_:{term.value}"
    return str(term)        # literals: N-Triples form, which is N3 as well


def ox_from_rdflib(term):
    """Convert an rdflib term to the corresponding pyoxigraph term."""
    if isinstance(term, URIRef):
        return ox.NamedNode(str(term))
    if isinstance(term, BNode):
        return ox.BlankNode(str(term))
    if isinstance(term, Literal):
        if term.language:
            return ox.Literal(str(term), language=term.language)
        if term.datatype is not None:
            return ox.Literal(str(term), datatype=ox.NamedNode(str(term.datatype)))
        return ox.Literal(str(term))
    raise TypeError(f"Cannot convert {term!r}")


def ox_from_sparql_json(binding):
    """Rebuild a pyoxigraph term from a SPARQL-JSON binding."""
    kind = binding.get("type")
    value = binding.get("value")
    if kind == "uri":
        return ox.NamedNode(value)
    if kind == "bnode":
        return ox.BlankNode(value)
    lang = binding.get("xml:lang")
    dtype = binding.get("datatype")
    if lang:
        return ox.Literal(value, language=lang)
    if dtype and dtype != _XSD_STRING:
        return ox.Literal(value, datatype=ox.NamedNode(dtype))
    return ox.Literal(value)


def ox_to_sparql_json(term):
    """Render a pyoxigraph term as a SPARQL-JSON binding dict."""
    if isinstance(term, ox.NamedNode):
        return {"type": "uri", "value": term.value}
    if isinstance(term, ox.BlankNode):
        return {"type": "bnode", "value": term.value}
    out = {"type": "literal", "value": term.value}
    if term.language:
        out["xml:lang"] = term.language
    elif term.datatype is not None and term.datatype.value != _XSD_STRING:
        out["datatype"] = term.datatype.value
    return out


def is_unbindable(term):
    """Return True for a term that cannot identify a node at the source.

    Blank nodes have no identity outside the response that produced them,
    and skolemised blank nodes (see SKOLEM_PREFIXES) cannot be looked up
    through a SPARQL-backed server. Such values are never substituted into
    a request; a fragment restricted by them is retrieved once and
    filtered locally instead.
    """
    if isinstance(term, (ox.BlankNode, BNode)):
        return True
    if isinstance(term, (ox.NamedNode, URIRef)):
        text = term.value if isinstance(term, ox.NamedNode) else str(term)
        return text.startswith(SKOLEM_PREFIXES) or "/.well-known/genid/" in text
    return False


# -------------------------------------------------------------------------
# QUERY DECOMPOSITION, WITH OPERATOR SCOPES
# -------------------------------------------------------------------------
# Every pattern is recorded together with its *scope*: the path of algebra
# operators from the root of the query to the basic graph pattern holding
# it. A scope is a tuple of Steps. A step names the operator node, the
# role the pattern's branch plays at that node, and -- for the roles whose
# patterns depend on another branch -- the variables that branch always
# binds.
#
#   role      at node                 meaning for the patterns below it
#   ------    ----------------------  ---------------------------------
#   bgp       BGP                     conjunctive with the rest of the BGP
#   and:1/2   Join                    conjunctive with the other operand
#   req       LeftJoin, Minus, Filter the side the other side depends on
#             or Extend with EXISTS
#   opt       LeftJoin                the optional side
#   minus     Minus                   the subtracted side
#   exists:k  Filter, Extend,         the k-th EXISTS / NOT EXISTS of the
#             LeftJoin expression     node's expression
#   branch:1/2 Union                  one alternative
#   sub       subquery                a variable scope of its own
#   opaque    anything unrecognised   treated like a subquery
#
# `restriction_plan` reads these to decide which harvested patterns may
# restrict a pattern, and through which variables.

Step = namedtuple("Step", ["node", "role", "cert"])

_REQUIRED_KINDS = frozenset({"bgp", "and", "req"})
_DEPENDENT_KINDS = frozenset({"opt", "minus", "exists"})
_BARRIER_KINDS = frozenset({"sub", "opaque"})
# (role of the restricting pattern, role of the restricted pattern) at the
# operator where their scopes part.
_ALLOWED_PAIRS = frozenset({
    ("and", "and"),        # the two operands of a Join
    ("req", "opt"),        # required side of OPTIONAL -> optional side
    ("req", "minus"),      # left of MINUS -> right of MINUS
    ("req", "exists"),     # filtered group -> its EXISTS patterns
    ("opt", "exists"),     # optional side -> EXISTS in OPTIONAL's filter
})

_TRANSPARENT = frozenset({"Distinct", "Reduced", "Slice", "OrderBy"})


def _kind(role):
    return role.split(":", 1)[0]


def _expr_exists(expr):
    """Find the EXISTS / NOT EXISTS sub-expressions of an expression."""
    found = []

    def visit(e):
        if isinstance(e, CompValue):
            if e.name in ("Builtin_EXISTS", "Builtin_NOTEXISTS"):
                found.append(e)
                return
            for value in e.values():
                visit(value)
        elif isinstance(e, (list, tuple)):
            for value in e:
                visit(value)

    visit(expr)
    return found


class _ScopeWalker:
    """Collect the patterns of an algebra tree together with their scopes."""

    def __init__(self):
        self.patterns = []
        self._ids = {}
        self._bnodes = {}

    def _id(self, node):
        return self._ids.setdefault(id(node), len(self._ids))

    def _term(self, term):
        # A blank node in a query is a variable that cannot be projected.
        if isinstance(term, BNode):
            return self._bnodes.setdefault(term, Variable(f"__bn{len(self._bnodes)}"))
        return term

    @staticmethod
    def _vars(*terms):
        return {str(t) for t in terms if isinstance(t, Variable)}

    def walk(self, node, path, graph=None, operator="BGP"):
        """Walk `node`, recording patterns; return the variables it always binds."""
        name = getattr(node, "name", None)
        nid = self._id(node)

        if name == "BGP":
            cert = set()
            scope = path + (Step(nid, "bgp", None),)
            for s, p, o in node.triples:
                s, o = self._term(s), self._term(o)
                if isinstance(p, Path) and not isinstance(p, URIRef):
                    self.patterns.append({
                        "subject": s, "predicate": _PATH_SENTINEL,
                        "predicate_path": p, "object": o, "graph": graph,
                        "scope": scope, "operator": operator,
                    })
                    cert |= self._vars(s, o)
                else:
                    p = self._term(p)
                    self.patterns.append({
                        "subject": s, "predicate": p, "object": o,
                        "graph": graph, "scope": scope, "operator": operator,
                    })
                    cert |= self._vars(s, p, o)
            return cert

        if name == "Join":
            c1 = self.walk(node.p1, path + (Step(nid, "and:1", None),), graph, operator)
            c2 = self.walk(node.p2, path + (Step(nid, "and:2", None),), graph, operator)
            return c1 | c2

        if name == "LeftJoin":
            left = self.walk(node.p1, path + (Step(nid, "req", None),), graph, operator)
            right = self.walk(node.p2, path + (Step(nid, "opt", frozenset(left)),), graph, "OPTIONAL")
            for k, e in enumerate(_expr_exists(getattr(node, "expr", None))):
                self._walk_exists(e, path + (Step(nid, f"exists:{k}", frozenset(left | right)),), graph)
            return left

        if name == "Minus":
            left = self.walk(node.p1, path + (Step(nid, "req", None),), graph, operator)
            self.walk(node.p2, path + (Step(nid, "minus", frozenset(left)),), graph, "MINUS")
            return left

        if name == "Union":
            c1 = self.walk(node.p1, path + (Step(nid, "branch:1", None),), graph, "UNION")
            c2 = self.walk(node.p2, path + (Step(nid, "branch:2", None),), graph, "UNION")
            return c1 & c2

        if name in ("Filter", "Extend"):
            exists = _expr_exists(getattr(node, "expr", None))
            if not exists:
                return self.walk(node.p, path, graph, operator)
            inner = self.walk(node.p, path + (Step(nid, "req", None),), graph, operator)
            for k, e in enumerate(exists):
                self._walk_exists(e, path + (Step(nid, f"exists:{k}", frozenset(inner)),), graph)
            return inner

        if name == "Graph":
            term = node.term
            inner = self.walk(node.p, path, term, operator)
            return inner | self._vars(term)

        if name == "Project":
            inner = self.walk(node.p, path, graph, operator)
            return inner & {str(v) for v in node.PV}

        if name in _TRANSPARENT:
            return self.walk(node.p, path, graph, operator)

        if name in ("Group", "AggregateJoin"):
            self.walk(node.p, path, graph, operator)
            return set()

        if name == "ToMultiSet":
            child = node.p
            if getattr(child, "name", None) == "values":
                return set()
            return self.walk(child, path + (Step(nid, "sub", None),), graph, "SUBQUERY")

        if name in ("values", "ServiceGraphPattern"):
            # SERVICE names its own endpoint: its patterns are not harvested.
            return set()

        if name in ("SelectQuery", "AskQuery", "ConstructQuery", "DescribeQuery"):
            return self.walk(node.p, path, graph, operator)

        # Unrecognised operator: walk its operands, but let nothing restrict
        # across it.
        logger.debug("Unrecognised algebra node %s treated as opaque", name)
        for attr in ("p", "p1", "p2"):
            child = getattr(node, attr, None)
            if getattr(child, "name", None):
                self.walk(child, path + (Step(nid, "opaque", None),), graph, operator)
        return set()

    def _walk_exists(self, exists_node, path, graph):
        label = "NOT EXISTS" if exists_node.name == "Builtin_NOTEXISTS" else "EXISTS"
        # rdflib keeps the translated pattern on the attribute and the
        # untranslated parse tree under the key of the same name.
        self.walk(exists_node.graph, path, graph, label)


def transform(query: str):
    """Turn a query into the pattern dicts the harvester works from.

    Parses the query, collects its patterns together with their operator
    scopes (including those inside FILTER EXISTS / NOT EXISTS), replaces
    blank nodes by fresh variables, and encodes every position
    canonically. A pattern that occurs more than once (for instance in
    two UNION branches) is collapsed into a single entry carrying every
    occurrence's scope, so that it is requested only once.

    Args:
        query: The SPARQL query.

    Returns:
        A list of pattern dicts. Empty if the query could not be parsed;
        the error is logged rather than raised.
    """
    with _parse_lock:
        try:
            algebra = translateQuery(parseQuery(query)).algebra
            walker = _ScopeWalker()
            walker.walk(algebra, ())
            raw = walker.patterns
        except Exception as e:
            logger.error("SPARQL parse error: %s", e)
            return []

    def enc(term):
        if term is None:
            return None
        if isinstance(term, (URIRef, Literal, Variable, BNode, str)):
            return term_to_pattern_str(term)
        return _PATH_SENTINEL

    index, bgp = {}, []
    for pat in raw:
        entry = {
            "subject":   enc(pat["subject"]),
            "predicate": enc(pat["predicate"]),
            "object":    enc(pat["object"]),
            "graph":     enc(pat["graph"]),
        }
        if "predicate_path" in pat:
            entry["predicate_path"] = pat["predicate_path"]
            predicate_key = f"{_PATH_SENTINEL}:{path_to_str(pat['predicate_path'])}"
        else:
            predicate_key = entry["predicate"]
        key = (entry["subject"], predicate_key, entry["object"], entry["graph"])
        if key in index:
            existing = bgp[index[key]]
            if pat["scope"] not in existing["scopes"]:
                existing["scopes"].append(pat["scope"])
                existing["operators"].append(pat["operator"])
            continue
        entry["scopes"] = [pat["scope"]]
        entry["operators"] = [pat["operator"]]
        index[key] = len(bgp)
        bgp.append(entry)

    logger.debug("Extracted %d patterns", len(bgp))
    return bgp


_parse_lock = threading.Lock()


# -------------------------------------------------------------------------
# PROPERTY PATHS
# -------------------------------------------------------------------------

def path_to_str(path) -> str:
    """Serialise a property path expression to a readable string."""
    if isinstance(path, URIRef):
        return str(path)
    if isinstance(path, MulPath):
        return f"({path_to_str(path.path)}){path.mod}"
    if isinstance(path, SequencePath):
        return "/".join(path_to_str(a) for a in path.args)
    if isinstance(path, AlternativePath):
        return "|".join(path_to_str(a) for a in path.args)
    if isinstance(path, InvPath):
        return f"^{path_to_str(path.arg)}"
    return repr(path)


def path_to_sparql(path):
    """Serialise a property path in SPARQL syntax, fully parenthesised.

    Returns:
        The expression, or None for a path that cannot be serialised (rdflib
        does not translate an inverse member of a negated property set).
    """
    if isinstance(path, URIRef):
        return f"<{path}>"
    if isinstance(path, MulPath):
        inner = path_to_sparql(path.path)
        return None if inner is None else f"({inner}){path.mod}"
    if isinstance(path, (SequencePath, AlternativePath)):
        parts = [path_to_sparql(a) for a in path.args]
        if any(p is None for p in parts):
            return None
        sep = "/" if isinstance(path, SequencePath) else "|"
        return "(" + sep.join(parts) + ")"
    if isinstance(path, InvPath):
        inner = path_to_sparql(path.arg)
        return None if inner is None else f"^({inner})"
    if isinstance(path, NegatedPath):
        parts = []
        for a in path.args:
            if isinstance(a, URIRef):
                parts.append(f"<{a}>")
            elif isinstance(a, InvPath) and isinstance(a.arg, URIRef):
                parts.append(f"^<{a.arg}>")
            else:
                return None
        return "!(" + "|".join(parts) + ")"
    return None


def path_is_nullable(path):
    """Return True if the path can match a zero-length route (x to x)."""
    if isinstance(path, URIRef):
        return False
    if isinstance(path, MulPath):
        return path.mod in ("*", "?") or path_is_nullable(path.path)
    if isinstance(path, SequencePath):
        return all(path_is_nullable(a) for a in path.args)
    if isinstance(path, AlternativePath):
        return any(path_is_nullable(a) for a in path.args)
    if isinstance(path, InvPath):
        return path_is_nullable(path.arg)
    return False


def is_all_variable(pat: dict) -> bool:
    """Return True if subject, predicate and object are all variables.

    Such a pattern selects every triple of a repository (or of a named graph,
    when it carries a graph IRI), whatever variables it uses.
    """
    return all(isinstance(pat.get(f), str) and pat[f].startswith("?")
               for f in ("subject", "predicate", "object"))


def is_path_pattern(pat: dict) -> bool:
    """Return True if a pattern's predicate is a property path expression."""
    return pat.get("predicate") == _PATH_SENTINEL


def synthetic_path_iri(path_obj) -> URIRef:
    """Build the reserved IRI a locally-evaluated path stores results under.

    Each pair of resources found by evaluating a path locally is stored in
    the working store as a triple with this predicate, so that binding
    queries can join with the path like with any harvested pattern. The
    serialised expression is percent-encoded, so two distinct paths get
    two distinct predicates. These triples are never published.
    """
    return URIRef("urn:tpf:path:" + quote(path_to_str(path_obj), safe=""))


def extract_base_iris_from_path(path) -> list:
    """Collect the concrete predicates a path expression references.

    Returns:
        The IRIs, in the order met, repeats included. Empty for a negated
        property set, whose extension cannot be enumerated from a fragments
        interface.
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
    return []


def path_support_patterns(pat, path_index):
    """Derive the patterns that must be harvested before a path can run.

    A fragments server will return the triples for a given predicate, but
    not the pairs of resources joined by an arbitrary-length chain of them,
    so the base predicates of a path are retrieved in full and the path is
    evaluated locally. Each support pattern uses fresh variables, so it
    cannot join with anything in the query by accident, and carries
    "derived_from_path" so provenance can tell it apart.
    """
    derived, seen = [], set()
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
            "scopes":    list(pat["scopes"]),
            "operators": list(pat["operators"]),
            "derived_from_path": path_to_str(pat["predicate_path"]),
        })
    return derived


def augment_bgp_with_path_support(bgp):
    """Add the support patterns every property path in a query needs.

    A support pattern is left out where the query already requests the
    same predicate, so nothing is fetched twice; in that case the path is
    evaluated over whatever that pattern retrieved, and is not used to
    restrict other patterns, since its local evaluation may be incomplete.
    """
    if not HARVEST_PATH_BASE_PREDICATES:
        if any(is_path_pattern(p) for p in bgp):
            logger.warning(
                "Property paths present but SCARAB_HARVEST_PATH_BASE_PREDICATES is "
                "disabled: a path over a predicate the query does not otherwise "
                "request will produce no local bindings.")
        return bgp

    existing = {p["predicate"] for p in bgp if not is_path_pattern(p)}
    augmented = list(bgp)
    for k, pat in enumerate(bgp):
        if not is_path_pattern(pat):
            continue
        for support in path_support_patterns(pat, k):
            if support["predicate"] in existing:
                continue
            existing.add(support["predicate"])
            augmented.append(support)
    return augmented


# -------------------------------------------------------------------------
# PATTERN HELPERS
# -------------------------------------------------------------------------

def extract_vars_from_pattern(pat, include_graph=True):
    """List the variable names a pattern uses, without their "?".

    Args:
        pat: A pattern dict.
        include_graph: Whether to include a variable in graph position.
            Restrictions never use it: harvested triples are stored in the
            source's assertion graph, not in the graph they came from.
    """
    fields = ("subject", "predicate", "object", "graph") if include_graph \
        else ("subject", "predicate", "object")
    out = []
    for field in fields:
        val = pat.get(field)
        if val and val != _PATH_SENTINEL and val.startswith("?"):
            out.append(val[1:])
    return out


def shares_variable(pat, processed_patterns):
    """Return True if a pattern shares a variable with any of the others."""
    mine = set(extract_vars_from_pattern(pat, include_graph=False))
    return any(mine & set(extract_vars_from_pattern(p, include_graph=False))
               for p in processed_patterns)


# -------------------------------------------------------------------------
# RESTRICTION PLANS
# -------------------------------------------------------------------------
# A restriction plan says which already-harvested patterns may restrict a
# pattern, and how the binding query that extracts the restricting values
# is to be built.
#
# Let D1 ... Dm be the OPTIONAL / MINUS / EXISTS boundaries that the
# restricted pattern t sits behind, outermost first, and C1 ... Cm the
# variables the required side of each always binds. A restricting pattern
# r lies at level L if it sits behind D1 ... DL together with t, and in
# front of D(L+1) ... Dm. Values may flow from level L-1 into level L only
# through C(L), so the binding query nests one subquery per boundary and
# projects each onto the corresponding C. Only restricting patterns
# connected to t through such variables are included, so no binding query
# contains a Cartesian product.

RestrictionPlan = namedtuple(
    "RestrictionPlan", ["members", "levels", "certs", "avail", "variables"]
)


def _divergence(r_scope, t_scope):
    """Index at which two scopes part, or None if r may not restrict t."""
    n = min(len(r_scope), len(t_scope))
    k = 0
    while k < n and r_scope[k] == t_scope[k]:
        k += 1
    if k == len(r_scope) == len(t_scope):
        return k                                     # same BGP
    if k == n:
        return None
    if (_kind(r_scope[k].role), _kind(t_scope[k].role)) not in _ALLOWED_PAIRS:
        return None
    if any(_kind(s.role) not in _REQUIRED_KINDS for s in r_scope[k + 1:]):
        return None                                  # r is not required
    if any(_kind(s.role) in _BARRIER_KINDS for s in t_scope[k:]):
        return None                                  # t is in a subquery
    return k


def restriction_plan(t_idx, bgp, harvested, restrictor_ok=None):
    """Work out how pattern `t_idx` may be restricted.

    Args:
        t_idx: Index of the pattern about to be harvested.
        bgp: All pattern dicts.
        harvested: Indices of the patterns already harvested.
        restrictor_ok: Optional predicate on an index, excluding patterns
            whose harvest cannot serve as a restriction (a property path
            whose local evaluation may be incomplete, for instance).

    Returns:
        A RestrictionPlan, or None if nothing may restrict the pattern.
        A pattern occurring more than once in the query, and a property
        path, are never restricted.
    """
    t = bgp[t_idx]
    if is_path_pattern(t) or len(t["scopes"]) != 1:
        return None
    t_scope = t["scopes"][0]
    crossings = [i for i, s in enumerate(t_scope) if _kind(s.role) in _DEPENDENT_KINDS]
    certs = [set(t_scope[i].cert or ()) for i in crossings]
    m = len(crossings)
    t_vars = set(extract_vars_from_pattern(t, include_graph=False))
    if not t_vars:
        return None

    candidates = {}
    for j in harvested:
        if j == t_idx or (restrictor_ok and not restrictor_ok(j)):
            continue
        best = None
        for r_scope in bgp[j]["scopes"]:
            k = _divergence(r_scope, t_scope)
            if k is None:
                continue
            level = sum(1 for c in crossings if c < k)
            best = level if best is None else max(best, level)
        if best is not None:
            candidates[j] = best

    if not candidates:
        return None

    def passable(l1, l2):
        lo, hi = min(l1, l2), max(l1, l2)
        if lo == hi:
            return None                  # same level: any shared variable
        allowed = set(certs[lo])
        for i in range(lo + 1, hi):
            allowed &= certs[i]
        return allowed

    var_cache = {j: set(extract_vars_from_pattern(bgp[j], include_graph=False))
                 for j in candidates}
    nodes = {"t": (m, t_vars)}
    changed = True
    while changed:
        changed = False
        for j, level in candidates.items():
            if j in nodes:
                continue
            for _, (other_level, other_vars) in list(nodes.items()):
                shared = var_cache[j] & other_vars
                gate = passable(level, other_level)
                if gate is not None:
                    shared &= gate
                if shared:
                    nodes[j] = (level, var_cache[j])
                    changed = True
                    break

    members = {j: lvl for j, (lvl, _) in nodes.items() if j != "t"}
    if not members:
        return None
    levels = [[] for _ in range(m + 1)]
    for j, lvl in members.items():
        levels[lvl].append(j)

    avail = []
    for level in range(m + 1):
        here = set().union(*(var_cache[j] for j in levels[level])) if levels[level] else set()
        if level > 0:
            here |= certs[level - 1] & avail[level - 1]
        avail.append(here)

    variables = sorted(t_vars & avail[m])
    if not variables:
        return None
    return RestrictionPlan(members, levels, certs, avail, variables)


def build_binding_query(plan, bgp, assertion_graph, paths_graph):
    """Render the SPARQL query extracting the bindings a plan describes.

    Args:
        plan: The RestrictionPlan.
        bgp: All pattern dicts.
        assertion_graph: IRI of the graph holding the harvested triples.
        paths_graph: IRI of the graph holding locally evaluated paths.
    """
    def clause(j):
        pat = bgp[j]
        s, o = sparql_term(pat["subject"]), sparql_term(pat["object"])
        if is_path_pattern(pat):
            return f"GRAPH <{paths_graph}> {{ {s} <{synthetic_path_iri(pat['predicate_path'])}> {o} . }}"
        return f"GRAPH <{assertion_graph}> {{ {s} {sparql_term(pat['predicate'])} {o} . }}"

    def body(level):
        parts = [clause(j) for j in sorted(plan.levels[level])]
        if level > 0:
            projected = sorted(plan.certs[level - 1] & plan.avail[level - 1])
            if projected:
                inner = body(level - 1)
                parts.append("{ SELECT DISTINCT " + " ".join("?" + v for v in projected)
                             + " WHERE {\n" + inner + "\n} }")
        return "\n".join(parts)

    top = len(plan.levels) - 1
    return ("SELECT DISTINCT " + " ".join("?" + v for v in plan.variables)
            + " WHERE {\n" + body(top) + "\n}")


def schedule_patterns(bgp, counts, restrictor_ok=None):
    """Order the simple patterns of a query for harvesting.

    The pattern with the smallest estimate goes first; at each later step
    the candidates are the patterns that what has been scheduled may
    restrict (per `restriction_plan`), and the smallest of them is taken.
    When none is restrictable, the smallest remaining pattern is taken.
    Property path patterns are left out; the harvest evaluates each one as
    soon as its support patterns are in.
    """
    remaining = [i for i, p in enumerate(bgp) if not is_path_pattern(p)]
    order = []
    while remaining:
        restrictable = [i for i in remaining
                        if order and restriction_plan(i, bgp, order, restrictor_ok)]
        pool = restrictable or remaining
        nxt = min(pool, key=lambda i: (counts.get(i, 0), i))
        order.append(nxt)
        remaining.remove(nxt)
    return order


# -------------------------------------------------------------------------
# STATISTICS
# -------------------------------------------------------------------------

class HarvestStats:
    """Counters for one source's harvest. Thread-safe."""

    _FIELDS = ("requests", "pages", "bytes", "cache_hits", "errors",
               "triples_received", "triples_ingested")

    def __init__(self, source_label):
        self.source = source_label
        self._lock = threading.Lock()
        for f in self._FIELDS:
            setattr(self, f, 0)
        self.patterns = []
        self.declared_incomplete = False
        self.triples_stored = 0
        self.seconds = 0.0
        self.phases = {}

    def add(self, **increments):
        with self._lock:
            for key, value in increments.items():
                setattr(self, key, getattr(self, key) + value)

    def phase(self, name, seconds):
        with self._lock:
            self.phases[name] = self.phases.get(name, 0.0) + seconds

    def as_dict(self):
        out = {"source": self.source}
        out.update({f: getattr(self, f) for f in self._FIELDS})
        out["triples_stored"] = self.triples_stored
        out["declared_incomplete"] = self.declared_incomplete
        out["seconds"] = round(self.seconds, 3)
        out["phases"] = {k: round(v, 3) for k, v in self.phases.items()}
        out["patterns"] = list(self.patterns)
        return out


_LAST_RUN_STATS = []


def last_run_stats():
    """Return per-source statistics of the last `FindBGPPriority` run.

    One dict per source, in source order, with the number of HTTP requests,
    fragment pages, bytes received, cache hits and errors; the triples
    received from the source, ingested into its working store, and stored
    in its assertion graph; the time spent; per pattern, how it was
    retrieved ("full", "full+filter", "bind", "empty", "path", "skipped" in
    strict mode, or "skipped-dump" for a forced dump that was not performed);
    and whether the harvest declared itself incomplete because of such a skip.
    """
    return [s.as_dict() for s in _LAST_RUN_STATS]


# -------------------------------------------------------------------------
# HTTP
# -------------------------------------------------------------------------
# One keep-alive session per thread: requests.Session is not guaranteed to
# be thread-safe, and a per-thread session still reuses its connections
# for every request that thread makes.

_thread_local = threading.local()


def _session():
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        retry = Retry(total=HTTP_RETRIES, backoff_factor=0.3,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=frozenset({"GET", "POST"}))
        adapter = HTTPAdapter(pool_connections=8, pool_maxsize=max(8, MAX_THREADS),
                              max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _thread_local.session = session
    return session


# -------------------------------------------------------------------------
# FRAGMENT PAGES
# -------------------------------------------------------------------------

Page = namedtuple("Page", ["data", "total", "per_page", "next_url", "ok"])

_FORMATS = {
    "application/trig": ox.RdfFormat.TRIG,
    "text/turtle": ox.RdfFormat.TURTLE,
    "application/n-triples": ox.RdfFormat.N_TRIPLES,
    "application/n-quads": ox.RdfFormat.N_QUADS,
    "application/rdf+xml": ox.RdfFormat.RDF_XML,
    "application/ld+json": ox.RdfFormat.JSON_LD,
    "text/n3": ox.RdfFormat.N3,
}
_ACCEPT = ("application/trig;q=1.0, application/n-quads;q=0.9, "
           "text/turtle;q=0.8, application/n-triples;q=0.7")


class _PageCache:
    """A bounded LRU cache of parsed fragment pages, cleared every run."""

    def __init__(self):
        self._data = OrderedDict()
        self._lock = threading.Lock()

    def get(self, url):
        with self._lock:
            page = self._data.get(url)
            if page is not None:
                self._data.move_to_end(url)
            return page

    def put(self, url, page):
        with self._lock:
            self._data[url] = page
            self._data.move_to_end(url)
            while len(self._data) > max(0, PAGE_CACHE_SIZE):
                self._data.popitem(last=False)

    def clear(self):
        with self._lock:
            self._data.clear()


page_cache = _PageCache()


def _is_control(quad, control_prefix):
    """Return True for a quad of fragment control metadata rather than data."""
    graph = quad.graph_name
    if isinstance(graph, ox.NamedNode) and graph.value.endswith("#metadata"):
        return True
    if quad.predicate.value.startswith(_CONTROL_NAMESPACES):
        return True
    subject = quad.subject
    return isinstance(subject, ox.NamedNode) and subject.value.startswith(control_prefix)


def _parse_body(body, content_type, base_iri):
    fmt = _FORMATS.get((content_type or "").split(";")[0].strip().lower())
    attempts = [fmt] if fmt else [ox.RdfFormat.TRIG, ox.RdfFormat.N_QUADS,
                                  ox.RdfFormat.TURTLE, ox.RdfFormat.RDF_XML]
    for f in attempts:
        try:
            return list(ox.parse(body, format=f, base_iri=base_iri))
        except Exception:
            continue
    raise ValueError(f"unparseable response ({content_type})")


_inflight = {}
_inflight_lock = threading.Lock()


def fetch_tpf_page(url, control_prefix=None, stats=None):
    """Fetch and parse one page of a fragment, once per URL at a time.

    Concurrent requests for the same URL (two patterns of a query with the
    same selector, counted at the same moment) are coalesced: one thread
    fetches and the others wait for its result. Besides saving a request,
    this matters for the Linked Data Fragments server, which answers a
    count requested while the same count is still being computed with a
    placeholder of 1e9 instead of the real number.
    """
    cached = page_cache.get(url)
    if cached is not None:
        if stats:
            stats.add(cache_hits=1)
        return cached
    with _inflight_lock:
        event = _inflight.get(url)
        owner = event is None
        if owner:
            event = threading.Event()
            _inflight[url] = event
    if not owner:
        event.wait(HTTP_TIMEOUT * (HTTP_RETRIES + 1) + 5)
        cached = page_cache.get(url)
        if cached is not None:
            if stats:
                stats.add(cache_hits=1)
            return cached
        return _fetch_tpf_page(url, control_prefix, stats)   # owner failed
    try:
        return _fetch_tpf_page(url, control_prefix, stats)
    finally:
        with _inflight_lock:
            _inflight.pop(url, None)
        event.set()


def _fetch_tpf_page(url, control_prefix=None, stats=None):
    """Fetch and parse one page of a fragment.

    Pages are cached by URL for the duration of a run (see
    SCARAB_PAGE_CACHE_SIZE), so a page counted for scheduling is not
    fetched again when it is harvested.

    Args:
        url: The page URL.
        control_prefix: IRI prefix of the server's own resources (its base
            URL); triples about them are control metadata, not data.
        stats: HarvestStats to update.

    Returns:
        A Page: the data quads, the fragment size and page size advertised
        by the control metadata (None where absent), the next page's URL,
        and whether the page could be fetched and parsed at all.
    """
    control_prefix = control_prefix or url.split("?", 1)[0]
    try:
        resp = _session().get(url, headers={"Accept": _ACCEPT}, timeout=HTTP_TIMEOUT)
        if stats:
            stats.add(requests=1, pages=1, bytes=len(resp.content))
        resp.raise_for_status()
        quads = _parse_body(resp.content, resp.headers.get("Content-Type"), url)
    except Exception as e:
        logger.warning("Fetch failed for %s: %s", url, e)
        if stats:
            stats.add(errors=1)
        return Page([], None, None, None, False)

    own_counts, other_counts = [], []
    per_page, next_url, data = None, None, []
    for q in quads:
        p = q.predicate.value
        if p in (_HYDRA_TOTAL, _VOID_TRIPLES):
            raw = re.sub(r"[,\u00b1~]", "", q.object.value.strip())
            if raw.isdigit():
                same = isinstance(q.subject, ox.NamedNode) and q.subject.value == url
                (own_counts if same else other_counts).append(int(raw))
        elif p in (_HYDRA_NEXT, _HYDRA_NEXT_PAGE):
            next_url = q.object.value
        elif p == _HYDRA_PER_PAGE:
            try:
                per_page = int(q.object.value)
            except ValueError:
                pass
        if not _is_control(q, control_prefix):
            data.append(q)

    total = own_counts[0] if own_counts else (min(other_counts) if other_counts else None)
    page = Page(data, total, per_page, next_url, True)
    page_cache.put(url, page)
    return page


def tpf_uri_request_builder(control_uri, subject, predicate, object_, graph=None):
    """Build the request URL for one pattern.

    Variables and unset positions are left out, so an all-variable pattern
    asks for the whole dataset and a fully-bound one asks whether a single
    triple is present. Including a graph makes the request a Quad Pattern
    Fragments request; leaving it out keeps it an ordinary TPF request.
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
        sep = "&" if "?" in control_uri else "?"
        return f"{control_uri}{sep}{urlencode(params)}"
    return control_uri


# -------------------------------------------------------------------------
# SELECTORS
# -------------------------------------------------------------------------

class Selector:
    """A pattern compiled for matching pyoxigraph triples.

    Constants are decoded once; a variable occurring in two positions
    (``?x ex:p ?x``) is enforced as well.
    """

    __slots__ = ("terms", "same")

    def __init__(self, pat):
        positions = (pat["subject"], pat["predicate"], pat["object"])
        self.terms = tuple(ox_from_pattern(p) for p in positions)
        seen, same = {}, []
        for i, p in enumerate(positions):
            if p and p.startswith("?"):
                if p in seen:
                    same.append((seen[p], i))
                else:
                    seen[p] = i
        self.same = tuple(same)

    def matches(self, s, p, o):
        t = (s, p, o)
        for want, got in zip(self.terms, t):
            if want is not None and want != got:
                return False
        for a, b in self.same:
            if t[a] != t[b]:
                return False
        return True


def concretize_pattern(pat, binding):
    """Substitute a binding (variable name -> pyoxigraph term) into a pattern."""
    def concretize(term):
        if term is None or not term.startswith("?"):
            return term
        value = binding.get(term[1:])
        return term if value is None else pattern_from_ox(value)

    bound = dict(pat)
    for field in ("subject", "predicate", "object"):
        bound[field] = concretize(pat[field])
    return bound


UNREACHABLE = 999_999_999


# -------------------------------------------------------------------------
# DATASOURCES (TPF/QPF, SPARQL endpoint, RDF dump)
# -------------------------------------------------------------------------
# A source answers three questions: how many triples it holds for a
# pattern, which triples those are, and which of them are compatible with
# a block of bindings. Everything above -- scheduling, restriction, the
# working store, publication and provenance -- is the same for all three.
# A SPARQL endpoint is never handed the query: it is asked one pattern at
# a time, exactly as a fragments server would be.

class DataSource:
    """What SCARAB needs from a repository, and nothing more.

    Attributes:
        kind: Short name of the source type, used in labels and provenance.
        location: Where the repository lives.
        bind_block_size: Bindings sent per bound request.
        full_page_weight: Cost of one page of a complete fragment relative
            to one bound request, in the choice between the two.
    """

    kind = "abstract"
    bind_block_size = 1

    def __init__(self, location):
        self.location = location
        self.full_page_weight = 1.0

    # -- provenance -------------------------------------------------------
    def identity_uri(self):
        """Return the IRI denoting this source in the provenance graph."""
        return URIRef(self.location)

    def provenance_type(self):
        """Return the (rdf:type, access property) describing this source."""
        return DCAT.DataService, DCAT.endpointURL

    def label(self):
        """Return a short human-readable description of the source."""
        return f"{self.kind}: {self.location}"

    # -- retrieval --------------------------------------------------------
    def page_size(self):
        """Triples retrieved per request when fetching a complete fragment."""
        return 1

    def count(self, pat, stats=None):
        """Estimate how many triples this source holds for a pattern."""
        raise NotImplementedError

    def fetch(self, pat, stats=None):
        """Yield every (s, p, o) the source holds for a pattern."""
        raise NotImplementedError

    def fetch_bound(self, pat, var_names, keys, stats=None):
        """Yield the triples matching a pattern under a block of bindings.

        Args:
            pat: The pattern dict.
            var_names: The restricted variables, in key order.
            keys: Tuples of pyoxigraph terms, one per binding.
        """
        for key in keys:
            yield from self.fetch(concretize_pattern(pat, dict(zip(var_names, key))), stats)

    def close(self):
        """Release resources held by the source."""


class TPFDataSource(DataSource):
    """A Triple or Quad Pattern Fragments server.

    The interface the rest of the design is written against. A pattern
    becomes a fragment selector, the size estimate comes from the
    fragment's control metadata, and the fragment is walked page by page.
    """

    kind = "tpf"

    def __init__(self, location):
        super().__init__(location)
        self._page_size = None
        self.full_page_weight = FULL_PAGE_WEIGHT

    def page_size(self):
        return self._page_size or 100

    def _url(self, pat):
        return tpf_uri_request_builder(self.location, pat["subject"], pat["predicate"],
                                       pat["object"], pat.get("graph"))

    def count(self, pat, stats=None):
        """Read the fragment's size estimate from its first page."""
        page = fetch_tpf_page(self._url(pat), self.location, stats)
        if not page.ok:
            return UNREACHABLE
        if page.per_page:
            self._page_size = page.per_page
        if page.total is not None:
            return page.total
        if page.next_url:
            return 10_000
        sel = Selector(pat)
        return sum(1 for q in page.data if sel.matches(q.subject, q.predicate, q.object))

    def fetch(self, pat, stats=None):
        """Walk the fragment for a pattern, yielding the triples that match.

        The walk stops when a page contributes nothing, when a page holds
        fewer triples than the advertised page size, when there is no next
        page, or when the next link repeats the current URL -- a guard
        against servers whose pagination fails to advance. Correctness
        depends on the server paginating deterministically.
        """
        sel = Selector(pat)
        url = self._url(pat)
        seen_urls = set()
        while url and url not in seen_urls:
            seen_urls.add(url)
            page = fetch_tpf_page(url, self.location, stats)
            if not page.ok:
                return
            if page.per_page:
                self._page_size = page.per_page
            n = 0
            for q in page.data:
                if sel.matches(q.subject, q.predicate, q.object):
                    n += 1
                    yield q.subject, q.predicate, q.object
            if stats:
                stats.add(triples_received=n)
            raw = len(page.data)
            if raw == 0 or (page.per_page and raw < page.per_page):
                return
            url = page.next_url


def get_pattern_count(control_uri, subject, predicate, object_, graph=None):
    """Estimate how many triples a fragments server holds for a pattern."""
    pat = {"subject": subject, "predicate": predicate, "object": object_, "graph": graph}
    return TPFDataSource(control_uri).count(pat)


class SPARQLDataSource(DataSource):
    """A remote SPARQL endpoint, asked one pattern at a time.

    Bound requests send a block of bindings in a VALUES clause
    (SCARAB_SPARQL_VALUES_BLOCK per request), instead of one request per
    binding. A pattern carrying a concrete graph IRI is scoped with GRAPH;
    one carrying none is matched against the default graph or any named
    graph, so content is found wherever the repository chose to put it.
    Blank nodes are skipped: one retrieved from a remote source has no
    identity outside the response that produced it.
    """

    kind = "sparql"

    def __init__(self, location):
        super().__init__(location)
        self.bind_block_size = max(1, SPARQL_VALUES_BLOCK)

    def page_size(self):
        return SPARQL_PAGE_EQUIV

    def _where_clause(self, pat, values=""):
        s, p, o = (sparql_term(pat[f]) for f in ("subject", "predicate", "object"))
        core = f"{s} {p} {o} ."
        graph = pat.get("graph")
        if graph is not None and not graph.startswith("?"):
            return f"{values} GRAPH {sparql_term(graph)} {{ {core} }}"
        return f"{values} {{ {core} }} UNION {{ GRAPH ?__g {{ {core} }} }}"

    def count(self, pat, stats=None):
        """Count the triples the endpoint holds for a pattern."""
        query = f"SELECT (COUNT(*) AS ?__count) WHERE {{ {self._where_clause(pat)} }}"
        rows = execute_sparql_query(query, endpoint=self.location, stats=stats)
        if not rows:
            return UNREACHABLE
        try:
            return int(rows[0].get("__count", 0))
        except (TypeError, ValueError):
            return UNREACHABLE

    def _select(self, pat, values, stats):
        positions = [pat[f] for f in ("subject", "predicate", "object")]
        variables = list(dict.fromkeys(p for p in positions if p.startswith("?")))
        projection = " ".join(variables) if variables else "*"
        query = f"SELECT DISTINCT {projection} WHERE {{ {self._where_clause(pat, values)} }}"
        rows = execute_sparql_query(query, include_types=True, endpoint=self.location,
                                    stats=stats)
        if not rows:
            return
        fixed = [None if p.startswith("?") else ox_from_pattern(p) for p in positions]
        n = 0
        for row in rows:
            try:
                terms = [ox_from_sparql_json(row[p[1:]]) if f is None else f
                         for p, f in zip(positions, fixed)]
            except KeyError:
                continue
            if isinstance(terms[0], ox.BlankNode) or isinstance(terms[2], ox.BlankNode):
                continue
            n += 1
            yield tuple(terms)
        if stats:
            stats.add(triples_received=n)

    def fetch(self, pat, stats=None):
        yield from self._select(pat, "", stats)

    def fetch_bound(self, pat, var_names, keys, stats=None):
        rows = " ".join("(" + " ".join(str(t) for t in key) + ")" for key in keys)
        values = "VALUES (" + " ".join("?" + v for v in var_names) + ") { " + rows + " }"
        yield from self._select(pat, values, stats)


_DUMP_FORMATS = {
    "turtle": ox.RdfFormat.TURTLE, "ttl": ox.RdfFormat.TURTLE,
    "nt": ox.RdfFormat.N_TRIPLES, "ntriples": ox.RdfFormat.N_TRIPLES,
    "nquads": ox.RdfFormat.N_QUADS, "nq": ox.RdfFormat.N_QUADS,
    "trig": ox.RdfFormat.TRIG, "xml": ox.RdfFormat.RDF_XML,
    "rdf": ox.RdfFormat.RDF_XML, "owl": ox.RdfFormat.RDF_XML,
    "n3": ox.RdfFormat.N3, "json-ld": ox.RdfFormat.JSON_LD,
    "jsonld": ox.RdfFormat.JSON_LD,
}


class DumpDataSource(DataSource):
    """A local (or downloadable) RDF file.

    Loaded once, on first use, into an on-disk pyoxigraph store, so that a
    pattern is answered by an index lookup rather than a scan, and a large
    file does not have to fit in memory. A quad-based serialisation keeps
    its named graphs and can be addressed by a pattern carrying a graph
    term, exactly as the QPF interface allows.
    """

    kind = "dump"

    def __init__(self, location, rdf_format=None):
        super().__init__(location)
        self.rdf_format = rdf_format
        self._store = None
        self._dir = None
        self._load_lock = threading.Lock()

    def identity_uri(self):
        try:
            return URIRef(_FsPath(self.location).resolve().as_uri())
        except Exception:
            return URIRef(str(self.location))

    def provenance_type(self):
        """Describe a dump as a dataset (a distribution), not a service."""
        return DCAT.Dataset, DCAT.downloadURL

    def _format(self, path):
        if self.rdf_format:
            return _DUMP_FORMATS.get(self.rdf_format.lower())
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        return _DUMP_FORMATS.get(ext) or ox.RdfFormat.from_extension(ext)

    def _ensure_loaded(self):
        with self._load_lock:
            if self._store is not None:
                return self._store
            self._dir = tempfile.mkdtemp(prefix="scarab-dump-", dir=WORKING_STORE_DIR
                                         if WORKING_STORE_DIR not in (None, "memory") else None)
            _register_temp(self._dir)
            store = ox.Store(self._dir)
            location = str(self.location)
            path = location[7:] if location.startswith("file://") else location
            try:
                if location.startswith(("http://", "https://")):
                    resp = _session().get(location, timeout=HTTP_TIMEOUT)
                    resp.raise_for_status()
                    path = os.path.join(self._dir, "download." + location.rsplit(".", 1)[-1])
                    with open(path, "wb") as fh:
                        fh.write(resp.content)
                store.bulk_load(path=path, format=self._format(path))
                logger.info("[DUMP] Loaded %d quads from %s", len(store), self.location)
            except Exception as e:
                logger.error("[DUMP] Could not load %s: %s", self.location, e)
            self._store = store
            return store

    def _graph(self, pat):
        graph = pat.get("graph")
        if graph is not None and not graph.startswith("?"):
            return ox_from_pattern(graph)
        return None

    def count(self, pat, stats=None):
        store = self._ensure_loaded()
        s, p, o = (sparql_term(pat[f]) for f in ("subject", "predicate", "object"))
        graph = self._graph(pat)
        where = (f"GRAPH {graph} {{ {s} {p} {o} }}" if graph is not None
                 else f"{{ {s} {p} {o} }} UNION {{ GRAPH ?__g {{ {s} {p} {o} }} }}")
        try:
            result = store.query(f"SELECT (COUNT(*) AS ?c) WHERE {{ {where} }}")
            return int(next(iter(result))["c"].value)
        except Exception:
            return sum(1 for _ in self.fetch(pat))

    def fetch(self, pat, stats=None):
        store = self._ensure_loaded()
        sel = Selector(pat)
        n = 0
        for q in store.quads_for_pattern(*sel.terms, self._graph(pat)):
            if isinstance(q.subject, ox.BlankNode) or isinstance(q.object, ox.BlankNode):
                continue
            if sel.same and not sel.matches(q.subject, q.predicate, q.object):
                continue
            n += 1
            yield q.subject, q.predicate, q.object
        if stats:
            stats.add(triples_received=n)

    def close(self):
        self._store = None
        if self._dir:
            _remove_temp(self._dir)
            self._dir = None


_DUMP_EXTENSIONS = (
    ".ttl", ".turtle", ".nt", ".ntriples", ".nq", ".nquads",
    ".trig", ".rdf", ".owl", ".n3", ".jsonld", ".json-ld", ".xml",
)


def make_datasource(spec):
    """Build a DataSource from a caller's description of a repository.

    Args:
        spec: One of
            - a DataSource, returned unchanged
            - {"type": "tpf"|"sparql"|"dump", "location": ..., "format": ...}
            - "tpf:<url>" or "qpf:<url>", a fragments server
            - "sparql:<url>", a SPARQL endpoint
            - "dump:<path>", an RDF file
            - a bare path or URL, inferred

    Note:
        Inference is deliberately cautious. Anything with a recognised RDF
        extension, a file:// URL, or a path that exists on disk is taken
        for a dump; anything else http(s) for a fragments server. A SPARQL
        endpoint therefore has to declare itself.
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
    for prefix, cls in (("tpf:", TPFDataSource), ("qpf:", TPFDataSource),
                        ("sparql:", SPARQLDataSource), ("dump:", DumpDataSource)):
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
# WORKING STORE
# -------------------------------------------------------------------------

class WorkingStore:
    """The embedded store a single source is harvested into.

    On disk by default (SCARAB_WORKING_STORE_DIR, or the system temporary
    directory), so that its memory footprint is bounded by pyoxigraph's
    caches rather than by the size of the harvest; "memory" keeps it in
    RAM. It holds the assertion graph, the synthetic triples of locally
    evaluated property paths (in a separate graph that is never published),
    and, once the harvest is complete, the other three nanopublication
    graphs.
    """

    def __init__(self, nanopub_base):
        if WORKING_STORE_DIR == "memory":
            self.path = None
            self.store = ox.Store()
        else:
            self.path = tempfile.mkdtemp(prefix="scarab-", dir=WORKING_STORE_DIR)
            if not KEEP_WORKING_STORES:
                _register_temp(self.path)
            self.store = ox.Store(self.path)
        self.base = nanopub_base
        self.assertion = ox.NamedNode(f"{nanopub_base}#assertion")
        self.paths = ox.NamedNode(f"{nanopub_base}#paths")

    def add(self, triples, graph=None):
        graph = graph or self.assertion
        quads = [ox.Quad(s, p, o, graph) for s, p, o in triples]
        if quads:
            self.store.extend(quads)
        return len(quads)

    def select(self, query):
        result = self.store.query(query)
        names = [v.value for v in result.variables]
        return [{n: sol[n] for n in names if sol[n] is not None} for sol in result]

    def size(self, graph=None):
        graph = graph or self.assertion
        result = self.store.query(f"SELECT (COUNT(*) AS ?c) WHERE {{ GRAPH {graph} {{ ?s ?p ?o }} }}")
        return int(next(iter(result))["c"].value)

    def nanopub_graphs(self):
        return [ox.NamedNode(f"{self.base}{suffix}")
                for suffix in ("#assertion", "#Head", "#provenance", "#pubinfo")]

    def quads(self, graphs):
        for graph in graphs:
            yield from self.store.quads_for_pattern(None, None, None, graph)

    def close(self):
        self.store = None
        if self.path and not KEEP_WORKING_STORES:
            _remove_temp(self.path)


# -------------------------------------------------------------------------
# HARVESTING ONE SOURCE
# -------------------------------------------------------------------------

def _key_positions(pat, var_names):
    """Map each restricted variable to the first position it occupies."""
    fields = ("subject", "predicate", "object")
    return [next(i for i, f in enumerate(fields) if pat[f] == "?" + v) for v in var_names]


class _SourceHarvest:
    """State and steps of harvesting one source for one query."""

    def __init__(self, source, bgp, nanopub_base, strict, stats):
        self.source = source
        self.bgp = bgp
        self.strict = strict
        self.stats = stats
        self.ws = WorkingStore(nanopub_base)
        self.counts = {}
        self.harvested = []          # indices, in order, usable as restrictors
        self.complete_predicates = set()
        self.ready_paths = set()     # path patterns usable as restrictors
        self.skipped_dumps = []      # all-variable patterns not retrieved

    # -- restrictions -----------------------------------------------------
    def _restrictor_ok(self, j):
        if j in self.skipped_dumps:
            return False             # its contents at this source are unknown
        pat = self.bgp[j]
        if not is_path_pattern(pat):
            return True
        return j in self.ready_paths

    def _bindings(self, idx):
        plan = restriction_plan(idx, self.bgp, self.harvested, self._restrictor_ok)
        if plan is None:
            return None, None
        query = build_binding_query(plan, self.bgp, self.ws.assertion.value, self.ws.paths.value)
        t0 = time.perf_counter()
        rows = self.ws.select(query)
        self.stats.phase("binding_queries", time.perf_counter() - t0)
        keys = {tuple(row.get(v) for v in plan.variables) for row in rows}
        keys = {k for k in keys if all(t is not None for t in k)}
        # A value that cannot occupy the position it would be substituted
        # into (a literal as subject, anything but an IRI as predicate) can
        # match nothing: such bindings are dropped before any request.
        pat = self.bgp[idx]
        subj = [i for i, v in enumerate(plan.variables) if pat["subject"] == "?" + v]
        pred = [i for i, v in enumerate(plan.variables) if pat["predicate"] == "?" + v]
        if subj or pred:
            before = len(keys)
            keys = {k for k in keys
                    if not any(isinstance(k[i], ox.Literal) for i in subj)
                    and all(isinstance(k[i], ox.NamedNode) for i in pred)}
            if before != len(keys):
                logger.debug("Dropped %d impossible bindings", before - len(keys))
            if before and not keys:
                return plan, None    # restricted, and nothing can match
        return plan, keys

    # -- retrieval --------------------------------------------------------
    def _ingest(self, triples, batch=20_000):
        buffer, n = [], 0
        for triple in triples:
            buffer.append(triple)
            if len(buffer) >= batch:
                n += self.ws.add(buffer)
                buffer = []
        n += self.ws.add(buffer)
        self.stats.add(triples_ingested=n)
        return n

    def _full(self, pat, keys=None, var_names=None):
        triples = self.source.fetch(pat, self.stats)
        if keys is not None:
            positions = _key_positions(pat, var_names)
            triples = (t for t in triples if tuple(t[i] for i in positions) in keys)
        return self._ingest(triples)

    def _bind(self, pat, keys, var_names, pool):
        block = max(1, self.source.bind_block_size)
        keys = list(keys)
        blocks = [keys[i:i + block] for i in range(0, len(keys), block)]
        in_flight, done, limit = set(), 0, max(1, MAX_THREADS) * MAX_IN_FLIGHT_FACTOR

        def task(chunk):
            return self.ws.add(list(self.source.fetch_bound(pat, var_names, chunk, self.stats)))

        def collect(finished):
            nonlocal done
            for f in finished:
                try:
                    done += f.result()
                except Exception as e:
                    self.stats.add(errors=1)
                    logger.warning("Bound request failed for %s: %s", self.source.location, e)

        for chunk in blocks:
            if len(in_flight) >= limit:
                finished, in_flight = wait(in_flight, return_when=FIRST_COMPLETED)
                collect(finished)
            in_flight.add(pool.submit(task, chunk))
        collect(in_flight)
        self.stats.add(triples_ingested=done)
        return done, len(blocks)

    def harvest_pattern(self, idx, pool):
        pat = self.bgp[idx]
        count = self.counts.get(idx, 0)
        record = {"index": idx, "pattern": _pattern_label(pat), "estimate": count}
        plan, keys = self._bindings(idx)
        requests_before = self.stats.requests

        if plan is not None and keys is None:
            # Every binding was impossible for this pattern's positions: the
            # restricted fragment is empty without asking.
            record.update(decision="empty", triples=0, requests=0)
            self.stats.patterns.append(record)
            return

        if plan is not None and not keys:
            if self.strict:
                record["decision"] = "skipped"
                self.stats.patterns.append(record)
                return
            if SKIP_FORCED_DUMPS and is_all_variable(pat) and count != 0:
                # Retrieving it in full would retrieve the whole repository.
                record.update(decision="skipped-dump", triples=0, requests=0)
                self.stats.patterns.append(record)
                self.skipped_dumps.append(idx)
                self.stats.declared_incomplete = True
                logger.warning("  pattern %d %s: forced dump of %s not performed "
                               "(declared incomplete)", idx, record["pattern"],
                               self.source.location)
                return
            plan = None             # nothing to restrict with: retrieve in full

        if count == 0:
            decision, n = "empty", 0
        elif plan is None:
            decision, n = "full", self._full(pat)
        else:
            var_names = plan.variables
            unbindable = any(is_unbindable(t) for key in keys for t in key)
            pages = math.ceil(count / max(1, self.source.page_size()))
            full_cost = pages * self.source.full_page_weight
            bind_cost = math.ceil(len(keys) / max(1, self.source.bind_block_size))
            record["bindings"] = len(keys)
            if unbindable or pages <= 1 or full_cost < bind_cost:
                decision, n = "full+filter", self._full(pat, keys, var_names)
            else:
                n, blocks = self._bind(pat, keys, var_names, pool)
                decision = "bind"

        record.update(decision=decision, triples=n,
                      requests=self.stats.requests - requests_before)
        self.stats.patterns.append(record)
        logger.info("  pattern %d %s: %s, %d triples, %d requests",
                    idx, record["pattern"], decision, n, record["requests"])

        graph = pat.get("graph")
        if (decision == "full" and pat["subject"].startswith("?")
                and pat["object"].startswith("?") and pat["subject"] != pat["object"]
                and not pat["predicate"].startswith("?")
                and (graph is None or graph.startswith("?"))):
            self.complete_predicates.add(pat["predicate"])

    def evaluate_path(self, idx):
        """Evaluate a property path locally and store its pairs.

        The path is evaluated over the triples harvested into the assertion
        graph. Its pairs are stored under a synthetic predicate in the
        working store's path graph, never in the published graphs. The path
        may afterwards restrict other patterns only if every predicate it
        references was retrieved completely and it cannot match a zero-length
        route; otherwise its local evaluation may be incomplete.
        """
        pat = self.bgp[idx]
        path = pat["predicate_path"]
        expr = path_to_sparql(path)
        record = {"index": idx, "pattern": _pattern_label(pat), "decision": "path"}
        if expr is None:
            record["triples"] = 0
            self.stats.patterns.append(record)
            return
        s, o = pat["subject"], pat["object"]
        s_sql, o_sql = sparql_term(s), sparql_term(o)
        graph = self.ws.assertion
        syn = ox.NamedNode(str(synthetic_path_iri(path)))
        triples = []
        if s.startswith("?") or o.startswith("?"):
            names = [t[1:] for t in (s, o) if t.startswith("?")]
            query = (f"SELECT DISTINCT {' '.join('?' + n for n in names)} WHERE "
                     f"{{ GRAPH {graph} {{ {s_sql} {expr} {o_sql} }} }}")
            for row in self.ws.select(query):
                sv = row.get(s[1:]) if s.startswith("?") else ox_from_pattern(s)
                ov = row.get(o[1:]) if o.startswith("?") else ox_from_pattern(o)
                if sv is not None and ov is not None and not isinstance(sv, ox.Literal):
                    triples.append((sv, syn, ov))
        elif bool(self.ws.store.query(f"ASK {{ GRAPH {graph} {{ {s_sql} {expr} {o_sql} }} }}")):
            triples.append((ox_from_pattern(s), syn, ox_from_pattern(o)))
        self.ws.add(triples, graph=self.ws.paths)
        record["triples"] = len(triples)
        self.stats.patterns.append(record)
        base = {str(i) for i in extract_base_iris_from_path(path)}
        if base and base <= self.complete_predicates and not path_is_nullable(path):
            self.ready_paths.add(idx)

    def _paths_ready_now(self, pending_paths):
        """Path patterns whose base predicates have all been harvested.

        A base predicate is provided by the path's support pattern, or by a
        pattern of the query with the same constant predicate when the
        support pattern was left out for that reason.
        """
        ready = []
        done = set(self.harvested)
        for idx in pending_paths:
            base = {str(i) for i in extract_base_iris_from_path(self.bgp[idx]["predicate_path"])}
            providers = [j for j, p in enumerate(self.bgp)
                         if not is_path_pattern(p) and p["predicate"] in base]
            if all(j in done for j in providers):
                ready.append(idx)
        return ready

    def run(self):
        """Harvest every pattern of the query from this source."""
        t_start = time.perf_counter()
        with ThreadPoolExecutor(max(1, MAX_THREADS)) as pool:
            simple = [i for i, p in enumerate(self.bgp) if not is_path_pattern(p)]
            t0 = time.perf_counter()
            for i, n in zip(simple, pool.map(lambda i: self.source.count(self.bgp[i], self.stats), simple)):
                self.counts[i] = n
            self.stats.phase("counts", time.perf_counter() - t0)

            if simple and all(self.counts[i] == UNREACHABLE for i in simple):
                logger.warning("Source %s unreachable; nothing harvested", self.source.location)
                for i in simple:
                    self.stats.patterns.append({"index": i, "pattern": _pattern_label(self.bgp[i]),
                                                "decision": "unreachable"})
                self.stats.seconds = time.perf_counter() - t_start
                return

            order = schedule_patterns(self.bgp, self.counts)
            pending_paths = [i for i, p in enumerate(self.bgp) if is_path_pattern(p)]
            logger.info("Harvesting %s, order %s", self.source.label(), order)
            t0 = time.perf_counter()
            for ready in self._paths_ready_now(pending_paths):
                self.evaluate_path(ready)
                self.harvested.append(ready)
                pending_paths.remove(ready)
            for idx in order:
                self.harvest_pattern(idx, pool)
                self.harvested.append(idx)
                for ready in self._paths_ready_now(pending_paths):
                    self.evaluate_path(ready)
                    self.harvested.append(ready)
                    pending_paths.remove(ready)
            for idx in pending_paths:
                self.evaluate_path(idx)
            self.stats.phase("retrieval", time.perf_counter() - t0)
        self.stats.triples_stored = self.ws.size()
        self.stats.seconds = time.perf_counter() - t_start


def _pattern_label(pat):
    if is_path_pattern(pat):
        return f"{pat['subject']} {path_to_str(pat['predicate_path'])} {pat['object']}"
    return f"{pat['subject']} {pat['predicate']} {pat['object']}"


# -------------------------------------------------------------------------
# NANOPUBLICATIONS
# -------------------------------------------------------------------------

# Terms SCARAB uses in provenance that no standard vocabulary provides.
SCARAB_VOCAB = Namespace(f"{SCARAB_CODEBASE_URI}/vocabulary#")


def nanopub_quads(nanopub_base, source, bgp, started_at, ended_at, skipped=()):
    """Build the head, provenance and publication-information graphs.

    A nanopublication is four named graphs sharing a base IRI: a head naming
    the other three, an assertion holding the content, a provenance
    describing how the assertion came about, and a pubinfo holding
    administrative metadata. The assertion graph is the graph the harvested
    triples were written into, so nothing is copied.

    When patterns were not retrieved because they were forced dumps
    (`skipped`, their indices), the provenance graph says so: each such
    pattern is linked to the source with scarab:notRetrievedFrom, with the
    reason, and the assertion graph is marked scarab:declaredIncomplete.

    Returns:
        (subject, predicate, object, graph) tuples of rdflib terms.
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

    quads = [
        (this, RDF.type, NP.Nanopublication, head_g),
        (this, NP.hasAssertion, assertion_g, head_g),
        (this, NP.hasProvenance, prov_g, head_g),
        (this, NP.hasPublicationInfo, pubinfo_g, head_g),
        (assertion_g, RDF.type, DCAT.Dataset, prov_g),
        (assertion_g, PROV.wasGeneratedBy, activity, prov_g),
        (assertion_g, PROV.wasDerivedFrom, endpoint, prov_g),
        (endpoint, RDF.type, source_type, prov_g),
        (endpoint, access_property, endpoint, prov_g),
        (activity, RDF.type, PROV.Activity, prov_g),
        (activity, RDFS.label, Literal(f"SCARAB {source.kind} bind-join harvest"), prov_g),
        (activity, PROV.wasAssociatedWith, instance, prov_g),
        (activity, PROV.used, endpoint, prov_g),
        (activity, PROV.startedAtTime, Literal(started_at, datatype=XSD.dateTime), prov_g),
        (activity, PROV.endedAtTime, Literal(ended_at, datatype=XSD.dateTime), prov_g),
    ]
    for i, pat in enumerate(bgp):
        pat_node = URIRef(f"{nanopub_base}#pattern{i}")
        comment = _pattern_label(pat)
        derived = pat.get("derived_from_path")
        if derived:
            label = f"Support pattern {i} (property path base predicate)"
            comment = f"{comment}  [derived from property path: {derived}]"
        else:
            label = f"Triple pattern {i}"
        quads += [
            (activity, PROV.used, pat_node, prov_g),
            (pat_node, RDF.type, PROV.Entity, prov_g),
            (pat_node, RDFS.label, Literal(label), prov_g),
            (pat_node, RDFS.comment, Literal(comment), prov_g),
        ]
        if i in skipped:
            quads += [
                (pat_node, SCARAB_VOCAB.notRetrievedFrom, endpoint, prov_g),
                (pat_node, SCARAB_VOCAB.skipReason, Literal(
                    "forced dump: every position is a variable and the patterns that "
                    "may restrict it yielded no bindings at this source, so retrieving "
                    "it would have retrieved the whole repository"), prov_g),
            ]
    if skipped:
        quads.append((assertion_g, SCARAB_VOCAB.declaredIncomplete,
                      Literal(True, datatype=XSD.boolean), prov_g))
    quads += [
        (SCARAB_CODEBASE_URI, RDF.type, PROV.SoftwareAgent, prov_g),
        (SCARAB_CODEBASE_URI, RDFS.label, Literal("SCARAB TPF Federated Harvester"), prov_g),
        (SCARAB_CODEBASE_URI, SCHEMA.softwareVersion, Literal(SCARAB_VERSION), prov_g),
        (SCARAB_CODEBASE_URI, DCAT.downloadURL, SCARAB_DOWNLOAD_URI, prov_g),
        (instance, RDF.type, PROV.Agent, prov_g),
        (instance, RDFS.label, Literal("SCARAB instance"), prov_g),
        (instance, PROV.wasAttributedTo, SCARAB_CODEBASE_URI, prov_g),
        (instance, SCHEMA.softwareVersion, Literal(SCARAB_VERSION), prov_g),
        (this, DCT.created, Literal(ended_at, datatype=XSD.dateTime), pubinfo_g),
        (this, DCT.creator, instance, pubinfo_g),
        (this, NPX.hasNanopubType, NPX.ProvenanceRecord, pubinfo_g),
    ]
    return quads


def write_nanopub_graphs(working_store, source, bgp, started_at, ended_at, skipped=()):
    """Add the provenance wrapper of a finished harvest to its working store."""
    quads = [ox.Quad(*(ox_from_rdflib(t) for t in q))
             for q in nanopub_quads(working_store.base, source, bgp, started_at, ended_at,
                                    skipped)]
    working_store.store.extend(quads)


# -------------------------------------------------------------------------
# PUBLICATION
# -------------------------------------------------------------------------

_local_store_lock = threading.Lock()
_LOCAL_STORE = None


def local_store():
    """Return the embedded store used when SCARAB_STORE_KIND is "local"."""
    global _LOCAL_STORE
    with _local_store_lock:
        if _LOCAL_STORE is None:
            if LOCAL_STORE_PATH in (None, "", "memory"):
                _LOCAL_STORE = ox.Store()
            else:
                _LOCAL_STORE = ox.Store(LOCAL_STORE_PATH)
        return _LOCAL_STORE


def _nquad_line(q):
    return f"{q.subject} {q.predicate} {q.object} {q.graph_name} .\n"


def _post_nquads(lines):
    payload = "".join(lines).encode("utf-8")
    r = _session().post(STORE_STATEMENTS_URL, data=payload,
                        headers={"Content-Type": "application/n-quads"},
                        timeout=max(HTTP_TIMEOUT, 300))
    if r.status_code not in (200, 201, 204):
        raise RuntimeError(f"store rejected {len(lines)} quads: {r.status_code} {r.text[:200]}")


def publish_harvest(working_store):
    """Publish the four nanopublication graphs of a finished harvest.

    To the remote triplestore (in batches of SCARAB_PUBLISH_BATCH quads;
    the head, provenance and publication information graphs go last, in a
    single request, so that a nanopublication never describes an assertion
    graph that is not complete yet) or to the local store, depending on
    SCARAB_STORE_KIND, and additionally to a TriG file in SCARAB_EXPORT_DIR
    if that is set. The path graph is never published.

    Returns:
        The path of the exported file, or None.
    """
    graphs = working_store.nanopub_graphs()
    if STORE_KIND == "local":
        local_store().extend(list(working_store.quads(graphs)))
    elif STORE_KIND == "remote":
        batch = []
        for q in working_store.quads(graphs[:1]):
            batch.append(_nquad_line(q))
            if len(batch) >= PUBLISH_BATCH:
                _post_nquads(batch)
                batch = []
        if batch:
            _post_nquads(batch)
        _post_nquads([_nquad_line(q) for q in working_store.quads(graphs[1:])])
    exported = None
    if EXPORT_DIR:
        os.makedirs(EXPORT_DIR, exist_ok=True)
        slug = working_store.base.rsplit(":", 1)[-1]
        exported = os.path.join(EXPORT_DIR, f"{slug}.trig")
        ox.serialize(input=working_store.quads(graphs), output=exported, format=ox.RdfFormat.TRIG)
    return exported


# -------------------------------------------------------------------------
# HARVEST EXECUTION
# -------------------------------------------------------------------------

def harvest_endpoint_optimized(source, bgp, nanopub_base, *, strict=None,
                               publish=True, keep_store=False, stats=None):
    """Harvest every pattern of a query from one source.

    Patterns are counted concurrently, ordered (see `schedule_patterns`)
    and requested one after the other. Each pattern is requested in full
    when nothing may restrict it; otherwise its restricting values are
    read from the working store with the query `build_binding_query`
    renders, and the cheaper of two strategies is used: one request per
    binding (or per block of bindings, for a SPARQL endpoint), or the
    complete fragment filtered locally against the bindings. Both keep
    exactly the same triples. A property path is evaluated locally as soon
    as its support patterns are in.

    Args:
        source: The DataSource, or anything `make_datasource` accepts.
        bgp: The query's pattern dicts, from `transform`.
        nanopub_base: Base IRI for this run and source.
        strict: Skip, rather than retrieve in full, a pattern whose
            restricting patterns yield no bindings. Defaults to
            INDEXING_MODE.
        publish: Publish the nanopublication when done.
        keep_store: Return the working store instead of closing it.
        stats: A HarvestStats to fill in; one is created if omitted.

    Returns:
        The working store if `keep_store`, else None.
    """
    source = make_datasource(source)
    stats = stats or HarvestStats(source.label())
    harvest = _SourceHarvest(source, bgp, nanopub_base,
                             INDEXING_MODE if strict is None else strict, stats)
    started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    try:
        harvest.run()
        ended_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        write_nanopub_graphs(harvest.ws, source, bgp, started_at, ended_at,
                             skipped=harvest.skipped_dumps)
        if publish:
            t0 = time.perf_counter()
            exported = publish_harvest(harvest.ws)
            stats.phase("publication", time.perf_counter() - t0)
            if exported:
                logger.info("Exported %s", exported)
    except Exception:
        harvest.ws.close()
        raise
    if keep_store:
        return harvest.ws
    harvest.ws.close()
    return None


def _prepare(query):
    bgp = transform(query)
    if not bgp:
        return []
    return augment_bgp_with_path_support(bgp)


def FindBGPPriority(query, endpoints, base_named_graph=None):
    """Harvest a query's data from every source.

    Each source is harvested in isolation, into its own working store and
    with bindings read from its own contribution only, and its
    nanopublication is published when it is done. Since the harvests are
    independent, up to SCARAB_PARALLEL_SOURCES of them run concurrently.

    The query itself is never rewritten. The decomposition governs
    retrieval only; the query's semantics are applied afterwards by the
    store, over everything harvested.

    Args:
        query: The SPARQL query.
        endpoints: The sources (see `make_datasource`); a single source may
            be given on its own.
        base_named_graph: Identifier for this run, from which the
            assertion graph IRIs are derived. A fresh one by default.

    Returns:
        The run's assertion graph IRIs, one per source in the order the
        sources were given. Pass them to `evaluate_over_run`.
    """
    global _LAST_RUN_STATS
    if isinstance(endpoints, (str, dict, DataSource)):
        endpoints = [endpoints]
    sources = [make_datasource(spec) for spec in endpoints]
    bgp = _prepare(query)
    if not bgp:
        logger.warning("No triple patterns extracted from query")
        return []
    page_cache.clear()
    run_id = base_named_graph or f"urn:tpf:run:{_uuid_mod.uuid4()}"
    stats = [HarvestStats(s.label()) for s in sources]
    _LAST_RUN_STATS = stats

    def one(i):
        harvest_endpoint_optimized(sources[i], bgp, mint_nanopub_uri(run_id, i + 1),
                                   stats=stats[i])

    workers = max(1, min(PARALLEL_SOURCES, len(sources)))
    if workers == 1:
        for i in range(len(sources)):
            one(i)
    else:
        with ThreadPoolExecutor(workers) as pool:
            for f in [pool.submit(one, i) for i in range(len(sources))]:
                f.result()
    for s in stats:
        logger.info("%s: %d requests, %d triples stored, %.2fs",
                    s.source, s.requests, s.triples_stored, s.seconds)
    return run_assertion_graphs(run_id, len(sources))


# -------------------------------------------------------------------------
# QUERYING
# -------------------------------------------------------------------------

def execute_sparql_query(query, include_types=False, endpoint=None,
                         default_graphs=None, named_graphs=None, stats=None):
    """Run a SPARQL query over HTTP and return its solutions.

    Args:
        query: The query.
        include_types: False flattens each binding to its plain string
            value; True keeps the SPARQL-JSON dict, so a caller can tell an
            IRI from a blank node or a literal.
        endpoint: Where to send it. Defaults to the remote store.
        default_graphs: Graph IRIs whose merge forms the default graph,
            sent as `default-graph-uri` parameters.
        named_graphs: Graph IRIs available to GRAPH clauses, sent as
            `named-graph-uri`; defaults to `default_graphs`.
        stats: HarvestStats to update.

    Returns:
        A list of solutions (a single {"boolean": ...} dict for ASK), or
        None if the query failed.
    """
    endpoint = endpoint or STORE_QUERY_URL
    params = []
    if default_graphs is not None:
        if named_graphs is None:
            named_graphs = default_graphs
        params += [("default-graph-uri", g) for g in default_graphs]
        params += [("named-graph-uri", g) for g in named_graphs]
    try:
        r = _session().post(endpoint, params=params or None, data=query.encode("utf-8"),
                            headers={"Content-Type": "application/sparql-query",
                                     "Accept": "application/sparql-results+json"},
                            timeout=HTTP_TIMEOUT)
        if stats:
            stats.add(requests=1, bytes=len(r.content))
        if r.status_code != 200:
            logger.warning("Query failed at %s: %s", endpoint, r.text[:300])
            if stats:
                stats.add(errors=1)
            return None
        data = r.json()
        if "boolean" in data:
            return [{"boolean": data["boolean"]}]
        return [{var: (val if include_types else val["value"]) for var, val in row.items()}
                for row in data["results"]["bindings"]]
    except Exception as e:
        logger.warning("SPARQL execution error at %s: %s", endpoint, e)
        if stats:
            stats.add(errors=1)
        return None


def _query_ox_store(store, query, graphs, include_types):
    graphs = [ox.NamedNode(g) for g in graphs]
    result = store.query(query, default_graph=graphs, named_graphs=graphs)
    if isinstance(result, ox.QueryBoolean):
        return [{"boolean": bool(result)}]
    if isinstance(result, ox.QueryTriples):
        return [{"subject": ox_to_sparql_json(t.subject), "predicate": ox_to_sparql_json(t.predicate),
                 "object": ox_to_sparql_json(t.object)} for t in result]
    names = [v.value for v in result.variables]
    rows = []
    for sol in result:
        row = {}
        for n in names:
            term = sol[n]
            if term is not None:
                row[n] = ox_to_sparql_json(term) if include_types else term.value
        rows.append(row)
    return rows


def evaluate_over_run(query, assertion_graphs, include_types=False):
    """Answer a query over the data of one run, and nothing else.

    The run's assertion graphs are set as the query's dataset: their merge
    is the default graph, and they are the only named graphs. The other
    nanopublication graphs, and the graphs of other runs, are invisible to
    the query, whose text is sent unmodified.
    """
    if not assertion_graphs:
        return []
    if STORE_KIND == "none":
        raise RuntimeError("SCARAB_STORE_KIND is 'none': nothing was published to evaluate "
                           "against. Use evaluate_harvest_files on the exported files.")
    if STORE_KIND == "local":
        return _query_ox_store(local_store(), query, assertion_graphs, include_types)
    return execute_sparql_query(query, include_types=include_types,
                                default_graphs=list(assertion_graphs))


def answer_query(query, endpoints, base_named_graph=None, include_types=False):
    """Harvest a query's data and answer it over that data alone."""
    graphs = FindBGPPriority(query, endpoints, base_named_graph=base_named_graph)
    return evaluate_over_run(query, graphs, include_types=include_types)


def load_harvest_files(paths, store=None):
    """Load exported harvests (TriG files) into a store for evaluation.

    This is the gathering step of an isolated deployment: each site
    exports its nanopublication with SCARAB_EXPORT_DIR, and whoever is
    entitled to combine them loads the files and evaluates the query.

    Args:
        paths: TriG files written by `publish_harvest`.
        store: A pyoxigraph Store; a new in-memory one if omitted.

    Returns:
        (store, assertion graph IRIs).
    """
    store = store if store is not None else ox.Store()
    for path in paths:
        store.bulk_load(path=str(path), format=ox.RdfFormat.TRIG)
    graphs = sorted(g.value for g in store.named_graphs() if g.value.endswith("#assertion"))
    return store, graphs


def evaluate_harvest_files(query, paths, include_types=False):
    """Load exported harvests and answer a query over their assertion graphs."""
    store, graphs = load_harvest_files(paths)
    return _query_ox_store(store, query, graphs, include_types)


def run_query_strict(query, endpoints, base_named_graph="urn:tpf:temp", strict=True):
    """Harvest a query in strict mode and hand back the triples.

    Used by SPHINX's TPF adapter, which wants the triples themselves
    rather than an answer. The harvest is kept in its working stores and
    never published, so exploratory indexing leaves nothing behind in the
    triplestore.

    Returns:
        A list of (subject, predicate, object) triples, each term a
        SPARQL-JSON dict so that its node kind survives.
    """
    if isinstance(endpoints, (str, dict, DataSource)):
        endpoints = [endpoints]
    endpoints = list(endpoints)
    if not endpoints:
        return []
    bgp = _prepare(query)
    if not bgp:
        return []
    run_id = f"{base_named_graph}/{_uuid_mod.uuid4()}"
    triples = []
    for i, spec in enumerate(endpoints):
        ws = harvest_endpoint_optimized(spec, bgp, mint_nanopub_uri(run_id, i + 1),
                                        strict=strict, publish=False, keep_store=True)
        try:
            for q in ws.quads([ws.assertion]):
                triples.append((ox_to_sparql_json(q.subject), ox_to_sparql_json(q.predicate),
                                ox_to_sparql_json(q.object)))
        finally:
            ws.close()
    logger.info("[run_query_strict] Returned %d triples", len(triples))
    return triples
