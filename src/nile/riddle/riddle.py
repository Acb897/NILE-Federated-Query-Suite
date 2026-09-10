"""
query_matching_class.py

Query -> SHACL matching (partial responsiveness by triple-pattern overlap)

Matching rules (any one hit = endpoint is responsive):
  1. Exact:            query (A, P, B)   in SHACL exact (A, P, B)
  2. SHACL wildcard:   query (A, P, *)   where SHACL has (A, P, ANY)
  3. Query B wildcard: query (A, P, ANY) where SHACL has exact (A, P, B)
  4. Subject ANY:      query (ANY, P, *) where P exists anywhere in SHACL

The query-side abstraction and the four matching rules are unchanged.
The endpoint-side abstraction A(E) is now produced by a loader that
tolerates shapes graphs emitted by extractors other than SPHINX
(sheXer, QSE, SHACLGen, ...). The differences it accommodates are:

  - sh:path may be a path expression rather than a plain IRI. An
    inverse path is folded into an (A, P, B) pattern with its ends
    exchanged, which is how sheXer records what SPHINX records as
    P-(c). Composite paths contribute their predicates to rule 4 only.
  - the subject class may be declared by an implicit class target
    (the shape node is itself an rdfs:Class) rather than by
    sh:targetClass, or may be absent altogether.
  - the value class may sit behind sh:node, sh:qualifiedValueShape or
    an sh:and / sh:or / sh:xone operand rather than on a direct
    sh:class.
  - profiles may arrive in a serialisation other than Turtle.

Abstracted profiles are cached on (mtime, size), since A(E) was
otherwise recomputed for every incoming query.
"""

import os
import logging
from typing import Dict, List, Optional, Sequence, Set, Tuple

from rdflib import Graph, URIRef, Literal, BNode, Namespace
from rdflib.namespace import RDF, RDFS
from rdflib.term import Variable
from rdflib.util import guess_format
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery

logger = logging.getLogger(__name__)

RDF_TYPE_STR = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
ANY = "ANY"  # Wildcard sentinel (mirrors Ruby :ANY)

SH = Namespace("http://www.w3.org/ns/shacl#")
DCT = Namespace("http://purl.org/dc/terms/")

# (subject_type_or_ANY, predicate_str, object_type_or_ANY)
APBPattern = Tuple[str, str, str]

# Serialisations a third-party profile may plausibly arrive in.
PROFILE_EXTENSIONS = (".ttl", ".nt", ".n3", ".nq", ".trig", ".jsonld",
                      ".rdf", ".owl", ".xml")

# Logical-constraint properties whose operands may hold further class or
# property information (QSE emits sh:or for heterogeneous ranges).
_LOGICAL = (SH["and"], SH["or"], SH.xone)


# ---------------------------------------------------------------------------
# 1) Algebra walker - collect every triple pattern in the algebra tree
# ---------------------------------------------------------------------------

def collect_triple_patterns(node, acc=None, visited=None):
    """
    Recursively descend a rdflib SPARQL algebra tree and collect all
    (subject, predicate, object) triples from BGP nodes.

    Handles:
      - BGP nodes         -> extract .triples directly
      - list / tuple      -> iterate elements
      - other CompValues  -> recurse into .values()
      - RDF terms / scalars -> ignored (leaves)
    A visited set (by id) prevents infinite loops on cycles.
    """
    if acc is None:
        acc = []
    if visited is None:
        visited = set()

    if node is None:
        return acc

    # Leaves - nothing to descend into
    if isinstance(node, (str, int, float, bool, bytes,
                         URIRef, Literal, BNode, Variable)):
        return acc

    # Lists / tuples - recurse into each element
    if isinstance(node, (list, tuple)):
        for item in node:
            collect_triple_patterns(item, acc, visited)
        return acc

    # Cycle guard
    nid = id(node)
    if nid in visited:
        return acc
    visited.add(nid)

    # BGP: the triple patterns live in node['triples']
    if getattr(node, 'name', None) == 'BGP':
        for triple in (node.get('triples') or []):
            if isinstance(triple, (list, tuple)) and len(triple) == 3:
                acc.append(tuple(triple))
        return acc

    # All other algebra nodes (Join, LeftJoin, Filter, Project, etc.)
    # - recurse into every value they hold
    if hasattr(node, 'values'):
        for value in node.values():
            collect_triple_patterns(value, acc, visited)

    return acc


# ---------------------------------------------------------------------------
# 2) Build (A, P, B) patterns from a SPARQL query string
# ---------------------------------------------------------------------------

def _is_var(term) -> bool:
    return isinstance(term, Variable)


def _is_uri(term) -> bool:
    return isinstance(term, URIRef)


def extract_APB_from_query(query_string: str, debug: bool = True) -> List[APBPattern]:
    """
    Parse query -> collect triple patterns -> derive (A, P, B).

    A/B are the concrete rdf:type IRI of the subject/object variable
    (from ?var rdf:type <IRI> patterns in the same query), or ANY when
    no type constraint is present.
    Patterns with a variable predicate are skipped.
    rdf:type triples are skipped (they supply type info, not data).
    """
    parsed = parseQuery(query_string)
    query_obj = translateQuery(parsed)
    algebra = query_obj.algebra

    raw = collect_triple_patterns(algebra)

    # Deduplicate by string representation
    seen: Set[tuple] = set()
    unique_raw = []
    for triple in raw:
        key = (str(triple[0]), str(triple[1]), str(triple[2]))
        if key not in seen:
            seen.add(key)
            unique_raw.append(triple)
    raw = unique_raw

    if debug:
        print(f"  [DEBUG] raw patterns collected from algebra: {len(raw)}")
        for s, p, o in raw:
            print(f"    S={s} P={p} O={o}")

    # variable name (str) -> set of concrete type IRIs
    var_types: Dict[str, Set[str]] = {}
    for s, p, o in raw:
        if _is_var(p):
            continue
        if str(p) != RDF_TYPE_STR:
            continue
        if _is_var(s) and _is_uri(o):
            var_types.setdefault(str(s), set()).add(str(o))

    if debug:
        print(f"  [DEBUG] var_types: "
              f"{[f'{k} => {list(v)}' for k, v in var_types.items()]}")

    result: Set[APBPattern] = set()
    for s, p, o in raw:
        if _is_var(p):
            continue
        if str(p) == RDF_TYPE_STR:
            continue
        pred = str(p)

        types_a = list(var_types.get(str(s), set())) if _is_var(s) else []
        if not types_a:
            types_a = [ANY]

        types_b = list(var_types.get(str(o), set())) if _is_var(o) else []
        if not types_b:
            types_b = [ANY]

        for a in types_a:
            for b in types_b:
                result.add((a, pred, b))

    result_list = list(result)
    if debug:
        print(f"  [DEBUG] APB patterns from query: {len(result_list)}")
        for apb in result_list:
            print(f"    {apb}")

    return result_list


# ---------------------------------------------------------------------------
# 3) Build (A, P, B) patterns from a SHACL shapes graph
# ---------------------------------------------------------------------------

class Profile:
    """
    The endpoint-side abstraction A(E), plus what is needed to evaluate
    matching rule 4 and to identify the repository the profile belongs
    to.

      patterns : set of (A, P, B), A/B possibly ANY
      paths    : every predicate IRI referenced by any sh:path,
                 including those buried inside path expressions. Rule 4
                 must be evaluated against this rather than against the
                 raw sh:path objects of the graph, which are blank-node
                 identifiers whenever a path expression is used.
      source   : the endpoint described (dct:source), when declared.
    """

    __slots__ = ("patterns", "paths", "source", "path")

    def __init__(self, patterns, paths, source=None, path=None):
        self.patterns: Set[APBPattern] = patterns
        self.paths: Set[str] = paths
        self.source: Optional[str] = source
        self.path: Optional[str] = path


def _list_items(g: Graph, node) -> List:
    """Traverse an rdf:List, guarding against cycles."""
    items, seen = [], set()
    while node is not None and node != RDF.nil and node not in seen:
        seen.add(node)
        first = g.value(node, RDF.first)
        if first is not None:
            items.append(first)
        node = g.value(node, RDF.rest)
    return items


def _analyse_path(g: Graph, node, depth: int = 0):
    """
    Resolve an sh:path value.

    Returns (simple, predicates) where

      simple     : (predicate_iri, inverted) when the path denotes a
                   single predicate traversed in one direction, else
                   None.
      predicates : every predicate IRI the expression references, used
                   for matching rule 4.

    Cardinality modifiers over a single predicate remain "simple", since
    the relation A --p--> B does hold of the underlying predicate.
    Sequence and alternative paths do not: they denote a composite
    relation whose two ends are not the ends of any single predicate, so
    they contribute their predicates to rule 4 only.
    """
    if depth > 8 or node is None:
        return None, []

    if isinstance(node, URIRef):
        return (str(node), False), [str(node)]

    inverse = g.value(node, SH.inversePath)
    if inverse is not None:
        simple, preds = _analyse_path(g, inverse, depth + 1)
        if simple is not None:
            return (simple[0], not simple[1]), preds
        return None, preds

    for modifier in (SH.zeroOrMorePath, SH.oneOrMorePath, SH.zeroOrOnePath):
        wrapped = g.value(node, modifier)
        if wrapped is not None:
            return _analyse_path(g, wrapped, depth + 1)

    alternative = g.value(node, SH.alternativePath)
    if alternative is not None:
        preds = []
        for item in _list_items(g, alternative):
            _, sub = _analyse_path(g, item, depth + 1)
            preds.extend(sub)
        return None, preds

    # A bare blank node bearing rdf:first is a sequence path.
    items = _list_items(g, node)
    if items:
        preds = []
        for item in items:
            _, sub = _analyse_path(g, item, depth + 1)
            preds.extend(sub)
        return None, preds

    return None, []


def _is_implicit_class(g: Graph, node) -> bool:
    """
    SHACL implicit class target: a node that is both a shape and an
    rdfs:Class is its own target. SHACLGen's --implicit mode and some
    ontology-derived profiles rely on this instead of sh:targetClass.
    """
    types = set(g.objects(node, RDF.type))
    return RDFS.Class in types and (SH.NodeShape in types or SH.Shape in types)


def _shape_class(g: Graph, shape) -> List[str]:
    """The class(es) a node shape targets, for use as the B position."""
    out = [str(o) for _, _, o in g.triples((shape, SH.targetClass, None))
           if isinstance(o, URIRef)]
    if not out and isinstance(shape, URIRef) and _is_implicit_class(g, shape):
        out.append(str(shape))
    return out


def _value_classes(g: Graph, prop_shape, depth: int = 0) -> List[str]:
    """
    Every class the value of a property shape may take: sh:class
    directly, the target class of an sh:node reference, the operands of
    sh:and / sh:or / sh:xone, and sh:qualifiedValueShape. sh:datatype
    and sh:nodeKind sh:Literal contribute nothing, which correctly
    yields the ANY wildcard and preserves the reading of a missing class
    as absence of information rather than absence of the relation.
    """
    if depth > 6:
        return []

    classes = [str(o) for _, _, o in g.triples((prop_shape, SH["class"], None))
               if isinstance(o, URIRef)]

    for _, _, node_shape in g.triples((prop_shape, SH.node, None)):
        classes.extend(_shape_class(g, node_shape))
        classes.extend(_value_classes(g, node_shape, depth + 1))

    for _, _, qvs in g.triples((prop_shape, SH.qualifiedValueShape, None)):
        classes.extend(_shape_class(g, qvs))
        classes.extend(_value_classes(g, qvs, depth + 1))

    for logical in _LOGICAL:
        for _, _, lst in g.triples((prop_shape, logical, None)):
            for operand in _list_items(g, lst):
                classes.extend(_shape_class(g, operand))
                classes.extend(_value_classes(g, operand, depth + 1))

    return classes


def _property_shapes(g: Graph, shape, depth: int = 0, seen=None) -> List:
    """
    Property shapes attached to a node shape, following sh:property
    directly and descending through sh:and / sh:or / sh:xone, which some
    emitters use to group alternative constraint sets.
    """
    if seen is None:
        seen = set()
    if depth > 6 or shape in seen:
        return []
    seen.add(shape)

    out = list(g.objects(shape, SH.property))

    for logical in _LOGICAL:
        for _, _, lst in g.triples((shape, logical, None)):
            for operand in _list_items(g, lst):
                out.extend(_property_shapes(g, operand, depth + 1, seen))

    return out


def _shape_targets(g: Graph, shape, prop_shapes: Sequence) -> List[str]:
    """
    The subject class(es) a node shape describes, i.e. the A position.

    Falls back through: sh:targetClass, implicit class target, and a
    property shape asserting `sh:path rdf:type ; sh:hasValue <C>`, which
    is how some emitters encode typing rather than as a target. A shape
    with no recoverable target still yields patterns, with A = ANY, so
    that its predicates remain visible to matching rule 4 rather than
    being discarded.
    """
    targets = _shape_class(g, shape)
    if targets:
        return targets

    for prop in prop_shapes:
        path = g.value(prop, SH.path)
        if path is None or str(path) != RDF_TYPE_STR:
            continue
        targets.extend(str(o) for _, _, o in g.triples((prop, SH.hasValue, None))
                       if isinstance(o, URIRef))

    return targets or [ANY]


def extract_profile(shapes_graph: Graph, path: Optional[str] = None) -> Profile:
    """
    Abstract a shapes graph, from any emitter, into A(E).
    """
    patterns: Set[APBPattern] = set()
    paths: Set[str] = set()

    shape_nodes = set(shapes_graph.subjects(RDF.type, SH.NodeShape))
    shape_nodes |= set(shapes_graph.subjects(SH.targetClass, None))
    shape_nodes |= set(shapes_graph.subjects(SH.property, None))
    shape_nodes |= set(shapes_graph.subjects(SH.targetSubjectsOf, None))
    shape_nodes |= set(shapes_graph.subjects(SH.targetObjectsOf, None))

    for shape in shape_nodes:

        prop_shapes = _property_shapes(shapes_graph, shape)
        subject_classes = _shape_targets(shapes_graph, shape, prop_shapes)

        # sh:targetSubjectsOf / sh:targetObjectsOf name a real predicate
        # of the data even though they say nothing about the class at
        # either end. Registering them keeps rule 4 correct.
        for target_prop in (SH.targetSubjectsOf, SH.targetObjectsOf):
            for _, _, pred in shapes_graph.triples((shape, target_prop, None)):
                if isinstance(pred, URIRef) and str(pred) != RDF_TYPE_STR:
                    paths.add(str(pred))
                    patterns.add((ANY, str(pred), ANY))

        for prop_shape in prop_shapes:

            path_node = shapes_graph.value(prop_shape, SH.path)
            if path_node is None:
                continue

            simple, predicates = _analyse_path(shapes_graph, path_node)

            for predicate in predicates:
                if predicate != RDF_TYPE_STR:
                    paths.add(predicate)

            if simple is None:
                # Composite path: its predicates are visible to rule 4,
                # but its endpoints are not the endpoints of any one of
                # them, so no concrete (A, P, B) can be asserted.
                for predicate in predicates:
                    if predicate != RDF_TYPE_STR:
                        patterns.add((ANY, predicate, ANY))
                continue

            predicate, inverted = simple

            # rdf:type carries typing, not a data relation, and is
            # stripped from the query side too.
            if predicate == RDF_TYPE_STR:
                continue

            value_classes = _value_classes(shapes_graph, prop_shape) or [ANY]

            for subject_class in subject_classes:
                for value_class in value_classes:
                    if inverted:
                        # sh:path [ sh:inversePath P ] on a shape
                        # targeting A, with value class B, describes
                        # B --P--> A in the data. This is how sheXer
                        # records what SPHINX records as P-(c).
                        patterns.add((value_class, predicate, subject_class))
                    else:
                        patterns.add((subject_class, predicate, value_class))

    source = None
    for _, _, o in shapes_graph.triples((None, DCT.source, None)):
        source = str(o)
        break

    return Profile(patterns, paths, source, path)


def extract_APB_from_shacl(shapes_graph: Graph) -> List[APBPattern]:
    """
    Signature-compatible with the original function, retained for
    callers that only need the pattern set. Note that a caller relying
    on this alone cannot evaluate rule 4 correctly, since the predicates
    of composite paths are carried on Profile.paths.
    """
    return list(extract_profile(shapes_graph).patterns)


# ---------------------------------------------------------------------------
# 4) Profile loading
# ---------------------------------------------------------------------------

# path -> (mtime, size, Profile)
_profile_cache: Dict[str, Tuple[float, int, Profile]] = {}


def load_profile(file_path: str, use_cache: bool = True) -> Optional[Profile]:
    """
    Parse and abstract one profile file. The serialisation is guessed
    from the extension rather than assumed to be Turtle, since the
    extractors NILE can consume profiles from do not all default to it.

    Cached on (mtime, size): A(E) was otherwise recomputed for every
    incoming query, which is tolerable for a SPHINX profile and not for
    a profile extracted from a large graph by a third-party tool.
    """
    try:
        stat = os.stat(file_path)
    except OSError as exc:
        logger.warning(f"Cannot stat {file_path}: {exc}")
        return None

    key = os.path.abspath(file_path)
    if use_cache and key in _profile_cache:
        mtime, size, cached = _profile_cache[key]
        if mtime == stat.st_mtime and size == stat.st_size:
            return cached

    fmt = guess_format(file_path) or "turtle"
    graph = Graph()
    try:
        graph.parse(file_path, format=fmt)
    except Exception as exc:
        logger.warning(f"Error parsing {file_path} as {fmt}: {exc}")
        return None

    profile = extract_profile(graph, path=file_path)
    _profile_cache[key] = (stat.st_mtime, stat.st_size, profile)
    return profile


def discover_profiles(shacl_dir: str) -> List[str]:
    """
    Every profile file in a directory, in a stable order. Replaces the
    previous glob for "*.ttl", which silently ignored profiles emitted
    in any other serialisation.
    """
    try:
        names = sorted(os.listdir(shacl_dir))
    except OSError as exc:
        logger.warning(f"Cannot list {shacl_dir}: {exc}")
        return []

    return [os.path.join(shacl_dir, name) for name in names
            if name.lower().endswith(PROFILE_EXTENSIONS)]


def clear_profile_cache() -> None:
    """Drop every cached abstraction (for tests, or after re-indexing)."""
    _profile_cache.clear()


# ---------------------------------------------------------------------------
# 5) Main validator
# ---------------------------------------------------------------------------

def shacl_validator(query_string: str, shacl_dir: str,
                    debug: bool = True,
                    identify_by: str = "filename") -> List[str]:
    """
    Determine which SHACL endpoint profiles are responsive to the given
    query.

    Matching rules (any hit = endpoint marked responsive):
      1. Exact:            query (A, P, B)   in SHACL exact (A, P, B)
      2. SHACL wildcard:   query (A, P, *)   where SHACL has (A, P, ANY)
      3. Query B wildcard: query (A, P, ANY) where SHACL has exact (A, P, B)
      4. Subject ANY:      query (ANY, P, *) where P exists in any SHACL path

    NOTE: earlier versions of this function required the query to contain
    at least one rdf:type triple pattern before any matching was
    attempted, on the assumption that a query with no explicit typing
    could not be usefully abstracted. That gate has been removed: a query
    with no rdf:type patterns at all still abstracts to one or more
    (ANY, P, ANY) APB patterns (see extract_APB_from_query), which rule 4
    is specifically designed to match against. Rejecting such queries
    outright discarded exactly the case rule 4 exists to handle.

    `identify_by` selects what is returned for each responsive profile:
    "filename" (the profile file's stem, the historical behaviour) or
    "source" (the dct:source endpoint the profile declares, falling back
    to the stem when it declares none, which is the case for every
    profile not generated by SPHINX).
    """
    if debug:
        print("[shacl_validator] Extracting APB from query...")
    query_patterns = extract_APB_from_query(query_string, debug=debug)

    if not query_patterns:
        if debug:
            print("  -> No matchable (A, P, B) patterns in query - skipping.")
        return []

    query_pattern_set = set(query_patterns)

    shacl_files = discover_profiles(shacl_dir)
    if debug:
        print(f"[shacl_validator] Found {len(shacl_files)} profile file(s)")

    responsive: List[str] = []

    for shacl_file in shacl_files:
        try:
            profile = load_profile(shacl_file)
            if profile is None:
                continue

            shacl_exact = {pat for pat in profile.patterns if pat[2] != ANY}
            shacl_wildcards = {(a, p) for a, p, b in profile.patterns
                               if b == ANY}
            # Taken from the profile rather than read back off the graph:
            # a raw sh:path object is a blank-node identifier whenever
            # the emitter uses a path expression, and the predicates of
            # inverse, sequence and alternative paths would then be
            # invisible to rule 4.
            shacl_all_paths = profile.paths

            # Rule 1: exact match (A, P, B) - both ends concrete
            exact_overlap = [
                (a, p, b) for a, p, b in query_patterns
                if a != ANY and b != ANY and (a, p, b) in shacl_exact
            ]
            # Rule 2: SHACL has (A, P, ANY) - matches any query (A, P, *)
            wildcard_shacl = [
                (a, p, b) for a, p, b in query_patterns
                if a != ANY and (a, p) in shacl_wildcards
            ]
            # Rule 3: query has (A, P, ANY) - matches SHACL exact (A, P, B)
            wildcard_query_b = [
                (a, p, b) for a, p, b in shacl_exact
                if (a, p, ANY) in query_pattern_set
            ]
            # Rule 4: query has (ANY, P, *) - matches if P exists in SHACL
            wildcard_any_subj = [
                (a, p, b) for a, p, b in query_patterns
                if a == ANY and p in shacl_all_paths
            ]

            hit = bool(exact_overlap or wildcard_shacl
                       or wildcard_query_b or wildcard_any_subj)

            if debug:
                fname = os.path.basename(shacl_file)
                print(f"\n=== {fname} ===")
                print(f"  source: {profile.source or '(undeclared)'}")
                print(f"  SHACL patterns: {len(profile.patterns)} "
                      f"| paths: {len(shacl_all_paths)}")
                print(f"  Rule1 exact: {len(exact_overlap)}  "
                      f"Rule2 shacl-wc: {len(wildcard_shacl)}  "
                      f"Rule3 query-wc: {len(wildcard_query_b)}  "
                      f"Rule4 any-subj: {len(wildcard_any_subj)}")
                print(f"  -> {'RESPONSIVE' if hit else 'not responsive'}")

            if hit:
                stem = os.path.splitext(os.path.basename(shacl_file))[0]
                if identify_by == "source" and profile.source:
                    responsive.append(profile.source)
                else:
                    responsive.append(stem)

        except Exception as e:
            logger.warning(f"Error processing {shacl_file}: {e}")
            continue

    return responsive