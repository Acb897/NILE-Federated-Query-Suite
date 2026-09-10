"""Matching queries against repository profiles.

RIDDLE decides which repositories are worth asking for a given SPARQL
query, using only the SHACL profiles produced offline. No repository is
contacted.

Both sides are reduced to the same thing: a set of (A, P, B) triples,
where A is the class of the subject, P the predicate, and B the class of
the object. A class that cannot be established becomes the wildcard ANY.
A repository is kept as soon as one of its triples is compatible with
one of the query's, under any of four rules:

    1. Exact             query (A, P, B)   against profile (A, P, B)
    2. Profile gap       query (A, P, *)   against profile (A, P, ANY)
    3. Query gap         query (A, P, ANY) against profile (A, P, B)
    4. Unknown subject   query (ANY, P, *) where P appears anywhere in
                         the profile

Rules 2 and 3 are what make the result useful rather than merely
correct. A missing class means "not known", not "does not occur", so a
repository is kept whenever it might contribute. The aim is to find
repositories that could help, not to prove that any one of them could
answer the query by itself.

Despite the vocabulary, this is not SHACL validation. Validation asks
whether a data graph satisfies a set of constraints; here two structural
descriptions are compared against each other, and a standard SHACL
engine would answer a different question.

Usage::

    from nile.riddle.riddle import shacl_validator

    responsive = shacl_validator(query, "./shacl_output",
                                 identify_by="source")

Profiles need not come from SPHINX. Shapes written by other extractors
(sheXer, QSE, SHACLGen and so on) are read too, which means
accommodating several things SPHINX never emits:

  - sh:path holding a path expression rather than a plain IRI. An
    inverse path becomes an (A, P, B) triple with its ends swapped,
    which is how sheXer records what SPHINX records as an incoming
    relationship. Composite paths contribute to rule 4 only.
  - a subject class declared by an implicit class target -- the shape
    node being an rdfs:Class itself -- rather than by sh:targetClass,
    or missing altogether.
  - a value class sitting behind sh:node, sh:qualifiedValueShape or an
    sh:and / sh:or / sh:xone operand rather than on a direct sh:class.
  - a serialisation other than Turtle.

Parsed profiles are cached on the file's modification time and size, so
each is abstracted once rather than once per incoming query.
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
    """Collect every triple pattern in a parsed query.

    Walks an rdflib SPARQL algebra tree and gathers the triples out of
    every basic graph pattern it contains, descending through everything
    on the way -- OPTIONAL, UNION, FILTER and the rest alike.

    Args:
        node: An algebra node, normally the root of a translated query.
        acc: Accumulator for the recursion. Leave unset.
        visited: Cycle guard for the recursion. Leave unset.

    Returns:
        A list of (subject, predicate, object) tuples of rdflib terms.

    Note:
        Because the walk descends everywhere, a pattern the query treats
        as optional is collected on the same footing as one it requires.
        For source selection that is intended: a repository holding only
        the optional part is still worth asking.
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
    """Return True if `term` is a SPARQL variable."""
    return isinstance(term, Variable)


def _is_uri(term) -> bool:
    """Return True if `term` is an IRI."""
    return isinstance(term, URIRef)


def extract_APB_from_query(query_string: str, debug: bool = True) -> List[APBPattern]:
    """Reduce a query to its (A, P, B) triples.

    Variables are typed from the rdf:type patterns in the same query, so
    `?p rdf:type ex:Plant` makes ex:Plant the class of ?p wherever it
    appears. Anything that cannot be typed that way becomes ANY.
    Variable names are then discarded, so two queries with the same
    shape and different names reduce to the same set.

    Args:
        query_string: The SPARQL query.
        debug: Whether to print the intermediate steps to standard
            output.

    Returns:
        A list of (A, P, B) triples of strings, where A and B may be
        ANY.

    Raises:
        Exception: Whatever rdflib raises if the query cannot be parsed.

    Note:
        Patterns with a variable predicate are skipped -- there is
        nothing there to match a profile against. rdf:type patterns are
        skipped too, since they supply the typing rather than a
        relationship of their own.

        A variable asserted to belong to more than one class carries all
        of them at once, and the triples are taken over every
        combination. That holds even when the two assertions sit in
        different branches of a UNION and could never apply together.
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
    """A repository profile, reduced to what matching needs.

    Attributes:
        patterns: The (A, P, B) triples, where A and B may be ANY.
        paths: Every predicate IRI referenced by any sh:path, including
            those buried inside path expressions. Rule 4 is evaluated
            against this rather than against the raw sh:path objects of
            the graph, which are blank-node identifiers whenever a path
            expression is used.
        source: The repository the profile describes, from dct:source,
            where it declares one.
        path: The file the profile was read from.
    """

    __slots__ = ("patterns", "paths", "source", "path")

    def __init__(self, patterns, paths, source=None, path=None):
        """Store one abstracted profile.

        Args:
            patterns: Set of (A, P, B) triples.
            paths: Set of predicate IRIs appearing in any sh:path.
            source: Repository the profile describes, if declared.
            path: File the profile was read from.
        """
        self.patterns: Set[APBPattern] = patterns
        self.paths: Set[str] = paths
        self.source: Optional[str] = source
        self.path: Optional[str] = path


def _list_items(g: Graph, node) -> List:
    """Read an rdf:List into a Python list.

    Args:
        g: The graph holding the list.
        node: The head of the list.

    Returns:
        The items, in order. A malformed or cyclic list stops the walk
        rather than hanging.
    """
    items, seen = [], set()
    while node is not None and node != RDF.nil and node not in seen:
        seen.add(node)
        first = g.value(node, RDF.first)
        if first is not None:
            items.append(first)
        node = g.value(node, RDF.rest)
    return items


def _analyse_path(g: Graph, node, depth: int = 0):
    """Work out what an sh:path value denotes.

    Args:
        g: The shapes graph.
        node: The object of an sh:path statement.
        depth: Recursion guard. Leave unset.

    Returns:
        A (simple, predicates) pair. `simple` is a (predicate IRI,
        inverted) tuple when the path denotes a single predicate
        travelled in one direction, and None otherwise. `predicates` is
        every predicate IRI the expression mentions, which is what rule
        4 needs.

    Note:
        A cardinality modifier over a single predicate still counts as
        simple, since the relationship does hold of the underlying
        predicate. Sequence and alternative paths do not: they denote a
        composite whose two ends are not the ends of any one predicate,
        so they feed rule 4 alone.
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
    """Return True for a node that is its own target class.

    SHACL lets a node that is both a shape and an rdfs:Class target
    itself instead of naming a target with sh:targetClass. SHACLGen's
    implicit mode and some ontology-derived profiles rely on this.
    """
    types = set(g.objects(node, RDF.type))
    return RDFS.Class in types and (SH.NodeShape in types or SH.Shape in types)


def _shape_class(g: Graph, shape) -> List[str]:
    """Return the class or classes a node shape targets.

    Args:
        g: The shapes graph.
        shape: The node shape.

    Returns:
        Their IRIs as strings, from sh:targetClass or from an implicit
        class target. Empty if the shape names neither.
    """
    out = [str(o) for _, _, o in g.triples((shape, SH.targetClass, None))
           if isinstance(o, URIRef)]
    if not out and isinstance(shape, URIRef) and _is_implicit_class(g, shape):
        out.append(str(shape))
    return out


def _value_classes(g: Graph, prop_shape, depth: int = 0) -> List[str]:
    """List every class the value of a property shape may take.

    Looks at sh:class directly, the target class behind an sh:node
    reference, the operands of sh:and / sh:or / sh:xone, and
    sh:qualifiedValueShape.

    Args:
        g: The shapes graph.
        prop_shape: The property shape.
        depth: Recursion guard. Leave unset.

    Returns:
        Class IRIs as strings. Empty when none can be established.

    Note:
        sh:datatype and sh:nodeKind sh:Literal contribute nothing, which
        is what is wanted here: an empty result becomes the ANY
        wildcard, so a missing class keeps its reading of "not known"
        rather than "no such relationship".
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
    """Return the property shapes attached to a node shape.

    Follows sh:property directly, and descends through sh:and / sh:or /
    sh:xone, which some extractors use to group alternative sets of
    constraints.

    Args:
        g: The shapes graph.
        shape: The node shape.
        depth: Recursion guard. Leave unset.
        seen: Cycle guard. Leave unset.

    Returns:
        The property shape nodes.
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
    """Return the class or classes a node shape describes: the A position.

    Tries sh:targetClass, then an implicit class target, then a property
    shape asserting `sh:path rdf:type ; sh:hasValue <C>`, which is how
    some extractors encode typing rather than as a target.

    Args:
        g: The shapes graph.
        shape: The node shape.
        prop_shapes: Its property shapes, from `_property_shapes`.

    Returns:
        Class IRIs as strings, or [ANY] when none can be recovered. A
        shape with no recoverable target still yields triples that way,
        so its predicates stay visible to rule 4 instead of being lost.
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
    """Reduce a shapes graph to a Profile.

    Works on shapes from any extractor, not only SPHINX; the module
    docstring lists what that involves accommodating.

    Args:
        shapes_graph: The parsed shapes graph.
        path: The file it came from, recorded on the result.

    Returns:
        The Profile.

    Note:
        sh:targetSubjectsOf and sh:targetObjectsOf name a real predicate
        of the data even though they say nothing about the class at
        either end, so they are registered as (ANY, P, ANY) to keep rule
        4 correct.
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
    """Reduce a shapes graph to its (A, P, B) triples alone.

    Args:
        shapes_graph: The parsed shapes graph.

    Returns:
        The triples, as a list.

    Note:
        Kept for callers that want nothing but the triples. Rule 4
        cannot be evaluated from this alone, because the predicates of
        composite paths are carried on `Profile.paths` and not here. Use
        `extract_profile` where matching is the aim.
    """
    return list(extract_profile(shapes_graph).patterns)


# ---------------------------------------------------------------------------
# 4) Profile loading
# ---------------------------------------------------------------------------

# path -> (mtime, size, Profile)
_profile_cache: Dict[str, Tuple[float, int, Profile]] = {}


def load_profile(file_path: str, use_cache: bool = True) -> Optional[Profile]:
    """Read one profile file and reduce it to a Profile.

    The serialisation is guessed from the extension rather than assumed
    to be Turtle, since the extractors NILE can read profiles from do
    not all default to it.

    Results are cached on the file's modification time and size, so a
    profile is abstracted once rather than once per query -- tolerable
    for a SPHINX profile, less so for one extracted from a large graph
    by another tool.

    Args:
        file_path: Path to the profile file.
        use_cache: Whether to reuse a cached abstraction.

    Returns:
        The Profile, or None if the file could not be read or parsed.
        Failures are logged as warnings rather than raised.
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
    """List the profile files in a directory.

    Args:
        shacl_dir: Directory to look in. Not searched recursively.

    Returns:
        Paths, sorted by filename. Only files whose extension is a
        recognised RDF serialisation are included, so a profile written
        with an unusual extension is passed over. Empty if the directory
        cannot be listed.
    """
    try:
        names = sorted(os.listdir(shacl_dir))
    except OSError as exc:
        logger.warning(f"Cannot list {shacl_dir}: {exc}")
        return []

    return [os.path.join(shacl_dir, name) for name in names
            if name.lower().endswith(PROFILE_EXTENSIONS)]


def clear_profile_cache() -> None:
    """Drop every cached abstraction.

    Worth calling after re-indexing, or between tests. Profiles are
    otherwise cached until their file's modification time or size
    changes.
    """
    _profile_cache.clear()


# ---------------------------------------------------------------------------
# 5) Main validator
# ---------------------------------------------------------------------------

def shacl_validator(query_string: str, shacl_dir: str,
                    debug: bool = True,
                    identify_by: str = "filename") -> List[str]:
    """Find the repositories responsive to a query.

    The query is reduced to its (A, P, B) triples, every profile in
    `shacl_dir` is reduced the same way, and the two are compared under
    the four rules described in the module docstring. One compatible
    pair is enough.

    Args:
        query_string: The SPARQL query.
        shacl_dir: Directory of profile files, as written by
            `nile.sphinx.sphinx.Engine.shacl_generator`.
        debug: Whether to print the reasoning for each profile to
            standard output.
        identify_by: How to name each responsive repository.
            "filename" gives the profile file's stem; "source" gives the
            repository the profile declares in dct:source, falling back
            to the stem where it declares none -- as every profile not
            written by SPHINX will.

    Returns:
        One entry per responsive profile, in filename order. Empty if
        the query holds nothing matchable, or if nothing matched.

    Note:
        A profile that cannot be read is logged and skipped, so one bad
        file does not cost the rest of the directory.

        A query with no rdf:type patterns at all is still matched: it
        reduces to (ANY, P, ANY) triples, which is precisely what rule 4
        exists to handle. Rejecting such queries would discard the case
        the rule was written for.
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