# SCARAB changes

## Correctness

- **Operator scopes.** Every pattern now records the operators it sits under
  (`transform()` → `"scopes"`). Restrictions follow them: required side of
  OPTIONAL → optional side, left of MINUS → right, filtered group → its
  EXISTS / NOT EXISTS patterns, never the reverse; never across UNION
  branches; never into or out of a subquery. A restriction crossing an
  OPTIONAL / MINUS / EXISTS boundary from outside uses only variables the
  required side always binds (otherwise removed triples create spurious
  rows); the binding query nests one subquery per boundary to enforce it.
- **EXISTS / NOT EXISTS** patterns are extracted and harvested (previously
  never collected, so EXISTS was always false and NOT EXISTS always true).
- **Blank nodes in the query** become fresh variables (previously sent to the
  server as constants, which matched nothing).
- **SERVICE** patterns are not harvested.
- **Binding queries** include only restricting patterns connected to the
  current one (no Cartesian products).
- **Malformed fragment pages.** A page holding an IRI that RFC 3987 forbids
  (a control character or a space, as in LinkedMDB's links) used to be
  rejected by the strict parser, and the walk of the fragment then stopped
  silently after the preceding pages. Such pages are now parsed leniently and
  the offending code points percent-encoded (RFC 3987, section 3.1); a rewritten
  IRI used as a constant in a later request is sent in its original spelling.
  A page that still cannot be read is fetched again as N-Quads and read one
  statement at a time, so a malformed statement costs only itself.
- **Partial retrieval is declared.** A pattern for which a request failed or
  a statement could not be read is reported as `incomplete` in
  `last_run_stats()`, linked to the source with
  `scarab:incompletelyRetrievedFrom` in the provenance, and the assertion
  graph is marked `scarab:declaredIncomplete`. The number of rewritten IRIs
  is recorded with `scarab:rewrittenIRIs`; the statistics gain
  `iris_rewritten` and `triples_dropped`.
- **Fragments are not retrieved twice.** Once a pattern's fragment has been
  retrieved in full, a later pattern it covers (same constants, any variable
  names, possibly more positions bound; `fragment_covers()`) is answered from
  the working store and reported as `reused`, with no request. SP2Bench q6
  and q7 repeat `?doc rdf:type ?class` in their OPTIONALs; where it cannot be
  restricted, each copy used to be retrieved in full, and the page cache could
  not help once the fragment had more pages than it holds. A fragment
  retrieved incompletely is never reused.
- **Page cache** is bounded and cleared every run (previously unbounded and
  kept for the life of the process, which also skewed benchmark repetitions).

## Performance

- Embedded pyoxigraph **working store** per source (on disk by default):
  binding queries and path evaluation run in-process. The ingestion queue,
  its 500-triple batches and 50 ms sleeps are gone.
- Keep-alive HTTP sessions; 16 concurrent requests per source (was 3); no
  barrier between groups of bound requests; counts and sources run
  concurrently.
- **Bind join vs. complete fragment**: the cheaper strategy is chosen
  (`w·⌈|F|/p⌉ < ⌈|B|/k⌉`); the complete fragment is filtered locally, so both
  keep the same triples.
- SPARQL sources receive bindings in **VALUES blocks** (100 per request).
- Bindings that cannot match (literal as subject, non-IRI as predicate) are
  dropped before any request.
- Fragment pages are parsed with Oxigraph according to their content type.
- Property paths are evaluated as soon as their base predicates are in, and
  may restrict joined patterns when their local evaluation is complete and
  they cannot match zero-length routes. Synthetic path triples are never
  published.

## Publication and evaluation

- `SCARAB_STORE_KIND`: `remote` (default; any SPARQL + graph store protocol
  triplestore), `local` (embedded pyoxigraph store) or `none`.
- `SCARAB_EXPORT_DIR`: also write each source's nanopublication as TriG.
- `load_harvest_files()` / `evaluate_harvest_files()`: combine exported
  harvests and answer a query over them (the gathering step of an isolated
  deployment).
- `run_query_strict()` (SPHINX's TPF adapter) no longer publishes anything.
- `last_run_stats()`: per-source requests, pages, bytes, errors, triples and
  per-pattern decisions; `configure()` changes settings at run time.

See the module docstring for the full list of settings.

## API changes

Removed (internal): `add_to_buffer`, `buffer_flusher_daemon`,
`insert_triples_stream`, `extract_upstream_bindings(_graphdb)`,
`build_query`, `fetch_binding(_batch)`, `bindable_bindings`,
`heuristic_cardinality`, `harvest_pattern_into_repo`,
`triple_matches_request`, `pattern_term_matches`, `evaluate_path_locally`.
`DataSource.harvest()` is replaced by `fetch()` / `fetch_bound()`.
`write_nanopub_graphs()` now takes the working store; `nanopub_quads()`
builds the quads. `fetch_tpf_page()` returns a `Page`.

New dependency: `pyoxigraph>=0.5`.

## Tests

`python -m pytest` runs 101 tests in about 10 s, without external services
(`tests/support` provides an in-process QPF server and SPARQL endpoint):
scope rules; 29 queries × {TPF, SPARQL, dump} compared with evaluation over
the complete data; publication, export and re-loading; SPHINX's strict mode;
isolated harvesting of sources with disjoint vocabularies.

## Recommended store configuration

Create the GraphDB repository without inference and with `owl:sameAs`
expansion disabled, so answers are computed over the harvested triples only.

## SPHINX: dump adapter reads named graphs

`RDFDumpAdapter` parsed files into a plain rdflib `Graph`, which silently
drops every triple a TriG or N-Quads file places in a named graph (for a
dataset with one named graph per record, such as CARE-SM, nearly all of
it). It now parses into `Dataset(default_union=True)` and indexes the union
of all graphs; a triple asserted in several graphs is indexed once.
`tests/test_sphinx_dump_adapter.py` checks TriG, N-Quads, Turtle and
N-Triples, and agreement with the TPF adapter.

## Forced dumps are skipped and declared

An all-variable pattern (`?s ?p ?o`) whose restricting patterns yield no
bindings at a source would be retrieved in full there: the whole repository.
Such a forced dump is now skipped (`SCARAB_SKIP_FORCED_DUMPS`, on by default;
`configure(skip_forced_dumps=False)` restores the old behaviour). The skip is
reported in `last_run_stats()` (decision `skipped-dump`, `declared_incomplete`)
and in the provenance graph (`scarab:notRetrievedFrom`, `scarab:skipReason`,
`scarab:declaredIncomplete`). A skipped pattern does not restrict others. An
all-variable pattern nothing may restrict is still retrieved in full.
`tests/test_scarab_forced_dumps.py` covers the S1 shape (nothing lost), the S9
shape (cross-repository answers lost and declared), and the unrestricted case.
