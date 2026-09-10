# NILE Federated Query Suite

NILE answers SPARQL queries that no single repository can answer on its own.

Given a query and a set of RDF repositories, NILE works out which of them are
worth asking, retrieves the relevant triples from each, loads everything into a
local triplestore, and evaluates the original query over the combined result.
Answers can therefore be assembled from triples that no one repository held
together.

Repositories may publish their data as a SPARQL endpoint, as a Triple or Quad
Pattern Fragments server, or as a downloadable RDF file. All three are
supported throughout and may be mixed freely within a single query. The query
author does not need to know which mechanism a given repository exposes.

---

## How it works

NILE runs in two phases, which may be carried out by different people at
different times.

**Offline — once per repository, repeated when its contents change.** SPHINX
visits a repository and writes down how its data is *shaped*: which classes it
holds, and which predicates link them to one another. The result is a SHACL
file. No instance data is copied, so the description stays small however large
the repository is, and two repositories following the same data model produce
near-identical files even with no data in common.

**Online — once per query.** RIDDLE compares an incoming query against those
descriptions and returns the repositories whose data could contribute. It
contacts nothing: both the query and each description are reduced to sets of
(subject class, predicate, object class) triples and compared directly. SCARAB
then harvests the selected repositories one triple pattern at a time, loads
what comes back into a local triplestore, and leaves the query to be evaluated
there.

| Module | Phase | Entry point |
| --- | --- | --- |
| **SPHINX** | offline | `nile.sphinx.sphinx.Engine` |
| **RIDDLE** | online | `nile.riddle.riddle.shacl_validator` |
| **SCARAB** | online | `nile.scarab.scarab_harvester.FindBGPPriority` |

Every module carries full docstrings, so `help(nile.scarab.scarab_harvester)`
and its equivalents serve as the reference documentation.

---

## Requirements

- Python 3.10 or newer
- A triplestore reachable over the SPARQL protocol and the RDF graph store
  protocol, for SCARAB to harvest into. The reference deployment uses GraphDB,
  but nothing specific to it is relied upon.

Python dependencies (`beautifulsoup4`, `requests`, `rdflib`, `SPARQLWrapper`)
install automatically.

SPHINX and RIDDLE need no triplestore of their own — unless SPHINX is pointed
at a fragments server, since that path runs through SCARAB and so needs the
same store.

## Installation

```bash
git clone https://github.com/Acb897/NILE.git
cd NILE
pip install -e .
```

With the test dependencies as well:

```bash
pip install -e ".[dev]"
```

---

## Configuration

SCARAB reads its settings from the environment at import time. **The defaults
point at the developer's own machine, so set the store location before running
anything.**

| Variable | Default | Meaning |
| --- | --- | --- |
| `SCARAB_STORE_BASE` | `http://acb8computer:7200` | Local triplestore |
| `SCARAB_STORE_REPOSITORY` | `test1` | Repository within it |
| `SCARAB_STORE_QUERY_URL` | derived from the two above | Overrides the query URL |
| `SCARAB_STORE_STATEMENTS_URL` | derived from the query URL | Overrides the statements URL |
| `SCARAB_MAX_THREADS` | `3` | Concurrent requests per bind join |
| `SCARAB_HARVEST_PATH_BASE_PREDICATES` | `1` | Set to `0` to stop property paths retrieving their base predicates |
| `SPHINX_MAX_WORKERS` | `6` | Classes explored concurrently while indexing |

```bash
export SCARAB_STORE_BASE=http://localhost:7200
export SCARAB_STORE_REPOSITORY=nile
```

---

## Usage

### 1. Index the repositories (SPHINX)

```python
from nile.sphinx.sphinx import Engine

engine = Engine()

index = engine.extract_patterns([
    "http://example.org/sparql",
    "http://other.example.org/sparql",
])

engine.shacl_generator(index, "./shacl_output")
```

`extract_patterns` also accepts `mode="dump"` (RDF file paths) or `mode="tpf"`
(fragments server URLs). One call uses one mode for all of its sources, so
index each kind separately and write the results into the same directory.

Indexing time depends mostly on how many distinct classes a repository holds,
not on how many triples. A repository that cannot be reached contributes an
empty entry rather than aborting the run.

> **Note.** `shacl_generator` appends a hash to a filename that is already
> taken, so re-indexing into a directory that already holds output *adds* files
> rather than replacing them. Since RIDDLE reads every file in the directory it
> is given, clear it first to avoid matching against a stale profile.

> **Privacy.** A profile discloses no individual records, but it does publish
> the repository's complete class and predicate vocabulary. In a sensitive
> domain, revealing that a rare or highly specific class is present can itself
> be disclosive.

### 2. Select the responsive repositories (RIDDLE)

```python
from nile.riddle.riddle import shacl_validator

responsive = shacl_validator(
    query,
    "./shacl_output",
    identify_by="source",   # endpoint URL rather than profile filename
    debug=False,            # defaults to True, which prints a great deal
)
```

Matching is deliberately generous. A class missing on either side is read as
"not known", never as "does not occur", so a repository is kept whenever it
*might* contribute. A false positive costs one wasted request during
harvesting; a false negative would lose data silently.

Profiles need not come from SPHINX — shapes written by sheXer, QSE, SHACLGen
and similar tools are read too.

### 3. Harvest and answer (SCARAB)

```python
from nile.scarab.scarab_harvester import FindBGPPriority, execute_sparql_query

FindBGPPriority(query, [f"sparql:{url}" for url in responsive],
                base_named_graph="urn:nile:run:2026-06-19-a")

results = execute_sparql_query(query)
```

`FindBGPPriority` returns nothing; its effect is the triples now sitting in the
local store. Query the store afterwards to get the answers.
`execute_sparql_query` returns `None` on failure and `[]` when a query simply
matched nothing, so test for `None` if you need to tell the two apart.

Pass a fresh `base_named_graph` per run. It defaults to `urn:tpf:run`, and
reusing it makes two runs indistinguishable in the store.

---

## Declaring sources

A bare `http(s)` location is read as a **fragments server**. A SPARQL endpoint
must say so explicitly, because the two cannot be told apart by their URL and
mistaking one for the other yields empty fragments rather than an error.

```python
FindBGPPriority(query, [
    "http://example.org/fragments",       # TPF or QPF server
    "sparql:http://example.org/sparql",   # SPARQL endpoint
    "dump:/data/repository.ttl",          # local RDF file
    {"type": "dump", "location": "/data/other.nq", "format": "nquads"},
])
```

Anything carrying a recognised RDF extension, a `file://` URL, or a path that
exists on disk is taken for a dump.

A SPARQL endpoint declared this way is never handed the query. It is asked for
one triple pattern at a time, exactly as a fragments server would be, so an
endpoint able to answer only part of a query still contributes everything it
holds for the rest.

---

## Provenance

Every harvesting run mints one nanopublication per source, stored in the local
triplestore alongside the data. Each records which triples came from which
source, through what kind of interface, under which triple patterns, and when.

The graph the harvested triples are written into *is* that nanopublication's
assertion graph, so nothing is stored twice. To read a run's data back without
attribution:

```sparql
PREFIX np: <http://www.nanopub.org/nschema#>
SELECT ?s ?p ?o WHERE {
  ?np np:hasAssertion ?assertionGraph .
  GRAPH ?assertionGraph { ?s ?p ?o }
}
```

Joining the provenance graph against the assertion graph on their shared base
IRI annotates each row with the source it came from.

---

## Examples

`examples/` holds three runnable scripts, one per module, and a corpus of
SPARQL queries used during evaluation:

| Directory | Queries | Benchmark |
| --- | --- | --- |
| `examples/queries/berlin` | 8 | Berlin SPARQL Benchmark |
| `examples/queries/sp2bench` | 17 | SP²Bench |
| `examples/queries/watdiv` | 20 | WatDiv |
| `examples/queries/LargeRDFBench` | 14 | LargeRDFBench |

The scripts carry hard-coded endpoint URLs and paths from the development
machine; edit those before running them.

---

## Known limitations

- **Optional patterns are treated as required.** Both RIDDLE and SCARAB descend
  into every part of the query algebra, so a pattern under `OPTIONAL` is
  collected on the same footing as one the query requires. For source selection
  this is intended — a repository holding only the optional part is still worth
  asking — but it does mean more is retrieved than a strict reading would.

- **Property paths are evaluated locally.** A fragments server cannot be asked
  for the pairs of resources joined by an arbitrary-length chain, so SCARAB
  retrieves the full extent of a path's base predicates and evaluates the
  expression over what it has. This can be expensive; see
  `SCARAB_HARVEST_PATH_BASE_PREDICATES`.

- **Fragments servers must paginate deterministically.** SCARAB follows a
  fragment's pagination links assuming successive pages cover it exactly once.
  A server whose backing store returns solutions in a different order on each
  request will silently skip some triples and repeat others. A fix for the
  Node.js server is available at
  [NodeJS-Triple-Pattern-Fragments-Server-fix-for-deterministic-paging](https://github.com/Acb897/NodeJS-Triple-Pattern-Fragments-Server-fix-for-deterministic-paging).

- **One run per process.** SCARAB's page cache, ingestion queue and daemon
  thread are module-level and shared across the process.

- **Blank nodes are skipped.** A blank node retrieved from a remote source has
  no identity outside the document that produced it, so it cannot be joined
  against anything afterwards. SPHINX likewise excludes blank-node classes.

---

## License

MIT. See [LICENSE](LICENSE).

## Author

Alberto Cámara — <https://github.com/Acb897/NILE>