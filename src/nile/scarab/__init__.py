"""SCARAB: retrieving the data and answering the query.

SCARAB takes a query and a set of repositories, and harvests each
repository on its own: the triples matching each of the query's patterns
are retrieved, kept with a nanopublication recording where they came
from, and the original query is evaluated afterwards over the union of
the harvests.

Retrieval and evaluation are kept apart on purpose. A repository that can
only answer part of the query still contributes the triples it holds, and
the repositories of a federation need not be reachable at the same time,
from the same network, or by the same party: harvests produced separately
(and exported as TriG files) can be gathered and evaluated together later.

Repositories may be fragments servers, SPARQL endpoints or RDF dump files.
All three are asked for one triple pattern at a time and are
interchangeable as far as the harvesting algorithm is concerned -- a
SPARQL endpoint is never handed the query as a whole.

The entry points are `nile.scarab.scarab_harvester.answer_query` and
`nile.scarab.scarab_harvester.FindBGPPriority`.
"""

