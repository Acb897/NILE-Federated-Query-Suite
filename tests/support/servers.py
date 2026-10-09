"""In-process servers for testing SCARAB without external services.

`FragmentsServer` imitates the patched Linked Data Fragments server used in
the thesis: Quad Pattern Fragments over a pyoxigraph store, deterministic
pagination, an unbound graph matching every graph (returned in the default
graph), and control metadata in a "#metadata" graph of a TriG response.

`SparqlServer` is a SPARQL endpoint over a pyoxigraph store that also
accepts N-Quads on a GraphDB-style ".../statements" route, and treats the
default graph as the union of all graphs when the request names no dataset
(as GraphDB does). It can therefore stand in both for a SPARQL source and
for the triplestore SCARAB publishes into.
"""

import json
import threading
import time
from collections import OrderedDict
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, urlencode

import pyoxigraph as ox

HYDRA = "http://www.w3.org/ns/hydra/core#"
VOID = "http://rdfs.org/ns/void#"


def _term(text):
    if text is None or text == "":
        return None
    if text.startswith("_:"):
        return ox.BlankNode(text[2:])
    if text.startswith('"'):
        return next(iter(ox.parse(f"<urn:x:s> <urn:x:p> {text} .", format=ox.RdfFormat.TURTLE))).object
    return ox.NamedNode(text.strip("<>"))


class _Server:
    def __init__(self, handler_cls):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
        self.httpd.daemon_threads = True
        self.httpd.owner = self
        self.requests = 0
        self._lock = threading.Lock()
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def count(self):
        with self._lock:
            self.requests += 1

    @property
    def base(self):
        host, port = self.httpd.server_address
        return f"http://{host}:{port}"

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()


class _FragmentsHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True   # as real servers do; avoids 40 ms delayed-ACK stalls

    def log_message(self, *args):
        pass

    def do_GET(self):
        srv = self.server.owner
        srv.count()
        if srv.latency:
            time.sleep(srv.latency)
        parsed = urlparse(self.path)
        params = {k: v[0] for k, v in parse_qs(parsed.query, keep_blank_values=True).items()}
        page = int(params.get("page", "1") or 1)
        selector = tuple(params.get(k) for k in ("subject", "predicate", "object", "graph"))
        matches = srv.matches(selector)
        size = srv.page_size
        chunk = matches[(page - 1) * size: page * size]
        url = f"{srv.base}{self.path}"
        fragment_params = {k: v for k, v in params.items() if k != "page"}
        fragment = f"{srv.base}{parsed.path}" + (f"?{urlencode(fragment_params)}" if fragment_params else "")
        meta = ox.NamedNode(f"{srv.base}{parsed.path}#metadata")
        quads = []
        for s, p, o, g in chunk:
            quads.append(ox.Quad(s, p, o, g if g is not None else ox.DefaultGraph()))
        total = ox.Literal(str(len(matches)), datatype=ox.NamedNode("http://www.w3.org/2001/XMLSchema#integer"))
        for subject in {url, fragment}:
            quads.append(ox.Quad(ox.NamedNode(subject), ox.NamedNode(HYDRA + "totalItems"), total, meta))
            quads.append(ox.Quad(ox.NamedNode(subject), ox.NamedNode(VOID + "triples"), total, meta))
        quads.append(ox.Quad(ox.NamedNode(url), ox.NamedNode(HYDRA + "itemsPerPage"),
                             ox.Literal(str(size), datatype=ox.NamedNode("http://www.w3.org/2001/XMLSchema#integer")), meta))
        if page * size < len(matches):
            nxt = dict(fragment_params, page=str(page + 1))
            quads.append(ox.Quad(ox.NamedNode(url), ox.NamedNode(HYDRA + "next"),
                                 ox.NamedNode(f"{srv.base}{parsed.path}?{urlencode(nxt)}"), meta))
        body = ox.serialize(quads, format=ox.RdfFormat.TRIG)
        self.send_response(200)
        self.send_header("Content-Type", "application/trig")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class FragmentsServer(_Server):
    """A Quad Pattern Fragments server over a pyoxigraph store."""

    def __init__(self, store, page_size=100, latency=0.0):
        super().__init__(_FragmentsHandler)
        self.store = store
        self.page_size = page_size
        self.latency = latency
        self._cache = OrderedDict()

    def matches(self, selector):
        with self._lock:
            if selector in self._cache:
                return self._cache[selector]
        s, p, o, g = (_term(x) for x in selector)
        if isinstance(s, ox.Literal) or (p is not None and not isinstance(p, ox.NamedNode)):
            return []
        if g is None:
            seen = {(q.subject, q.predicate, q.object) for q in self.store.quads_for_pattern(s, p, o, None)}
            rows = [(a, b, c, None) for a, b, c in seen]
        else:
            rows = [(q.subject, q.predicate, q.object, q.graph_name)
                    for q in self.store.quads_for_pattern(s, p, o, g)]
        rows.sort(key=lambda r: (str(r[0]), str(r[1]), str(r[2])))
        with self._lock:
            self._cache[selector] = rows
            if len(self._cache) > 50_000:
                self._cache.popitem(last=False)
        return rows


def _sparql_json(term):
    if isinstance(term, ox.NamedNode):
        return {"type": "uri", "value": term.value}
    if isinstance(term, ox.BlankNode):
        return {"type": "bnode", "value": term.value}
    out = {"type": "literal", "value": term.value}
    if term.language:
        out["xml:lang"] = term.language
    elif term.datatype.value != "http://www.w3.org/2001/XMLSchema#string":
        out["datatype"] = term.datatype.value
    return out


class _SparqlHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    disable_nagle_algorithm = True

    def log_message(self, *args):
        pass

    def _reply(self, code, body, ctype="application/sparql-results+json"):
        data = body if isinstance(body, bytes) else body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _query(self, query, params):
        srv = self.server.owner
        defaults = [ox.NamedNode(g) for g in params.get("default-graph-uri", [])]
        named = [ox.NamedNode(g) for g in params.get("named-graph-uri", [])]
        kwargs = {}
        if defaults:
            kwargs["default_graph"] = defaults
            kwargs["named_graphs"] = named or defaults
        elif srv.union_default_graph:
            kwargs["use_default_graph_as_union"] = True
        try:
            result = srv.store.query(query, **kwargs)
        except Exception as e:
            return self._reply(400, str(e), "text/plain")
        if isinstance(result, ox.QueryBoolean):
            return self._reply(200, json.dumps({"head": {}, "boolean": bool(result)}))
        names = [v.value for v in result.variables]
        rows = []
        for sol in result:
            rows.append({n: _sparql_json(sol[n]) for n in names if sol[n] is not None})
        self._reply(200, json.dumps({"head": {"vars": names}, "results": {"bindings": rows}}))

    def do_GET(self):
        self.server.owner.count()
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        self._query(params.get("query", [""])[0], params)

    def do_POST(self):
        srv = self.server.owner
        srv.count()
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if parsed.path.endswith("/statements"):
            srv.store.load(input=body, format=ox.RdfFormat.N_QUADS)
            return self._reply(204, b"", "text/plain")
        ctype = self.headers.get("Content-Type", "")
        if ctype.startswith("application/x-www-form-urlencoded"):
            form = parse_qs(body.decode("utf-8"))
            params.update(form)
            query = form.get("query", [""])[0]
        else:
            query = body.decode("utf-8")
        self._query(query, params)


class SparqlServer(_Server):
    """A SPARQL endpoint (and GraphDB-style statements route) over a store."""

    def __init__(self, store, union_default_graph=True):
        super().__init__(_SparqlHandler)
        self.store = store
        self.union_default_graph = union_default_graph

    @property
    def query_url(self):
        return f"{self.base}/repositories/test"

    @property
    def statements_url(self):
        return f"{self.base}/repositories/test/statements"
