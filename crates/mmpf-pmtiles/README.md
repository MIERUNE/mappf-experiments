# mmpf-pmtiles

`mmpf-pmtiles` is a flexible, customizable building block for reading
PMTiles v3 archives. Rather than imposing a storage, caching, routing, or
transport architecture, it owns the format rules, directory traversal, metadata
decoding, range-read contract, and resource limits while exposing extension
points for integration into different systems.

The main extension points are:

- `ArchiveBackend` for services that already own decoded index caches,
  single-flight, or peer routing;
- `RangeSource` for files, HTTP, object storage, chunk caches, or layered reads;
- `DirectoryStore` for no cache, process-local caches, or shared cache adapters;
- `ReadObserver` for metrics, traces, tests, and simulator input;
- `ReaderLimits` for bootstrap overfetch, traversal depth, and decoded sizes.

`DirectoryWalker` exposes the validated, I/O-independent traversal state
machine separately. `ArchiveReader::with_backend` is the higher-level form of
the same separation: distributed services can retain their peer routing and
single-flight policy while delegating tile-id validation, section bounds,
directory traversal, access traces, and tile assembly to this crate.

The standard raw-range `ArchiveReader` constructors bind the reader to an
`ArchiveIdentity` containing both a stable name and an immutable generation.
Use an ETag, object version, or content digest as the generation so replacing an
object at the same path cannot reuse stale directory offsets.

Ishikari remains responsible for mapping a tileset id to a source, peer routing,
negative caching, admission policy, and translating source errors to HTTP.
