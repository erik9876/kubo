package measure

// Event type strings used as the discriminator in JSONL log files. Each value
// also doubles as the on-disk filename (<value>.jsonl). The strings are the
// stable contract with the offline pandas analysis in phase 6 — do not rename
// without updating PLAN.md and any downstream summary scripts.
const (
	// Emitted by all three modes (lurker, pinger, ponger).

	// Periodic snapshot of node state: peerstore size, active connection count,
	// uptime. Cadence is cfg.SampleInterval.
	EventStateSample = "state-sample"

	// Connection lifecycle events from libp2p's network.Notifiee. Offline join
	// open+close by remote peer ID and remote multiaddr to derive lifetimes.
	EventConnOpen  = "conn-open"
	EventConnClose = "conn-close"

	// One entry per completed identify roundtrip: envelope presence, seq counter,
	// number of certified multiaddrs vs. listen addrs, agent version.
	EventPeerRecord = "peer-record"

	// One entry per terminated DHT lookup, aggregated by lookup UUID. Carries
	// the origin tag set via dht.WithLookupOrigin so background lookups and
	// measurement-induced lookups can be separated in analysis.
	EventDHTLookup = "dht-lookup"

	// Pinger-only events.

	// One entry per pinger tick that resulted in an actual OOB query send.
	// Carries query_id, strategy, target, total latency, done status.
	EventPingerQuery = "pinger-query"

	// Emitted when a tick is skipped because the previous send is still
	// outstanding (prev-pending). Sequencer is NOT advanced for these.
	EventQuerySkip = "query-skip"

	// Emitted when the chosen strategy returned no candidate (empty pool).
	// Sequencer IS advanced for these — the strategy was attempted.
	EventTargetSkip = "target-skip"

	// Ponger-only events.

	// One entry per processed query: case classification (I / II_ok /
	// II_fail_III / III), per-phase latencies, state snapshot, errors.
	// Joinable with EventPingerQuery by query_id.
	EventPongerQuery = "ponger-query"

	// Per-receive marker emitted in handleConn before queuing the query for the
	// worker. Redundant with EventPongerQuery in steady state (1:1 mapping); kept
	// for debugging when the worker hangs and the per-query timestamps diverge.
	EventPongerQueryRecv = "ponger-query-recv"

	// Periodic snapshots of internal drop counters. Emitted every 60s plus once
	// at shutdown. logger-dropped is the count of Log() calls that found the
	// event channel full; lookup-dropped is the count of DHT LookupEvents that
	// the subscriber's onEvent dropped because its own channel was full.
	// Without these, a busy 24h run could lose events silently.
	EventLoggerDropped = "logger-dropped"
	EventLookupDropped = "lookup-dropped"

	// Periodic snapshot of incoming DHT-query counts per message type (PDF S.4
	// optional: "eingehende DHT-Queries als Counter zur Charakterisierung der
	// Gesamtaktivität"). Emitted every 60s plus once at shutdown. Counters are
	// monotonic; diff consecutive samples for per-window rates.
	EventDHTIncoming = "dht-incoming"
)
