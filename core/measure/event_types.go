package measure

const (
	// Alle drei Nodes — kontinuierlich
	EventStateSample = "state-sample" // Peerstore-Größe, Connection-Count, Uptime
	EventConnOpen    = "conn-open"    // Connection geöffnet (für Lifetime-Verteilung)
	EventConnClose   = "conn-close"   // Connection geschlossen

	// Alle drei Nodes — Phase 2
	EventDHTLookup = "dht-lookup" // ausgehender Lookup mit Origin-Tag (background vs. mess-induziert)

	// Pinger only
	EventQuerySent = "query-sent" // welche PID, welche Strategie (RT/PS/Random-DHT), query_id

	// Ponger only
	EventCaseResult = "case-result" // Case I/II/III, Latenz, Stale-Rate, query_id zur Korrelation
	EventPeerRecord = "peer-record" // Envelope vorhanden, Signatur valide, Seq, Anzahl Multiaddrs
)
