# Empirical study analysis

`analyze_run.py` recomputes the figures, tables and prose numbers of the
empirical study from the JSONL logs a measurement run writes.

    python analyze_run.py <rundir> [--style plot.mplstyle]

Figures land in `<rundir>/out/`, everything else goes to stdout. Needs pandas,
numpy and matplotlib. The run data itself is not published; `expected-output.txt`
holds what the run behind the thesis printed.

## Expected layout

    <rundir>/pinger-a/*.jsonl    node A, selects the probed peers
    <rundir>/ponger-b/*.jsonl    node B, resolves and classifies them
    <rundir>/lurker-c/*.jsonl    node C, passive baseline

## Log format

One event per line: a `Timestamp`, a `Type`, and a `Payload` whose keys become
dataframe columns.

```json
{"Timestamp":"2026-05-15T16:49:41.716014092Z","Type":"state-sample","Payload":{"peerstore_size":1431,"peers_with_addrs":1367,"active_conns":407,"transport_counts":{"quic":259,"tcp":147,"unknown":1},"uptime_ms":30006}}
```

A run writes more types than this; four of them are read here.

| file | written by | one record is |
| --- | --- | --- |
| `dht-lookup.jsonl` | `lookup_subscriber.go` | one finished outgoing DHT lookup |
| `ponger-query.jsonl` | `oob_ponger.go` | one forwarding probe resolved by B |
| `state-sample.jsonl` | `state_sampler.go` | peerstore and connection counts, every 30 s |
| `conn-close.jsonl` | `conn_notifiee.go` | one closed libp2p connection |

Fields the analysis depends on:

- `dht-lookup`: `num_queried`; `origin`, empty for maintenance traffic and
  `measurement-<strategy>` for probe-induced lookups; `terminate_reason`,
  where `stopped` means the lookup's stopFn fired, so `FindPeer` reached its
  target, while `completed` means the walk converged without it
- `ponger-query`: `strategy`, `case_path` (`I`, `II_ok`, `II_fail_III`, `III`),
  `total_ms`, `lookup_err`, `connect_fallback_err`, `peerstore_age_ms`
- `state-sample`: `peers_with_addrs`, `active_conns`
- `conn-close`: `open_time`, subtracted from `Timestamp` for the lifetime
