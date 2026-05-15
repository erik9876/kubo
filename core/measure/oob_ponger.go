package measure

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

type OOBPonger struct {
	addr      string
	logger    *Logger
	listener  net.Listener
	host      host.Host
	dht       *dht.IpfsDHT
	tracker   *PeerstoreTracker
	startTime time.Time
}

func newOOBPonger(addr string, logger *Logger, h host.Host, d *dht.IpfsDHT, tracker *PeerstoreTracker) *OOBPonger {
	return &OOBPonger{
		addr:      addr,
		logger:    logger,
		host:      h,
		dht:       d,
		tracker:   tracker,
		startTime: time.Now(),
	}
}

func (ps *OOBPonger) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ln, err := net.Listen("tcp", ps.addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "measure: oob listen failed on ponger: %v\n", err)
		return
	}
	ps.listener = ln

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	var connWg sync.WaitGroup
	defer connWg.Wait()
	for {
		conn, err := ln.Accept()
		if err != nil {
			// Suppress the "use of closed network connection" error
			// triggered by our own shutdown bridge above.
			if ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "measure: tcp accept failed on ponger: %v\n", err)
			}
			return
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}
		connWg.Add(1)
		go ps.handleConn(ctx, conn, &connWg)
	}
}

func (ps *OOBPonger) handleConn(ctx context.Context, conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer conn.Close()

	var writeMu sync.Mutex
	queryCh := make(chan oobQuery, 8)
	connCtx, cancel := context.WithCancel(ctx)

	var workerWg sync.WaitGroup
	defer workerWg.Wait()
	defer cancel()

	// Close conn when parent ctx (service shutdown) fires,
	// so the Scanner below unblocks.
	go func() {
		<-connCtx.Done()
		conn.Close()
	}()

	workerWg.Add(1)
	go ps.runWorker(connCtx, cancel, conn, &writeMu, queryCh, &workerWg)

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg, err := decodeOOBMessage(scanner.Bytes())
		if err != nil {
			fmt.Fprintf(os.Stderr, "measure: oob decode error: %v\n", err)
			return
		}
		switch m := msg.(type) {
		case oobQuery:
			writeMu.Lock()
			err := writeJSONLine(conn, newOOBAck(m.QueryID))
			writeMu.Unlock()
			if err != nil {
				fmt.Fprintf(os.Stderr, "measure: oob write ack failed: %v\n", err)
				return
			}
			ps.logger.Log(EventPongerQueryRecv, m)
			queryCh <- m
		default:
			// Pinger only sends queries; anything else is protocol error.
			return
		}
	}
}

type PongerQueryPayload struct {
	QueryID   string  `json:"query_id"`
	Strategy  string  `json:"strategy"`
	TargetPID peer.ID `json:"target_pid,omitempty"`

	CasePath string `json:"case_path,omitempty"`
	TotalMs  int64  `json:"total_ms"`

	ConnectInitialMs   int64  `json:"connect_initial_ms,omitempty"`
	ConnectInitialErr  string `json:"connect_initial_err,omitempty"`
	LookupMs           int64  `json:"lookup_ms,omitempty"`
	LookupErr          string `json:"lookup_err,omitempty"`
	ConnectFallbackMs  int64  `json:"connect_fallback_ms,omitempty"`
	ConnectFallbackErr string `json:"connect_fallback_err,omitempty"`
	PeerstoreAgeMs     int64  `json:"peerstore_age_ms,omitempty"`

	// NumAddrs is the count of multiaddrs the ponger had for the target at
	// the point that determined the case. PLAN.md:547 contract:
	//   Case I        -> len(Peerstore.Addrs(target))     (known multiaddrs)
	//   Case II_ok    -> len(Peerstore.Addrs(target))     (used for connect)
	//   Case II_fail_III, III -> len(FindPeer.Addrs)      (from DHT result)
	// 0 in II_fail_III/III with lookup_err set means the lookup yielded nothing.
	NumAddrs int `json:"num_addrs"`

	UptimeMs      uint64 `json:"uptime_ms"`
	PeerstoreSize int    `json:"peerstore_size"`
	ActiveConns   int    `json:"active_conns"`

	ParseErr string `json:"parse_err,omitempty"`
}

func (ps *OOBPonger) runWorker(ctx context.Context, cancel context.CancelFunc, conn net.Conn, writeMu *sync.Mutex, queryCh <-chan oobQuery, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case q, ok := <-queryCh:
			if !ok {
				return
			}
			payload, done := ps.handleQuery(ctx, q)
			ps.logger.Log(EventPongerQuery, payload)

			writeMu.Lock()
			err := writeJSONLine(conn, done)
			writeMu.Unlock()
			if err != nil {
				fmt.Fprintf(os.Stderr, "measure: oob write done failed: %v\n", err)
				cancel()
				return
			}
		}
	}
}

func (ps *OOBPonger) handleQuery(ctx context.Context, q oobQuery) (PongerQueryPayload, oobDone) {
	payload := PongerQueryPayload{
		QueryID:       q.QueryID,
		Strategy:      q.Strategy,
		UptimeMs:      uint64(time.Since(ps.startTime).Milliseconds()),
		PeerstoreSize: len(ps.host.Peerstore().Peers()),
		ActiveConns:   len(ps.host.Network().Conns()),
	}

	target, err := peer.Decode(q.TargetPID)
	if err != nil {
		payload.ParseErr = err.Error()
		return payload, newOOBDone(q.QueryID, false, "parse: "+err.Error())
	}
	payload.TargetPID = target

	if len(ps.host.Network().ConnsToPeer(target)) > 0 {
		payload.CasePath = "I"
		payload.TotalMs = 0
		payload.NumAddrs = len(ps.host.Peerstore().Addrs(target))
		return payload, newOOBDone(q.QueryID, true, "")
	}

	addrs := ps.host.Peerstore().Addrs(target)
	if len(addrs) > 0 {
		if firstSeen, ok := ps.tracker.FirstSeen(target); ok {
			payload.PeerstoreAgeMs = time.Since(firstSeen).Milliseconds()
		}

		connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		start := time.Now()
		initErr := ps.host.Connect(connectCtx, peer.AddrInfo{ID: target, Addrs: addrs})
		payload.ConnectInitialMs = time.Since(start).Milliseconds()
		cancel()

		if initErr == nil {
			payload.CasePath = "II_ok"
			payload.NumAddrs = len(addrs)
			payload.TotalMs = payload.ConnectInitialMs
			return payload, newOOBDone(q.QueryID, true, "")
		}

		payload.CasePath = "II_fail_III"
		payload.ConnectInitialErr = initErr.Error()
		ps.findPeerAndConnect(ctx, q.Strategy+"-fallback", target, &payload)
		payload.TotalMs = payload.ConnectInitialMs + payload.LookupMs + payload.ConnectFallbackMs
		ok, errStr := forwardingOutcome(&payload)
		return payload, newOOBDone(q.QueryID, ok, errStr)
	}

	payload.CasePath = "III"
	ps.findPeerAndConnect(ctx, q.Strategy, target, &payload)
	payload.TotalMs = payload.LookupMs + payload.ConnectFallbackMs
	ok, errStr := forwardingOutcome(&payload)
	return payload, newOOBDone(q.QueryID, ok, errStr)
}

func forwardingOutcome(payload *PongerQueryPayload) (bool, string) {
	if payload.LookupErr != "" {
		return false, "lookup: " + payload.LookupErr
	}
	if payload.ConnectFallbackErr != "" {
		return false, "connect: " + payload.ConnectFallbackErr
	}
	return true, ""
}

func (ps *OOBPonger) findPeerAndConnect(parent context.Context, originSuffix string, target peer.ID, payload *PongerQueryPayload) {
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	ctx = dht.WithLookupOrigin(ctx, "measurement-"+originSuffix)

	lookupStart := time.Now()
	addrInfo, lookupErr := ps.dht.FindPeer(ctx, target)
	payload.LookupMs = time.Since(lookupStart).Milliseconds()

	if lookupErr != nil {
		payload.LookupErr = lookupErr.Error()
		return
	}
	if len(addrInfo.Addrs) == 0 {
		payload.LookupErr = "no addrs"
		return
	}
	payload.NumAddrs = len(addrInfo.Addrs)

	connectStart := time.Now()
	connErr := ps.host.Connect(ctx, addrInfo)
	payload.ConnectFallbackMs = time.Since(connectStart).Milliseconds()
	if connErr != nil {
		payload.ConnectFallbackErr = connErr.Error()
	}
}

func writeJSONLine(w io.Writer, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(append(data, '\n'))
	return err
}
