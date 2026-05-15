package measure

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

type OOBPinger struct {
	partnerAddr string
	logger      *Logger

	// Send-side: queries from SendQuery() get pushed here,
	// current writeLoop drains it onto the active conn.
	sendCh chan oobQuery

	// Pending-Maps: keyed by query_id.
	// ackCh: closed by readLoop when Ack arrives.
	// doneCh: receives the Done message (one-shot).
	pendingAcks  sync.Map // map[string]chan struct{}
	pendingDones sync.Map // map[string]chan oobDone
}

func newOOBPinger(partnerAddr string, logger *Logger) *OOBPinger {
	return &OOBPinger{
		partnerAddr: partnerAddr,
		logger:      logger,
		sendCh:      make(chan oobQuery, 8),
	}
}

func (pc *OOBPinger) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	backoff := time.Second
	const backoffMax = 30 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := net.Dial("tcp", pc.partnerAddr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "measure: oob dial failed: %v (retry in %s)\n", err, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > backoffMax {
				backoff = backoffMax
			}
			continue
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetKeepAlive(true)
			tcpConn.SetKeepAlivePeriod(30 * time.Second)
		}
		fmt.Fprintf(os.Stderr, "measure: oob-connected to %s\n", pc.partnerAddr)
		backoff = time.Second // reset after successful connect

		pc.serveConn(ctx, conn)

		fmt.Fprintf(os.Stderr, "measure: oob-disconnected from %s\n", pc.partnerAddr)
		pc.failAllPending()
	}
}

func (pc *OOBPinger) serveConn(parentCtx context.Context, conn net.Conn) {
	defer conn.Close()

	connCtx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	var writeMu sync.Mutex
	var loopWg sync.WaitGroup

	// Close conn when ctx fires, so readLoop's scanner unblocks.
	go func() {
		<-connCtx.Done()
		conn.Close()
	}()

	loopWg.Add(2)
	go pc.readLoop(cancel, conn, &loopWg)
	go pc.writeLoop(connCtx, cancel, conn, &writeMu, &loopWg)

	loopWg.Wait()
}

func (pc *OOBPinger) readLoop(cancel context.CancelFunc, conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	defer cancel()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg, err := decodeOOBMessage(scanner.Bytes())
		if err != nil {
			fmt.Fprintf(os.Stderr, "measure: oob decode error on pinger: %v\n", err)
			return
		}
		switch m := msg.(type) {
		case oobAck:
			if v, ok := pc.pendingAcks.Load(m.QueryID); ok {
				close(v.(chan struct{}))
				pc.pendingAcks.Delete(m.QueryID)
			}
		case oobDone:
			if v, ok := pc.pendingDones.Load(m.QueryID); ok {
				// Non-blocking send: doneCh is buffered (size 1).
				select {
				case v.(chan oobDone) <- m:
				default:
				}
			}
		default:
			// Pinger receives only ack/done. Anything else is a protocol error.
			return
		}
	}
}

func (pc *OOBPinger) writeLoop(ctx context.Context, cancel context.CancelFunc, conn net.Conn, writeMu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return
		case q := <-pc.sendCh:
			writeMu.Lock()
			err := writeJSONLine(conn, q)
			writeMu.Unlock()
			if err != nil {
				fmt.Fprintf(os.Stderr, "measure: oob write query failed: %v\n", err)
				return
			}
		}
	}
}

func (pc *OOBPinger) failAllPending() {
	sentinel := oobDone{Type: oobTypeDone, OK: false, Err: "disconnected"}
	pc.pendingAcks.Range(func(k, v any) bool {
		// Close the ackCh so any waiter unblocks. SendQuery's logic
		// then sees the disconnected sentinel via failed doneCh.
		close(v.(chan struct{}))
		pc.pendingAcks.Delete(k)
		return true
	})
	pc.pendingDones.Range(func(k, v any) bool {
		select {
		case v.(chan oobDone) <- sentinel:
		default:
		}
		pc.pendingDones.Delete(k)
		return true
	})
}

func (pc *OOBPinger) SendQuery(q oobQuery) oobDone {
	ackCh := make(chan struct{})
	doneCh := make(chan oobDone, 1)
	pc.pendingAcks.Store(q.QueryID, ackCh)
	pc.pendingDones.Store(q.QueryID, doneCh)
	defer pc.pendingAcks.Delete(q.QueryID)
	defer pc.pendingDones.Delete(q.QueryID)

	// Enqueue for writeLoop. Bounded wait so we don't hang forever
	// if writeLoop has exited but failAllPending hasn't run yet.
	select {
	case pc.sendCh <- q:
	case <-time.After(5 * time.Second):
		return oobDone{Type: oobTypeDone, QueryID: q.QueryID, OK: false, Err: "send-timeout"}
	}

	// Phase 1: wait for Ack.
	select {
	case <-ackCh:
		// ok, continue to phase 2
	case <-time.After(5 * time.Second):
		return oobDone{Type: oobTypeDone, QueryID: q.QueryID, OK: false, Err: "ack-timeout"}
	}

	// Phase 2: wait for Done.
	select {
	case d := <-doneCh:
		return d
	case <-time.After(100 * time.Second):
		return oobDone{Type: oobTypeDone, QueryID: q.QueryID, OK: false, Err: "done-timeout"}
	}
}
