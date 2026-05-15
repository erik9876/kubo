package measure

import (
	"encoding/json"
	"fmt"
)

const (
	oobTypeQuery = "query"
	oobTypeAck   = "ack"
	oobTypeDone  = "done"
)

type oobQuery struct {
	Type      string `json:"type"`
	QueryID   string `json:"query_id"`
	Strategy  string `json:"strategy"`
	TargetPID string `json:"target_pid"`
}

type oobAck struct {
	Type    string `json:"type"`
	QueryID string `json:"query_id"`
}

type oobDone struct {
	Type    string `json:"type"`
	QueryID string `json:"query_id"`
	OK      bool   `json:"ok"`
	Err     string `json:"err"`
}

func newOOBQuery(qid, strat, pid string) oobQuery {
	return oobQuery{
		Type:      oobTypeQuery,
		QueryID:   qid,
		Strategy:  strat,
		TargetPID: pid,
	}
}

func newOOBAck(qid string) oobAck {
	return oobAck{
		Type:    oobTypeAck,
		QueryID: qid,
	}
}

func newOOBDone(qid string, ok bool, err string) oobDone {
	return oobDone{
		Type:    oobTypeDone,
		QueryID: qid,
		OK:      ok,
		Err:     err,
	}
}

func decodeOOBMessage(line []byte) (any, error) {
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(line, &probe); err != nil {
		return nil, err
	}
	switch probe.Type {
	case oobTypeQuery:
		var m oobQuery
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, err
		}
		return m, nil
	case oobTypeAck:
		var m oobAck
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, err
		}
		return m, nil
	case oobTypeDone:
		var m oobDone
		if err := json.Unmarshal(line, &m); err != nil {
			return nil, err
		}
		return m, nil
	default:
		return nil, fmt.Errorf("unknown oob message type: %q", probe.Type)
	}
}
