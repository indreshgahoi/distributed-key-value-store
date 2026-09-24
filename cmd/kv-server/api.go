package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/consensus/raft"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/replica"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/mvcc"
)

const (
	// requestTimeout bounds how long a write waits to commit and apply, and a
	// read waits to confirm leadership and catch up, before failing.
	requestTimeout = 2 * time.Second
	maxBodyBytes   = 1 << 20

	// latestTS reads the newest version of every key.
	latestTS = ^uint64(0)
)

// api serves the client HTTP interface:
//
//	POST /put     {"key": "...", "value": "..."}   replicated write
//	POST /delete  {"key": "..."}                   replicated delete
//	GET  /get?key=...[&consistency=stale]          linearizable read by default
//	GET  /status                                   node state, for operators
//
// Writes and linearizable reads must go to the leader; other nodes answer
// 307 with a Location header when they know the leader's address, else 503.
type api struct {
	node      *raft.RaftNode
	store     *mvcc.Store
	proposals *replica.ProposalTracker
	httpPeers map[uint64]string
}

func (a *api) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /put", a.handlePut)
	mux.HandleFunc("POST /delete", a.handleDelete)
	mux.HandleFunc("GET /get", a.handleGet)
	mux.HandleFunc("GET /status", a.handleStatus)
	return mux
}

func (a *api) handlePut(w http.ResponseWriter, r *http.Request) {
	var req struct{ Key, Value string }
	if !decodeBody(w, r, &req) {
		return
	}
	a.replicate(w, r, replica.Command{Op: replica.OpPut, Key: req.Key, Value: req.Value})
}

func (a *api) handleDelete(w http.ResponseWriter, r *http.Request) {
	var req struct{ Key string }
	if !decodeBody(w, r, &req) {
		return
	}
	a.replicate(w, r, replica.Command{Op: replica.OpDelete, Key: req.Key})
}

// replicate proposes cmd and responds once it is applied - never earlier,
// because until then a leadership change could still discard it.
func (a *api) replicate(w http.ResponseWriter, r *http.Request, cmd replica.Command) {
	if cmd.Key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	index, result, isLeader := a.proposals.Propose(a.node, cmd.Encode())
	if !isLeader {
		a.redirectToLeader(w, r)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	select {
	case err := <-result:
		switch {
		case errors.Is(err, replica.ErrProposalSuperseded):
			writeError(w, http.StatusConflict, err.Error())
		case err != nil:
			writeError(w, http.StatusBadRequest, err.Error())
		default:
			writeJSON(w, http.StatusOK, map[string]any{"status": "committed", "index": index})
		}
	case <-ctx.Done():
		a.proposals.Cancel(index)
		writeError(w, http.StatusGatewayTimeout, "commit timeout: outcome unknown, safe to retry")
	}
}

func (a *api) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key parameter is required")
		return
	}
	// consistency=stale serves this node's local state immediately: it may
	// lag the leader, but works on any node and needs no network round trip.
	if r.URL.Query().Get("consistency") != "stale" {
		if !a.awaitLinearizable(w, r) {
			return
		}
	}
	val, err := a.store.Get([]byte(key), latestTS)
	if errors.Is(err, mvcc.ErrKeyNotFound) {
		writeError(w, http.StatusNotFound, "key not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": string(val)})
}

// awaitLinearizable runs Read Index: confirm leadership with a quorum, then
// wait until the local store has applied everything committed before the
// read arrived. Reports false (having responded) on failure.
func (a *api) awaitLinearizable(w http.ResponseWriter, r *http.Request) bool {
	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	readIndex, err := a.node.ReadIndex(ctx)
	if errors.Is(err, raft.ErrNotLeader) {
		a.redirectToLeader(w, r)
		return false
	}
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("cannot confirm linearizable read: %v", err))
		return false
	}
	if err := a.node.WaitApplied(ctx, readIndex); err != nil {
		writeError(w, http.StatusGatewayTimeout, "timed out waiting for the state machine to catch up")
		return false
	}
	return true
}

func (a *api) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := a.node.Status()
	used, capacity, _ := a.store.MemoryUsage()
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":        st.ID,
		"term":           st.Term,
		"role":           st.Role.String(),
		"is_leader":      st.Role == raft.RoleLeader,
		"leader_id":      st.Leader,
		"commit_index":   st.CommitIndex,
		"last_applied":   st.LastApplied,
		"last_log_index": st.LastIndex,
		"snapshot_index": st.Snapshot,
		"store_bytes":    used,
		"store_capacity": capacity,
	})
}

// redirectToLeader points the client at the leader if we know where it is.
func (a *api) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leader := a.node.Leader()
	if addr, ok := a.httpPeers[leader]; ok && leader != 0 {
		w.Header().Set("Location", "http://"+addr+r.URL.RequestURI())
		writeJSON(w, http.StatusTemporaryRedirect, map[string]any{"error": "not leader", "leader_id": leader})
		return
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "not leader", "leader_id": leader})
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes)).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
