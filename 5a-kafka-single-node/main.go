package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendRequest struct {
	Key   string `json:"key"`
	Value int    `json:"msg"`
}

type SendResponse struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

type PollRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type LogEntry [2]int

type PollResponse struct {
	Type     string                `json:"type"`
	Messages map[string][]LogEntry `json:"msgs"`
}

type CommitOffsetsRequest struct {
	Offsets map[string]int `json:"offsets"`
}

type ListCommittedOffsetsRequest struct {
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsResponse struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func IsMaelstromError(err error, code int) bool {
	target := (&maelstrom.RPCError{})
	return errors.As(err, &target) && target.Code == code
}

type State struct {
	mu   sync.RWMutex
	node *maelstrom.Node
	kv   *maelstrom.KV
	ctx  context.Context

	log     map[string][]int
	commits map[string]int
}

func NewState(node *maelstrom.Node, kv *maelstrom.KV) State {
	return State{node: node, kv: kv, ctx: context.Background(), log: make(map[string][]int), commits: make(map[string]int)}
}

func (s *State) Append(k string, v int) (offset int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.log[k]; !ok {
		s.log[k] = []int{}
	}

	s.log[k] = append(s.log[k], v)
	return len(s.log[k]) - 1
}

func (s *State) ReadFromOffset(k string, offset int) (result []LogEntry) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i, n := range s.log[k][offset:] {
		result = append(result, [2]int{offset + i, n})
	}

	return
}

func (s *State) CommitOffset(k string, offset int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	prevOffset, ok := s.commits[k]

	if !ok {
		s.commits[k] = offset
		return
	}

	if offset > prevOffset {
		s.commits[k] = offset
		return
	}
}

func (s *State) GetCommittedOffset(k string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.commits[k]
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	state := NewState(n, kv)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offset := state.Append(body.Key, body.Value)

		return n.Reply(msg, SendResponse{Type: "send_ok", Offset: offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body PollRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var response PollResponse
		response.Type = "poll_ok"
		response.Messages = make(map[string][]LogEntry)

		for k, offset := range body.Offsets {
			response.Messages[k] = state.ReadFromOffset(k, offset)
		}

		return n.Reply(msg, response)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body CommitOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for k, offset := range body.Offsets {
			state.CommitOffset(k, offset)
		}

		response := make(map[string]any)
		response["type"] = "commit_offsets_ok"

		return n.Reply(msg, response)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body ListCommittedOffsetsRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		var response ListCommittedOffsetsResponse
		response.Type = "list_committed_offsets_ok"
		response.Offsets = make(map[string]int)

		for _, k := range body.Keys {
			response.Offsets[k] = state.GetCommittedOffset(k)
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
