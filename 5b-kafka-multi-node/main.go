package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

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
	node *maelstrom.Node
	ctx  context.Context

	// Store log data
	seqKV *maelstrom.KV

	// Store committed offsets + latest offset per log
	linKV *maelstrom.KV
}

func NewState(node *maelstrom.Node, seqKV, linKV *maelstrom.KV) State {
	return State{node: node, seqKV: seqKV, linKV: linKV, ctx: context.Background()}
}

func (s *State) getOffsetKey(k string) string {
	return fmt.Sprintf("latest_offset_%s", k)
}

func (s *State) getDataKey(k string, offset int) string {
	return fmt.Sprintf("data_%s_%d", k, offset)
}

func (s *State) getCommittedOffsetKey(k string) string {
	return fmt.Sprintf("committed_offset_%s", k)
}

func (s *State) getWriteOffset(k string) (int, error) {
	for {
		// Get current latest offset
		offset, err := s.linKV.ReadInt(s.ctx, s.getOffsetKey(k))
		if IsMaelstromError(err, maelstrom.KeyDoesNotExist) {
			// Do nothing
		} else if err != nil {
			return 0, err
		}

		// Increment offset for this write
		err = s.linKV.CompareAndSwap(s.ctx, s.getOffsetKey(k), offset, offset+1, true)
		if IsMaelstromError(err, maelstrom.PreconditionFailed) {
			// This offset is now taken, try again
			continue
		} else if err != nil {
			return 0, err
		}

		return offset + 1, nil
	}
}

func (s *State) Append(k string, v int) (int, error) {
	offset, err := s.getWriteOffset(k)
	if err != nil {
		return 0, err
	}

	err = s.seqKV.Write(s.ctx, s.getDataKey(k, offset), v)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func (s *State) ReadFromOffset(k string, offset int) ([]LogEntry, error) {
	latestOffset, err := s.linKV.ReadInt(s.ctx, s.getOffsetKey(k))
	if IsMaelstromError(err, maelstrom.KeyDoesNotExist) {
		return []LogEntry{}, nil
	} else if err != nil {
		return nil, err
	}

	results := []LogEntry{}

	for i := offset; i <= latestOffset; i++ {
		v, err := s.seqKV.ReadInt(s.ctx, s.getDataKey(k, i))
		if IsMaelstromError(err, maelstrom.KeyDoesNotExist) {
			continue
		} else if err != nil {
			return nil, err
		}

		results = append(results, [2]int{offset + i, v})
	}

	return results, nil
}

func (s *State) CommitOffset(k string, offset int) error {
	for {
		// Get current committed offset
		prevOffset, err := s.linKV.ReadInt(s.ctx, s.getCommittedOffsetKey(k))
		if IsMaelstromError(err, maelstrom.KeyDoesNotExist) {
			// Do nothing
		} else if err != nil {
			return err
		}

		// Replace committed offset if the new offset is newer
		if offset <= prevOffset {
			return nil
		}

		err = s.linKV.CompareAndSwap(s.ctx, s.getCommittedOffsetKey(k), prevOffset, offset, true)
		if IsMaelstromError(err, maelstrom.PreconditionFailed) {
			// Another commit has happened concurrently, try again
			continue
		} else if err != nil {
			return err
		}

		return nil
	}
}

func (s *State) GetCommittedOffset(k string) (int, error) {
	return s.linKV.ReadInt(s.ctx, s.getCommittedOffsetKey(k))
}

func main() {
	n := maelstrom.NewNode()
	seqKV := maelstrom.NewSeqKV(n)
	linKV := maelstrom.NewLinKV(n)
	state := NewState(n, seqKV, linKV)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body SendRequest
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offset, err := state.Append(body.Key, body.Value)
		if err != nil {
			return err
		}

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
			messages, err := state.ReadFromOffset(k, offset)
			if err != nil {
				return err
			}

			response.Messages[k] = messages
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
			offsets, err := state.GetCommittedOffset(k)
			if err != nil {
				return err
			}

			response.Offsets[k] = offsets
		}

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
