package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func IsMaelstromError(err error, code int) bool {
	target := (&maelstrom.RPCError{})
	return errors.As(err, &target) && target.Code == code
}

type State struct {
	mu   sync.RWMutex
	node *maelstrom.Node
	kv   *maelstrom.KV

	ctx context.Context

	value int
	delta int
}

func NewState(node *maelstrom.Node, kv *maelstrom.KV) State {
	return State{node: node, kv: kv, ctx: context.Background()}
}

func (s *State) Get() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.value + s.delta
}

func (s *State) Add(delta int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.delta += delta
}

func (s *State) PersistDelta() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		// Refresh base value
		value, err := s.kv.ReadInt(s.ctx, "counter")
		if IsMaelstromError(err, maelstrom.KeyDoesNotExist) {
			// Do nothing
		} else if err != nil {
			return err
		}
		s.value = value

		// Persist delta
		err = s.kv.CompareAndSwap(s.ctx, "counter", s.value, s.value+s.delta, true)
		if IsMaelstromError(err, maelstrom.PreconditionFailed) {
			// Base has changed, start over
			continue
		} else if err != nil {
			return err
		}

		break
	}

	s.value += s.delta
	s.delta = 0

	return nil
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	state := NewState(n, kv)

	persist := time.NewTicker(time.Second)

	go func() {
		for {
			<-persist.C
			err := state.PersistDelta()
			if err != nil {
				fmt.Printf("PERSIST ERROR: %s\n", err)
			}
		}
	}()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		delta := body["delta"].(float64)
		state.Add(int(delta))

		response := make(map[string]any)
		response["type"] = "add_ok"
		response["in_reply_to"] = body["msg_id"]

		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["value"] = state.Get()

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
