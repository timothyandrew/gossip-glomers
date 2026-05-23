package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type State struct {
	mu   sync.RWMutex
	data []int
}

func (s *State) Add(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data, n)
}

func (s *State) Get() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data
}

func main() {
	n := maelstrom.NewNode()
	state := State{}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"].(float64)
		state.Add(int(message))

		response := make(map[string]any)
		response["type"] = "broadcast_ok"
		response["in_reply_to"] = body["msg_id"]

		return n.Reply(msg, response)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = state.Get()

		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		response := make(map[string]any)
		response["type"] = "topology_ok"
		response["in_reply_to"] = body["msg_id"]

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
