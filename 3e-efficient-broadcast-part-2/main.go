package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BatchBroadcastMessage struct {
	Type    string `json:"type"`
	Message []int  `json:"message"`
}

type TopologyMessage struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
	MsgID    int                 `json:"msg_id"`
}

type State struct {
	mu        sync.RWMutex
	neighbors []string
	node      *maelstrom.Node

	data   []int
	lookup map[int]bool
}

func NewState(node *maelstrom.Node) State {
	return State{lookup: make(map[int]bool), node: node}
}

func (s *State) Add(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.lookup[n]; ok {
		return
	}

	s.data = append(s.data, n)
	s.lookup[n] = true
}

func (s *State) RegisterNeighbors(neighbors []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.neighbors = neighbors
}

func (s *State) Get() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.data
}

func (s *State) Broadcast() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, n := range s.neighbors {
		err := s.node.Send(n, BatchBroadcastMessage{
			Type:    "broadcast_batch",
			Message: s.data,
		})

		if err != nil {
			fmt.Printf("Broadcast error: %s\n", err)
		}
	}

	return nil
}

func main() {
	n := maelstrom.NewNode()
	state := NewState(n)

	broadcast := time.NewTicker(time.Second)

	go func() {
		for {
			<-broadcast.C
			err := state.Broadcast()
			if err != nil {
				fmt.Printf("Broadcast error: %s\n", err)
			}
		}
	}()

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

	n.Handle("broadcast_batch", func(msg maelstrom.Message) error {
		var body BatchBroadcastMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for _, n := range body.Message {
			state.Add(n)
		}

		return nil
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
		var body TopologyMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		neighborsLookup := make(map[string]bool)

		for _, v := range body.Topology {
			for _, node := range v {
				neighborsLookup[node] = true
			}
		}

		neighbors := []string{}

		for node, _ := range neighborsLookup {
			neighbors = append(neighbors, node)
		}

		state.RegisterNeighbors(neighbors)

		response := make(map[string]any)
		response["type"] = "topology_ok"
		response["in_reply_to"] = body.MsgID

		return n.Reply(msg, response)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
