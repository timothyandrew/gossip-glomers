package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Txn struct {
	Op    string
	Key   int
	Value *int
}

func (t Txn) MarshalJSON() ([]byte, error) {
	return json.Marshal([3]any{t.Op, t.Key, t.Value})
}

func (t *Txn) UnmarshalJSON(data []byte) error {
	var raw [3]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[0], &t.Op); err != nil {
		return err
	}
	if err := json.Unmarshal(raw[1], &t.Key); err != nil {
		return err
	}
	return json.Unmarshal(raw[2], &t.Value)
}

type TxnMessage struct {
	Type  string `json:"type"`
	MsgId int    `json:"msg_id"`
	Txn   []Txn  `json:"txn"`
}

func IsMaelstromError(err error, code int) bool {
	target := (&maelstrom.RPCError{})
	return errors.As(err, &target) && target.Code == code
}

type State struct {
	mu   sync.Mutex
	node *maelstrom.Node
	ctx  context.Context

	storage map[int]int
}

func (s *State) ProcessTransaction(tx Txn) Txn {
	s.mu.Lock()
	defer s.mu.Unlock()

	if tx.Op == "r" {
		value := s.storage[tx.Key]
		tx.Value = &value
	}

	if tx.Op == "w" {
		s.storage[tx.Key] = *tx.Value
	}

	return tx
}

func NewState(node *maelstrom.Node) State {
	return State{node: node, ctx: context.Background(), storage: make(map[int]int)}
}

func main() {
	n := maelstrom.NewNode()
	state := NewState(n)

	n.Handle("txn", func(msg maelstrom.Message) error {
		var body TxnMessage
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		results := []Txn{}

		for _, tx := range body.Txn {
			result := state.ProcessTransaction(tx)
			results = append(results, result)
		}

		body.Type = "txn_ok"
		body.Txn = results

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
