package main

import (
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n      *maelstrom.Node
	currId atomic.Uint64
	once   sync.Once
}

func (s *Server) Init() error {
	if s.n == nil {
		return fmt.Errorf("node is nil")
	}

	if id := slices.Index(s.n.NodeIDs(), s.n.ID()); id != -1 {
		s.currId.Store(uint64(id) * 1_000_000)
	} else {
		return fmt.Errorf("node id not found in nodes")
	}

	return nil
}

func (s *Server) Generate(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var initErr error
	s.once.Do(func() {
		initErr = s.Init()
	})

	if initErr != nil {
		return initErr
	}

	body["type"] = "generate_ok"
	body["id"] = s.currId.Add(1)

	return s.n.Reply(msg, body)
}
