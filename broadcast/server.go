package main

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Server struct {
	n      *maelstrom.Node
	values map[float64]struct{}
	ngbrs  []string
	mu     sync.RWMutex
}

func (s *Server) Init() error {
	s.values = make(map[float64]struct{})
	return nil
}

func (s *Server) Broadcast(msg maelstrom.Message) error {
	var body struct {
		Message float64 `json:"message"`
	}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.mu.Lock()
	if _, ok := s.values[body.Message]; ok {
		s.mu.Unlock()
		return s.n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	} else {
		s.values[body.Message] = struct{}{}
	}
	s.mu.Unlock()

	for _, ngbr := range s.ngbrs {
		if ngbr == msg.Src {
			continue
		}
		err := s.n.Send(ngbr, map[string]any{
			"type":    "broadcast",
			"message": body.Message,
		})

		if err != nil {
			return err
		}
	}

	return s.n.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (s *Server) Read(msg maelstrom.Message) error {
	var body map[string]any

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var values []float64

	s.mu.RLock()
	for k := range s.values {
		values = append(values, k)
	}
	s.mu.RUnlock()

	body["type"] = "read_ok"
	body["messages"] = values

	return s.n.Reply(msg, body)
}

func (s *Server) Topology(msg maelstrom.Message) error {
	var body struct {
		Topology map[string][]string `json:"topology"`
	}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.ngbrs = body.Topology[s.n.ID()]

	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}
