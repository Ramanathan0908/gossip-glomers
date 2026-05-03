package main

import (
	"encoding/json"
	"sync"
	"time"

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

	go s.handleMsg(body.Message, msg.Src)

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

func (s *Server) storeMsg(msg float64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.values[msg]; ok {
		return false
	}

	s.values[msg] = struct{}{}
	return true
}

func (s *Server) gossip(msg float64, dest string) {
	gossipMsg := map[string]any{
		"type":    "broadcast",
		"message": msg,
	}
	for {
		done := make(chan struct{})

		err := s.n.RPC(dest, gossipMsg, func(msg maelstrom.Message) error {
			close(done)
			return nil
		})

		if err != nil {
			time.Sleep(250 * time.Millisecond)
		}

		select {
		case <-done:
			return
		case <-time.After(300 * time.Millisecond):
			continue
		}
	}
}

func (s *Server) handleMsg(msg float64, src string) {
	if !s.storeMsg(msg) {
		return
	}

	for _, ngbr := range s.ngbrs {
		if ngbr == src {
			continue
		}
		go s.gossip(msg, ngbr)
	}
}
