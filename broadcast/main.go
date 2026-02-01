package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	s := &Server{n: maelstrom.NewNode()}

	s.Init()

	s.n.Handle("broadcast", s.Broadcast)
	s.n.Handle("read", s.Read)
	s.n.Handle("topology", s.Topology)
	s.n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
