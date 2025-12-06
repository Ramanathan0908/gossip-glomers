package main

import (
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	s := &Server{n: maelstrom.NewNode()}

	s.n.Handle("generate", s.Generate)

	if err := s.n.Run(); err != nil {
		log.Fatal(err)
	}
}
