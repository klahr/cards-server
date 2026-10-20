// The meeting place for the Sailfish card games: a lobby where players find
// and join tables, and a log of moves per table that every player's app
// replays. The rules run in the apps; the server never looks inside a move.
package main

import (
	"flag"
	"log"
	"net"
	"time"
)

func main() {
	addr := flag.String("addr", ":7777", "address to listen on")
	idle := flag.Duration("idle", time.Hour, "how long a started game is kept after everyone has left")
	flag.Parse()

	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s", l.Addr())
	log.Fatal(newServer(*idle).serve(l))
}
