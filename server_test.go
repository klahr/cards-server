package main

import (
	"bufio"
	"encoding/json"
	"net"
	"testing"
	"time"
)

type testClient struct {
	t  *testing.T
	nc net.Conn
	r  *bufio.Reader
}

func start(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go newServer(time.Hour, minVersions{all: 1, game: map[string]int{"bridge": 2}}).serve(l)
	return l.Addr().String()
}

func dial(t *testing.T, addr, name, token string) (*testClient, string) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	c := &testClient{t: t, nc: nc, r: bufio.NewReader(nc)}
	c.send(map[string]any{"t": "hello", "game": "hearts", "v": 1, "name": name, "token": token})
	w := c.expect("welcome")
	c.expect("lobby")
	return c, w["token"].(string)
}

func (c *testClient) send(v map[string]any) {
	b, _ := json.Marshal(v)
	if _, err := c.nc.Write(append(b, '\n')); err != nil {
		c.t.Fatal(err)
	}
}

// Reads until a message of kind t, skipping others.
func (c *testClient) expect(t string) map[string]any {
	c.t.Helper()
	c.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		line, err := c.r.ReadBytes('\n')
		if err != nil {
			c.t.Fatalf("waiting for %s: %v", t, err)
		}
		var m map[string]any
		if err := json.Unmarshal(line, &m); err != nil {
			c.t.Fatal(err)
		}
		if m["t"] == t {
			return m
		}
	}
}

func (c *testClient) until(t string, ok func(map[string]any) bool) map[string]any {
	c.t.Helper()
	for {
		if m := c.expect(t); ok(m) {
			return m
		}
	}
}

func seatOf(table map[string]any, i int) map[string]any {
	return table["seats"].([]any)[i].(map[string]any)
}

func TestPlayAndRejoin(t *testing.T) {
	addr := start(t)
	alice, _ := dial(t, addr, "Alice", "")
	bob, bobToken := dial(t, addr, "Bob", "")

	alice.send(map[string]any{"t": "create", "seats": 4, "password": "pw", "title": "Friday"})
	table := alice.expect("table")
	if table["seat"].(float64) != 0 || table["locked"] != true {
		t.Fatalf("table %v", table)
	}
	alice.expect("log")

	lobby := bob.expect("lobby")
	games := lobby["games"].([]any)
	if len(games) != 1 {
		t.Fatalf("lobby %v", lobby)
	}
	id := games[0].(map[string]any)["id"].(string)

	bob.send(map[string]any{"t": "join", "id": id, "password": "nope"})
	bob.expect("error")
	bob.send(map[string]any{"t": "join", "id": id, "password": "pw"})
	if s := bob.expect("table")["seat"].(float64); s != 1 {
		t.Fatalf("bob in seat %v", s)
	}

	bob.send(map[string]any{"t": "start"}) // only the creator may start
	alice.send(map[string]any{"t": "start"})
	table = bob.until("table", func(m map[string]any) bool { return m["started"] == true })
	if seatOf(table, 2)["bot"] != true || seatOf(table, 1)["bot"] != false {
		t.Fatalf("seats %v", table["seats"])
	}

	alice.send(map[string]any{"t": "act", "a": map[string]any{"deal": 1}})
	if a := bob.expect("act"); a["n"].(float64) != 0 {
		t.Fatalf("act %v", a)
	}

	// Bob drops; Alice may hand the seat to a bot until Bob is back.
	bob.nc.Close()
	alice.until("table", func(m map[string]any) bool { return !seatOf(m, 1)["online"].(bool) })
	alice.send(map[string]any{"t": "bot", "seat": 1, "on": true})
	alice.until("table", func(m map[string]any) bool { return seatOf(m, 1)["bot"].(bool) })
	alice.send(map[string]any{"t": "act", "a": map[string]any{"play": 2}})
	alice.expect("act")

	bob, _ = dial(t, addr, "Bob", bobToken)
	bob.send(map[string]any{"t": "join", "id": id})
	table = bob.expect("table")
	if table["seat"].(float64) != 1 || seatOf(table, 1)["bot"] != false {
		t.Fatalf("rejoined %v", table)
	}
	if acts := bob.expect("log")["acts"].([]any); len(acts) != 2 {
		t.Fatalf("log %v", acts)
	}
}

func TestOwnerLeavesBeforeStart(t *testing.T) {
	addr := start(t)
	alice, _ := dial(t, addr, "Alice", "")
	bob, _ := dial(t, addr, "Bob", "")

	alice.send(map[string]any{"t": "create", "seats": 2})
	alice.expect("table")
	id := bob.expect("lobby")["games"].([]any)[0].(map[string]any)["id"]
	bob.send(map[string]any{"t": "join", "id": id})
	bob.expect("table")

	alice.send(map[string]any{"t": "leave"})
	table := bob.expect("table")
	if table["admin"].(float64) != 1 {
		t.Fatalf("admin %v", table["admin"])
	}
	bob.send(map[string]any{"t": "leave"})
	if games := bob.expect("lobby")["games"].([]any); len(games) != 0 {
		t.Fatalf("games %v", games)
	}
}

func TestDropBeforeStartKeepsSeat(t *testing.T) {
	addr := start(t)
	alice, aliceToken := dial(t, addr, "Alice", "")
	bob, _ := dial(t, addr, "Bob", "")

	alice.send(map[string]any{"t": "create", "seats": 2})
	alice.expect("table")
	id := bob.expect("lobby")["games"].([]any)[0].(map[string]any)["id"]

	// Alice's connection drops: the game and her seat wait for her.
	alice.nc.Close()
	lobby := bob.until("lobby", func(m map[string]any) bool { return len(m["games"].([]any)) == 1 })
	bob.send(map[string]any{"t": "join", "id": id})
	if s := bob.expect("table")["seat"].(float64); s != 1 {
		t.Fatalf("bob in seat %v, lobby %v", s, lobby)
	}

	alice, _ = dial(t, addr, "Alice", aliceToken)
	alice.send(map[string]any{"t": "join", "id": id})
	table := alice.expect("table")
	if table["seat"].(float64) != 0 || table["admin"].(float64) != 0 {
		t.Fatalf("rejoined %v", table)
	}
}

func TestInvite(t *testing.T) {
	addr := start(t)
	a, _ := dial(t, addr, "Ann", "")
	a.send(map[string]any{"t": "create", "seats": 4, "password": "secret"})
	table := a.expect("table")

	b, _ := dial(t, addr, "Bob", "")
	// Bob shows up in the list Ann sees at her table.
	list := a.until("players", func(m map[string]any) bool { return len(m["players"].([]any)) == 1 })
	bob := list["players"].([]any)[0].(map[string]any)
	if bob["name"] != "Bob" || bob["id"] == "" {
		t.Fatalf("players: %v", list)
	}

	// Someone hidden is left out.
	h, _ := dial(t, addr, "Hid", "")
	h.send(map[string]any{"t": "visible", "on": false})
	a.until("players", func(m map[string]any) bool { return len(m["players"].([]any)) == 1 })

	a.send(map[string]any{"t": "invite", "id": bob["id"]})
	inv := b.expect("invited")
	if inv["id"] != table["id"] || inv["from"] != "Ann" {
		t.Fatalf("invited: %v", inv)
	}
	// The invitation stands in for the password.
	b.send(map[string]any{"t": "join", "id": inv["id"]})
	if seat := b.expect("table")["seat"]; seat != 1.0 {
		t.Fatalf("seat %v", seat)
	}
	// At the table, Bob is no longer in the list.
	a.until("players", func(m map[string]any) bool { return len(m["players"].([]any)) == 0 })

	// Without an invitation, the password still holds.
	h.send(map[string]any{"t": "join", "id": table["id"]})
	if msg := h.expect("error")["msg"]; msg != "wrong password" {
		t.Fatalf("error %v", msg)
	}
}

func TestVersion(t *testing.T) {
	addr := start(t)
	hello := func(game string, v int) *testClient {
		nc, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { nc.Close() })
		c := &testClient{t: t, nc: nc, r: bufio.NewReader(nc)}
		c.send(map[string]any{"t": "hello", "game": game, "v": v})
		return c
	}
	// No version at all, and too old for a game with its own minimum.
	for _, c := range []*testClient{hello("hearts", 0), hello("bridge", 1)} {
		if msg := c.expect("error")["msg"]; msg != "update" {
			t.Fatalf("error %v", msg)
		}
		if _, err := c.r.ReadBytes('\n'); err == nil {
			t.Fatal("still connected")
		}
	}
	hello("bridge", 2).expect("welcome")
	hello("hearts", 1).expect("welcome")
}

func TestParseMinVersions(t *testing.T) {
	mv, err := parseMinVersions("1, hearts=3")
	if err != nil || mv.of("hearts") != 3 || mv.of("bridge") != 1 {
		t.Fatalf("%v %v", mv, err)
	}
	for _, bad := range []string{"x", "hearts=", "Hearts=2", "-1"} {
		if _, err := parseMinVersions(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}
