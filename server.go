package main

import (
	"bufio"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	maxLine     = 64 << 10 // longest message a client may send
	maxActs     = 20000    // longest log a game may grow to
	maxGames    = 500
	maxSeats    = 5
	maxName     = 24
	readTimeout = 45 * time.Second // clients ping every 15 seconds
	// How long a seat is held for a player whose connection dropped before the start.
	grace = 2 * time.Minute
)

var (
	kindRe  = regexp.MustCompile(`^[a-z]{1,16}$`)
	tokenRe = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// A message either way; each kind uses the fields it needs.
type msg struct {
	T        string          `json:"t"`
	Game     string          `json:"game,omitempty"`
	Name     string          `json:"name,omitempty"`
	Token    string          `json:"token,omitempty"`
	ID       string          `json:"id,omitempty"`
	Password string          `json:"password,omitempty"`
	Title    string          `json:"title,omitempty"`
	Seats    int             `json:"seats,omitempty"`
	Seat     int             `json:"seat,omitempty"`
	On       bool            `json:"on,omitempty"`
	A        json.RawMessage `json:"a,omitempty"`
}

type server struct {
	mu      sync.Mutex
	games   map[string]*game
	clients map[*client]struct{}
	// How long a started game is kept once nobody is at the table.
	idle time.Duration
}

type game struct {
	id, kind, title, password string
	// The creator's token; passes to the next player if they leave before the start.
	owner   string
	started bool
	seats   []seat
	log     []json.RawMessage
	// When the last player went away; zero while someone is at the table.
	empty time.Time
}

type seat struct {
	// Empty for a seat no one has taken; a bot plays it once the game starts.
	token string
	name  string
	// A bot plays for the player while they are away.
	bot  bool
	conn *client
	// When the player's connection dropped.
	gone time.Time
}

type client struct {
	srv    *server
	nc     net.Conn
	out    chan []byte
	closed bool
	token  string
	name   string
	kind   string // the card game the client plays; empty until hello
	game   *game
	seat   int
}

func newServer(idle time.Duration) *server {
	return &server{games: map[string]*game{}, clients: map[*client]struct{}{}, idle: idle}
}

func (s *server) serve(l net.Listener) error {
	go s.collect()
	for {
		nc, err := l.Accept()
		if err != nil {
			return err
		}
		c := &client{srv: s, nc: nc, out: make(chan []byte, 256)}
		s.mu.Lock()
		s.clients[c] = struct{}{}
		s.mu.Unlock()
		go c.write()
		go c.read()
	}
}

// Drops started games nobody has come back to, and frees the seats of
// players who dropped before the start and did not come back in time.
func (s *server) collect() {
	for range time.Tick(15 * time.Second) {
		s.mu.Lock()
		changed := map[string]bool{}
		for id, g := range s.games {
			if !g.empty.IsZero() && time.Since(g.empty) > s.idle {
				delete(s.games, id)
				changed[g.kind] = true
				continue
			}
			if g.started {
				continue
			}
			freed := false
			for i, st := range g.seats {
				if st.token != "" && st.conn == nil && time.Since(st.gone) > grace {
					s.free(g, i)
					freed = true
				}
			}
			if freed {
				s.sendTable(g)
				changed[g.kind] = true
			}
		}
		for kind := range changed {
			s.sendLobbies(kind)
		}
		s.mu.Unlock()
	}
}

func (c *client) read() {
	defer c.srv.drop(c)
	sc := bufio.NewScanner(c.nc)
	sc.Buffer(make([]byte, 4096), maxLine)
	for {
		c.nc.SetReadDeadline(time.Now().Add(readTimeout))
		if !sc.Scan() {
			return
		}
		var m msg
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			return
		}
		c.srv.mu.Lock()
		ok := c.srv.handle(c, &m)
		c.srv.mu.Unlock()
		if !ok {
			return
		}
	}
}

func (c *client) write() {
	for b := range c.out {
		c.nc.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if _, err := c.nc.Write(b); err != nil {
			c.nc.Close()
			for range c.out {
			}
			return
		}
	}
	c.nc.Close()
}

// Called with the lock held. A client that does not keep up is cut off.
func (c *client) send(v any) {
	if c.closed {
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		log.Print(err)
		return
	}
	select {
	case c.out <- append(b, '\n'):
	default:
		c.nc.Close()
	}
}

func (c *client) fail(text string) {
	c.send(map[string]any{"t": "error", "msg": text})
}

func (s *server) drop(c *client) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.close(c)
}

func (s *server) close(c *client) {
	if c.closed {
		return
	}
	s.leave(c, false)
	c.closed = true
	close(c.out)
	delete(s.clients, c)
}

// Returns false to hang up on the client.
func (s *server) handle(c *client, m *msg) bool {
	if c.kind == "" && m.T != "hello" {
		return false
	}
	switch m.T {
	case "hello":
		if c.kind != "" || !kindRe.MatchString(m.Game) {
			return false
		}
		c.kind = m.Game
		c.name = cleanName(m.Name)
		c.token = m.Token
		if !tokenRe.MatchString(c.token) {
			c.token = randomHex(16)
		}
		// The same player connecting again: the old connection is dead.
		for other := range s.clients {
			if other != c && other.token == c.token && other.kind == c.kind {
				s.close(other)
			}
		}
		c.send(map[string]any{"t": "welcome", "token": c.token})
		c.send(s.lobby(c))
	case "ping":
		c.send(map[string]any{"t": "pong"})
	case "name":
		c.name = cleanName(m.Name)
		if g := c.game; g != nil {
			g.seats[c.seat].name = c.name
			s.sendTable(g)
			s.sendLobbies(g.kind)
		}
	case "create":
		if m.Seats < 2 || m.Seats > maxSeats {
			c.fail("bad seats")
			break
		}
		if len(s.games) >= maxGames {
			c.fail("server full")
			break
		}
		s.leave(c, true)
		g := &game{
			id:       s.newID(),
			kind:     c.kind,
			title:    cleanName(m.Title),
			password: m.Password,
			owner:    c.token,
			seats:    make([]seat, m.Seats),
		}
		s.games[g.id] = g
		s.sit(c, g, 0)
	case "join":
		s.join(c, m)
	case "leave":
		s.leave(c, true)
		c.send(s.lobby(c))
	case "start":
		g := c.game
		if g == nil || g.started || s.admin(g) != c.seat {
			break
		}
		g.started = true
		s.sendTable(g)
		s.sendLobbies(g.kind)
	case "bot":
		g := c.game
		if g == nil || !g.started || s.admin(g) != c.seat || m.Seat < 0 || m.Seat >= len(g.seats) {
			break
		}
		st := &g.seats[m.Seat]
		if st.token != "" && st.conn == nil {
			st.bot = m.On
			s.sendTable(g)
		}
	case "act":
		g := c.game
		if g == nil || !g.started || len(m.A) == 0 {
			break
		}
		if len(g.log) >= maxActs {
			c.fail("game too long")
			break
		}
		g.log = append(g.log, m.A)
		out := map[string]any{"t": "act", "n": len(g.log) - 1, "a": m.A}
		for _, st := range g.seats {
			if st.conn != nil {
				st.conn.send(out)
			}
		}
	}
	return true
}

func (s *server) join(c *client, m *msg) {
	g := s.games[m.ID]
	if g == nil || g.kind != c.kind {
		c.fail("no such game")
		return
	}
	if g == c.game {
		return
	}
	free := -1
	for i, st := range g.seats {
		if st.token == c.token {
			// Your own seat: no password needed to come back.
			s.leave(c, true)
			s.sit(c, g, i)
			return
		}
		if st.token == "" && free < 0 {
			free = i
		}
	}
	if g.password != "" && subtle.ConstantTimeCompare([]byte(g.password), []byte(m.Password)) != 1 {
		c.fail("wrong password")
		return
	}
	if free < 0 {
		c.fail("game full")
		return
	}
	s.leave(c, true)
	s.sit(c, g, free)
}

func (s *server) sit(c *client, g *game, i int) {
	g.seats[i] = seat{token: c.token, name: c.name, conn: c}
	g.empty = time.Time{}
	c.game, c.seat = g, i
	acts := g.log
	if acts == nil {
		acts = []json.RawMessage{}
	}
	s.sendTable(g)
	c.send(map[string]any{"t": "log", "acts": acts})
	s.sendLobbies(g.kind)
}

// Takes the client from its table. A player who leaves before the start
// frees the seat; one whose connection dropped keeps it for a while. After the
// start, the seat is kept for the player to come back to.
func (s *server) leave(c *client, explicit bool) {
	g := c.game
	if g == nil {
		return
	}
	c.game = nil
	st := &g.seats[c.seat]
	st.conn = nil
	st.gone = time.Now()
	if !g.started && explicit {
		s.free(g, c.seat)
	} else if s.online(g) == 0 && g.started {
		g.empty = time.Now()
	}
	s.sendTable(g)
	s.sendLobbies(g.kind)
}

// Empties a seat before the start, passing the table on if its creator goes
// and dropping it once no one is left.
func (s *server) free(g *game, i int) {
	token := g.seats[i].token
	g.seats[i] = seat{}
	if g.owner == token {
		g.owner = ""
		for _, o := range g.seats {
			if o.token != "" {
				g.owner = o.token
				break
			}
		}
	}
	if g.owner == "" {
		delete(s.games, g.id)
	}
}

func (s *server) online(g *game) int {
	n := 0
	for _, st := range g.seats {
		if st.conn != nil {
			n++
		}
	}
	return n
}

// The seat that runs the bots and deals: the first player at the table.
func (s *server) driver(g *game) int {
	for i, st := range g.seats {
		if st.conn != nil {
			return i
		}
	}
	return -1
}

// The seat that may start the game and give an absent player's seat to a
// bot: the creator, or the driver while the creator is away.
func (s *server) admin(g *game) int {
	for i, st := range g.seats {
		if st.token == g.owner && st.conn != nil {
			return i
		}
	}
	return s.driver(g)
}

func (s *server) sendTable(g *game) {
	if s.games[g.id] != g {
		return
	}
	seats := make([]map[string]any, len(g.seats))
	for i, st := range g.seats {
		seats[i] = map[string]any{
			"name":   st.name,
			"human":  st.token != "",
			"online": st.conn != nil,
			"bot":    st.token == "" || st.bot,
		}
	}
	for i, st := range g.seats {
		if st.conn == nil {
			continue
		}
		st.conn.send(map[string]any{
			"t":       "table",
			"id":      g.id,
			"title":   g.title,
			"locked":  g.password != "",
			"started": g.started,
			"seat":    i,
			"driver":  s.driver(g),
			"admin":   s.admin(g),
			"seats":   seats,
		})
	}
}

func (s *server) lobby(c *client) map[string]any {
	games := []map[string]any{}
	for _, g := range s.games {
		if g.kind != c.kind {
			continue
		}
		mine := false
		names := make([]string, len(g.seats))
		for i, st := range g.seats {
			names[i] = st.name
			mine = mine || st.token == c.token
		}
		// A started game everyone has left is only of interest to its players.
		if g.started && s.online(g) == 0 && !mine {
			continue
		}
		games = append(games, map[string]any{
			"id":      g.id,
			"title":   g.title,
			"players": names,
			"locked":  g.password != "",
			"started": g.started,
			"mine":    mine,
		})
	}
	return map[string]any{"t": "lobby", "games": games}
}

// To everyone of the kind not at a table.
func (s *server) sendLobbies(kind string) {
	for c := range s.clients {
		if c.kind == kind && c.game == nil {
			c.send(s.lobby(c))
		}
	}
}

func (s *server) newID() string {
	for {
		id := randomHex(3)
		if s.games[id] == nil {
			return id
		}
	}
}

func cleanName(name string) string {
	name = strings.Join(strings.Fields(name), " ")
	name = strings.Map(func(r rune) rune {
		if r < 32 || r == 127 {
			return -1
		}
		return r
	}, name)
	for utf8.RuneCountInString(name) > maxName {
		_, size := utf8.DecodeLastRuneInString(name)
		name = name[:len(name)-size]
	}
	return name
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
