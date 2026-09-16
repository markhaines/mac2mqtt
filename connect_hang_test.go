package main

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

// ============================================================================
// Startup against a broker that is reachable but never completes the connect
//
// The TCP probe succeeds, then the MQTT connect never finishes: a stalled
// CONNACK, credentials rejected on every retry, a TLS mismatch. With
// ConnectRetry set, Paho's connect token only completes on success, so startup
// used to wait on it forever: no main loop, no timers, no network check, and
// no death signal beyond a will the broker never accepted.
// ============================================================================

// pendingToken is a connect or publish token that completes when done closes.
type pendingToken struct{ done chan struct{} }

func (t pendingToken) Wait() bool { <-t.done; return true }
func (t pendingToken) WaitTimeout(d time.Duration) bool {
	select {
	case <-t.done:
		return true
	case <-time.After(d):
		return false
	}
}
func (t pendingToken) Done() <-chan struct{} { return t.done }
func (t pendingToken) Error() error          { return nil }

// gatedClient behaves like a Paho client with ConnectRetry set whose broker
// has not accepted yet. Connect returns a token that completes only once the
// gate opens. Until then IsConnected reports true, as Paho's does while
// connecting, IsConnectionOpen reports false, and a publish or subscribe
// returns a token that never completes, as a Paho QoS 0 publish made while
// connecting does. When the gate opens it connects and runs OnConnect once.
type gatedClient struct {
	*fakeClient
	opts  *mqtt.ClientOptions
	gate  chan struct{}
	stop  chan struct{}
	token pendingToken

	connected       atomic.Bool
	connects        atomic.Int32
	onConnects      atomic.Int32
	whileConnecting atomic.Int32 // publishes and subscribes made before connecting
}

func (c *gatedClient) IsConnected() bool      { return true }
func (c *gatedClient) IsConnectionOpen() bool { return c.connected.Load() }

func (c *gatedClient) Connect() mqtt.Token {
	if c.connects.Add(1) == 1 {
		go func() {
			select {
			case <-c.gate:
			case <-c.stop:
				return
			}
			c.connected.Store(true)
			close(c.token.done)
			c.opts.OnConnect(c)
			c.onConnects.Add(1)
		}()
	}
	return c.token
}

var neverDone = pendingToken{done: make(chan struct{})}

func (c *gatedClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	if !c.connected.Load() {
		c.whileConnecting.Add(1)
		return neverDone
	}
	return c.fakeClient.Publish(topic, qos, retained, payload)
}

func (c *gatedClient) Subscribe(topic string, qos byte, h mqtt.MessageHandler) mqtt.Token {
	if !c.connected.Load() {
		c.whileConnecting.Add(1)
		return neverDone
	}
	return c.fakeClient.Subscribe(topic, qos, h)
}

type gatedFactory struct {
	stop    chan struct{}
	open    bool // build clients whose gate is already open: a healthy broker
	mu      sync.Mutex
	clients []*gatedClient
}

func (f *gatedFactory) build(opts *mqtt.ClientOptions) mqtt.Client {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := &gatedClient{fakeClient: &fakeClient{}, opts: opts, gate: make(chan struct{}), stop: f.stop,
		token: pendingToken{done: make(chan struct{})}}
	if f.open {
		close(c.gate)
	}
	f.clients = append(f.clients, c)
	return c
}

func (f *gatedFactory) built() []*gatedClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*gatedClient(nil), f.clients...)
}

// stallingApp is an app whose broker always probes reachable, wired to f.
func stallingApp(t *testing.T, open bool) (*Application, *gatedFactory, *atomic.Int32) {
	t.Helper()
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	f := &gatedFactory{stop: stop, open: open}
	probes := &atomic.Int32{}

	app := pinHost(connectTestApp(baseConfig()))
	app.reachableFn = func() bool { probes.Add(1); return true }
	app.newClientFn = f.build
	app.startupConnectWait = 50 * time.Millisecond
	return app, f, probes
}

// runInBackground starts Run and returns a function that stops it and waits
// for it to return. Everything Run owns (app.client, initialSetupPending) is
// only read after that, which is what keeps these tests race-free.
func runInBackground(t *testing.T, app *Application) (stopRun func()) {
	t.Helper()
	stop := make(chan struct{})
	app.stop = stop
	done := make(chan error, 1)
	go func() { done <- app.Run() }()
	return func() {
		t.Helper()
		close(stop)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run returned %v", err)
			}
		case <-time.After(connectDeadline):
			t.Fatal("Run did not return after stop: the main loop is blocked")
		}
	}
}

// assertConnectOrder checks one connect put availability and the command
// subscription on the wire, both ahead of discovery, exactly once each.
func assertConnectOrder(t *testing.T, ops []op) {
	t.Helper()
	aliveIdx := indexOf(ops, "publish", aliveTopic)
	subIdx := indexOf(ops, "subscribe", commandWild)
	discIdx := indexOf(ops, "publish", discoveryDest)
	if aliveIdx < 0 || subIdx < 0 || discIdx < 0 {
		t.Fatalf("connect incomplete: alive %d, subscribe %d, discovery %d", aliveIdx, subIdx, discIdx)
	}
	if aliveIdx > discIdx || subIdx > discIdx {
		t.Errorf("availability gated on discovery: alive %d, subscribe %d, discovery %d", aliveIdx, subIdx, discIdx)
	}
	if o := ops[aliveIdx]; !o.retained || o.qos != 0 || string(o.payload) != "online" {
		t.Errorf("availability = retained %v qos %d payload %q, want retained qos 0 \"online\"", o.retained, o.qos, o.payload)
	}
	for _, o := range []op{{kind: "publish", topic: aliveTopic}, {kind: "subscribe", topic: commandWild}, {kind: "publish", topic: discoveryDest}} {
		if n := countOps(ops, o.kind, o.topic); n != 1 {
			t.Errorf("%s %s happened %d times, want 1", o.kind, o.topic, n)
		}
	}
	assertPinnedDevice(t, ops)
}

// The bug: a connect that never completes must not stop Run reaching its main
// loop. The loop's network check runs repeatedly while the client is still
// connecting, builds no second client and publishes nothing through it; when
// the broker finally accepts, connectHandler runs once, availability first.
func TestStartupDoesNotBlockOnConnectThatNeverCompletes(t *testing.T) {
	app, f, probes := stallingApp(t, false)
	app.networkCheckInterval = 2 * time.Millisecond

	stopRun := runInBackground(t, app)
	// One probe is startup's own; the rest can only come from the loop.
	waitUntil(t, connectDeadline, "main loop network checks while the connect is pending", func() bool {
		return probes.Load() >= 6
	})
	stopRun()

	clients := f.built()
	if len(clients) != 1 {
		t.Fatalf("built %d clients, want exactly 1", len(clients))
	}
	c := clients[0]
	if app.client != mqtt.Client(c) {
		t.Fatal("the connecting client was not installed as app.client")
	}
	if !app.initialSetupPending {
		t.Error("initial setup not left pending for the loop to run once connected")
	}
	if n := c.connects.Load(); n != 1 {
		t.Errorf("Connect called %d times, want 1", n)
	}
	if n := c.whileConnecting.Load(); n != 0 {
		t.Errorf("%d publishes/subscribes made while still connecting, want 0: their tokens never complete", n)
	}
	if app.isClientConnected() {
		t.Error("isClientConnected reports a client that is still connecting as connected")
	}

	close(c.gate)
	waitUntil(t, connectDeadline, "connectHandler once the broker accepts", func() bool { return c.onConnects.Load() == 1 })
	if !c.opts.WillEnabled || c.opts.WillTopic != aliveTopic || string(c.opts.WillPayload) != "offline" || !c.opts.WillRetained {
		t.Errorf("will = enabled %v topic %q payload %q retained %v", c.opts.WillEnabled, c.opts.WillTopic, c.opts.WillPayload, c.opts.WillRetained)
	}
	assertConnectOrder(t, c.snapshot())
	if !app.isClientConnected() {
		t.Error("app does not report connected after the broker accepted")
	}
	if n := len(f.built()); n != 1 {
		t.Errorf("built %d clients in total, want 1", n)
	}
}

// The healthy path is unchanged: a broker that accepts at once is connected
// before startup returns, so Run makes its initial publishes immediately rather
// than leaving them to the network check.
func TestHealthyStartupIsConnectedBeforeTheLoop(t *testing.T) {
	app, f, _ := stallingApp(t, true)
	app.startupConnectWait = connectDeadline // only a hang detector here

	if err := app.connectAtStartup(); err != nil {
		t.Fatalf("connectAtStartup returned %v", err)
	}
	clients := f.built()
	if len(clients) != 1 || app.client != mqtt.Client(clients[0]) {
		t.Fatalf("built %d clients, want 1 installed as app.client", len(clients))
	}
	if !app.isClientConnected() {
		t.Error("healthy startup returned before the connect completed")
	}
	if app.initialSetupPending {
		t.Error("healthy startup deferred its initial setup to the network check")
	}
	c := clients[0]
	waitUntil(t, connectDeadline, "connectHandler", func() bool { return c.onConnects.Load() == 1 })
	assertConnectOrder(t, c.snapshot())
}

// A client that exists but has not connected yet belongs to Paho's retry loop.
// However many network checks see the broker reachable, none may build or
// connect another client.
func TestNetworkCheckWhileConnectingBuildsNoSecondClient(t *testing.T) {
	app, f, _ := stallingApp(t, false)
	done := make(chan error, 1)
	go func() {
		if err := app.connectAtStartup(); err != nil {
			done <- err
			return
		}
		var st connectivityState
		for i := 0; i < 5; i++ {
			app.runPendingInitialSetup()
			app.networkCheck(&st)
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("connectAtStartup returned %v", err)
		}
	case <-time.After(connectDeadline):
		t.Fatal("startup or the network check blocked on a client that is still connecting")
	}
	clients := f.built()
	if len(clients) != 1 {
		t.Fatalf("built %d clients while the first was connecting, want 1", len(clients))
	}
	if app.client != mqtt.Client(clients[0]) {
		t.Error("the connecting client was replaced")
	}
	if n := clients[0].connects.Load(); n != 1 {
		t.Errorf("Connect called %d times, want 1", n)
	}
	if n := clients[0].whileConnecting.Load(); n != 0 {
		t.Errorf("%d publishes/subscribes made while connecting, want 0", n)
	}
}

// ============================================================================
// Against the real Paho client
// ============================================================================

// stallingBroker accepts TCP and reads the CONNECT, but never answers it while
// stalling. After unstall it drops the stalled connections and serves every
// new one as a working broker (loopbackBroker from recovery_test.go).
type stallingBroker struct {
	*loopbackBroker
	ln       net.Listener
	mu       sync.Mutex
	stalling bool
	held     []net.Conn
	stalled  []*packets.ConnectPacket
}

func newStallingBroker(t *testing.T) *stallingBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &stallingBroker{loopbackBroker: &loopbackBroker{t: t, addr: ln.Addr().String()}, ln: ln, stalling: true}
	t.Cleanup(func() {
		ln.Close()
		b.mu.Lock()
		for _, c := range b.held {
			c.Close()
		}
		b.mu.Unlock()
		b.loopbackBroker.stop()
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			b.mu.Lock()
			stalling := b.stalling
			b.mu.Unlock()
			if stalling {
				go b.hold(conn)
				continue
			}
			lb := b.loopbackBroker
			lb.mu.Lock()
			lb.open = append(lb.open, conn)
			lb.mu.Unlock()
			go lb.serve(conn)
		}
	}()
	return b
}

func (b *stallingBroker) hold(conn net.Conn) {
	pkt, err := packets.ReadPacket(conn)
	cp, ok := pkt.(*packets.ConnectPacket)
	b.mu.Lock()
	defer b.mu.Unlock()
	if err != nil || !ok || !b.stalling {
		conn.Close() // a probe dial, or unstall won the race: let the client retry
		return
	}
	b.stalled = append(b.stalled, cp)
	b.held = append(b.held, conn)
}

func (b *stallingBroker) stalledConnects() []*packets.ConnectPacket {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]*packets.ConnectPacket(nil), b.stalled...)
}

func (b *stallingBroker) unstall() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stalling = false
	for _, c := range b.held {
		c.Close()
	}
	b.held = nil
}

// realPahoApp wires an app to a real Paho client for addr. The only option
// changed from production is the retry interval, 15s there, so a retry happens
// inside the test; the factory records every client it builds.
func realPahoApp(t *testing.T, addr string, ssl bool) (app *Application, clients func() []mqtt.Client, schemes func() []string) {
	t.Helper()
	host, port, _ := net.SplitHostPort(addr)
	cfg := baseConfig()
	cfg.IP, cfg.Port, cfg.SSL = host, port, ssl
	app = pinHost(connectTestApp(cfg))

	var mu sync.Mutex
	var built []mqtt.Client
	var seen []string
	app.newClientFn = func(opts *mqtt.ClientOptions) mqtt.Client {
		opts.SetConnectRetryInterval(20 * time.Millisecond)
		c := mqtt.NewClient(opts)
		mu.Lock()
		defer mu.Unlock()
		built = append(built, c)
		for _, s := range opts.Servers {
			seen = append(seen, s.Scheme)
		}
		return c
	}
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, c := range built {
			c.Disconnect(0)
		}
	})
	clients = func() []mqtt.Client {
		mu.Lock()
		defer mu.Unlock()
		return append([]mqtt.Client(nil), built...)
	}
	schemes = func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
	return app, clients, schemes
}

func TestRealPahoStartupSurvivesBrokerThatNeverAcks(t *testing.T) {
	broker := newStallingBroker(t)
	app, clients, _ := realPahoApp(t, broker.addr, false)
	app.startupConnectWait = 100 * time.Millisecond
	app.networkCheckInterval = 5 * time.Millisecond
	var probes atomic.Int32
	app.reachableFn = func() bool {
		probes.Add(1)
		conn, err := net.DialTimeout("tcp", broker.addr, time.Second)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}

	stopRun := runInBackground(t, app)
	waitUntil(t, connectDeadline, "the stalled CONNECT to reach the broker", func() bool {
		return len(broker.stalledConnects()) >= 1
	})
	waitUntil(t, connectDeadline, "main loop network checks while Paho is stuck connecting", func() bool {
		return probes.Load() >= 6
	})
	if cs := clients(); len(cs) == 1 {
		t.Logf("while stalled: Paho IsConnected=%v IsConnectionOpen=%v", cs[0].IsConnected(), cs[0].IsConnectionOpen())
	}
	// Stop the loop before the broker accepts: once connected, the pending
	// initial setup would run real host commands (df, top, curl).
	stopRun()

	cs := clients()
	if len(cs) != 1 {
		t.Fatalf("built %d clients, want exactly 1", len(cs))
	}
	if app.client != cs[0] {
		t.Fatal("the connecting client was not installed as app.client")
	}
	if !app.initialSetupPending {
		t.Error("initial setup not left pending")
	}
	if app.isClientConnected() {
		t.Error("isClientConnected true while the broker has not acknowledged the CONNECT")
	}

	broker.unstall()
	waitUntil(t, connectDeadline, "Paho to connect and run connectHandler once the broker behaves", func() bool {
		return broker.sessionComplete(0)
	})

	for i, cp := range append(broker.stalledConnects(), broker.connections()[0].connect) {
		if !cp.WillFlag || cp.WillTopic != aliveTopic || string(cp.WillMessage) != "offline" || !cp.WillRetain || cp.WillQos != 0 {
			t.Errorf("CONNECT %d will = flag %v topic %q payload %q retain %v qos %d, want %q \"offline\" retained qos 0",
				i, cp.WillFlag, cp.WillTopic, cp.WillMessage, cp.WillRetain, cp.WillQos, aliveTopic)
		}
	}
	if n := len(broker.connections()); n != 1 {
		t.Errorf("broker saw %d accepted MQTT sessions, want 1", n)
	}
	assertConnectOrder(t, broker.connections()[0].ops)
	if !app.isClientConnected() {
		t.Error("app does not report connected once the broker accepted")
	}
	if n := len(clients()); n != 1 {
		t.Errorf("built %d clients in total, want 1", n)
	}
}

// firstByteListener records the first byte of every connection that sends
// anything, then drops it. A TLS client opens with a handshake record (0x16);
// a plaintext MQTT client opens with CONNECT (0x10).
type firstByteListener struct {
	addr  string
	mu    sync.Mutex
	first []byte
}

func newFirstByteListener(t *testing.T) *firstByteListener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	l := &firstByteListener{addr: ln.Addr().String()}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				conn.SetReadDeadline(time.Now().Add(connectDeadline))
				b := make([]byte, 1)
				if n, _ := conn.Read(b); n == 1 {
					l.mu.Lock()
					l.first = append(l.first, b[0])
					l.mu.Unlock()
				}
			}()
		}
	}()
	return l
}

func (l *firstByteListener) seen() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.first...)
}

// mqtt_ssl means TLS only. Against a broker that cannot complete TLS, startup
// must still return promptly, and neither startup nor Paho's retries may ever
// fall back to sending the credentials-bearing CONNECT in plaintext.
func TestSSLStartupIsBoundedAndNeverFallsBackToPlaintext(t *testing.T) {
	l := newFirstByteListener(t)
	app, clients, schemes := realPahoApp(t, l.addr, true)
	app.config.User, app.config.Password = "someuser", "somepassword"
	app.startupConnectWait = 100 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- app.connectAtStartup() }()
	var atReturn int
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("connectAtStartup returned %v", err)
		}
		atReturn = len(l.seen())
	case <-time.After(connectDeadline):
		t.Fatal("startup blocked on a TLS connect that cannot complete")
	}

	// Let every client retry a few times after startup has returned: any
	// fallback would have to connect then.
	waitUntil(t, connectDeadline, "several connect attempts after startup", func() bool { return len(l.seen()) >= atReturn+6 })

	for i, b := range l.seen() {
		if b != 0x16 {
			t.Errorf("connection %d opened with byte %#x, want 0x16 (TLS handshake): a non-TLS connect was made", i, b)
		}
	}
	if !app.config.SSL {
		t.Error("mqtt_ssl was switched off: TLS fell back to plaintext")
	}
	for _, s := range schemes() {
		if s != "ssl" {
			t.Errorf("client built for scheme %q, want only ssl", s)
		}
	}
	if n := len(clients()); n != 1 || app.client != clients()[0] {
		t.Errorf("built %d clients, want 1 installed as app.client", n)
	}
	if app.isClientConnected() {
		t.Error("reports connected to a broker that never completed TLS")
	}
	if !app.initialSetupPending {
		t.Error("initial setup not left pending")
	}
}
