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
// Offline-start recovery
//
// If the broker is unreachable at startup the process enters offline mode with
// no MQTT client. Before this fix nothing ever created one, so the host stayed
// silently dead to Home Assistant until restarted, even though the network
// check logged that the broker was reachable again.
// ============================================================================

// waitUntil polls cond for at most d. Every recovery assertion is bounded by
// it, so a regression fails the test rather than hanging it.
func waitUntil(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for: %s", d, what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// --- fake broker and client factory ------------------------------------------

// switchableBroker is the reachability the fake client and the probe both see.
type switchableBroker struct{ up atomic.Bool }

// recoveringClient is a fakeClient whose connection state follows the broker.
// Connect behaves like Paho's with ConnectRetry set: it returns at once and a
// background goroutine keeps trying until the broker is up, then runs
// OnConnect off the caller's goroutine, as Paho does.
type recoveringClient struct {
	*fakeClient
	opts       *mqtt.ClientOptions
	broker     *switchableBroker
	stop       chan struct{}
	connected  atomic.Bool
	connects   atomic.Int32 // Connect() calls
	onConnects atomic.Int32 // OnConnect invocations that have completed
}

func (c *recoveringClient) IsConnected() bool      { return c.connected.Load() }
func (c *recoveringClient) IsConnectionOpen() bool { return c.connected.Load() }

func (c *recoveringClient) Connect() mqtt.Token {
	c.connects.Add(1)
	go func() {
		for !c.broker.up.Load() {
			select {
			case <-c.stop:
				return
			case <-time.After(time.Millisecond):
			}
		}
		c.connected.Store(true)
		c.opts.OnConnect(c)
		c.onConnects.Add(1)
	}()
	return fakeToken{}
}

// clientFactory is the newClientFn seam. It records every client it builds and
// what the options looked like at construction, before Connect.
type clientFactory struct {
	broker  *switchableBroker
	stop    chan struct{}
	mu      sync.Mutex
	clients []*recoveringClient
}

func (f *clientFactory) build(opts *mqtt.ClientOptions) mqtt.Client {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := &recoveringClient{fakeClient: &fakeClient{}, opts: opts, broker: f.broker, stop: f.stop}
	f.clients = append(f.clients, c)
	return c
}

func (f *clientFactory) built() []*recoveringClient {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*recoveringClient(nil), f.clients...)
}

// offlineApp builds an app whose broker is unreachable, wired to the factory.
func offlineApp(t *testing.T, cfg *config) (*Application, *switchableBroker, *clientFactory) {
	t.Helper()
	broker := &switchableBroker{}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	f := &clientFactory{broker: broker, stop: stop}

	app := pinMediaControl(connectTestApp(cfg), false)
	app.reachableFn = broker.up.Load
	app.newClientFn = f.build
	return app, broker, f
}

func countOps(ops []op, kind, topic string) int {
	n := 0
	for _, o := range ops {
		if o.kind == kind && o.topic == topic {
			n++
		}
	}
	return n
}

// assertHealthyConnect checks what a single successful connect must have put on
// the wire: the will in the options, availability and the command subscription
// both ahead of discovery.
func assertHealthyConnect(t *testing.T, c *recoveringClient) {
	t.Helper()
	if !c.opts.WillEnabled || c.opts.WillTopic != aliveTopic ||
		string(c.opts.WillPayload) != "offline" || !c.opts.WillRetained || c.opts.WillQos != 0 {
		t.Errorf("will = enabled %v topic %q payload %q retained %v qos %d, want enabled %q \"offline\" retained qos 0",
			c.opts.WillEnabled, c.opts.WillTopic, c.opts.WillPayload, c.opts.WillRetained, c.opts.WillQos, aliveTopic)
	}
	if c.opts.OnConnect == nil {
		t.Fatal("client built without OnConnect: connectHandler would never run")
	}

	ops := c.snapshot()
	aliveIdx := indexOf(ops, "publish", aliveTopic)
	subIdx := indexOf(ops, "subscribe", commandWild)
	discIdx := indexOf(ops, "publish", discoveryDest)
	if aliveIdx < 0 || subIdx < 0 || discIdx < 0 {
		t.Fatalf("recovered connect incomplete: alive %d, subscribe %d, discovery %d", aliveIdx, subIdx, discIdx)
	}
	if aliveIdx > discIdx || subIdx > discIdx {
		t.Errorf("recovered connect gated availability on discovery: alive %d, subscribe %d, discovery %d",
			aliveIdx, subIdx, discIdx)
	}
	if o := ops[aliveIdx]; !o.retained || o.qos != 0 || string(o.payload) != "online" {
		t.Errorf("availability = retained %v qos %d payload %q, want retained qos 0 \"online\"", o.retained, o.qos, o.payload)
	}
}

// The production bug: start offline, broker comes back, a later network check
// must build the client and run the normal connect path. Run in every sensor
// configuration, because the recovered path must honour them like startup does.
func TestOfflineStartRecoversWhenBrokerBecomesReachable(t *testing.T) {
	for _, tc := range allKeyCombinations() {
		t.Run(tc.name, func(t *testing.T) {
			app, broker, f := offlineApp(t, tc.cfg)
			var st connectivityState // offline start: nothing reachable, nothing connected

			for i := 0; i < 3; i++ {
				app.networkCheck(&st)
			}
			if n := len(f.built()); n != 0 || app.client != nil {
				t.Fatalf("built %d clients while the broker was unreachable, want 0", n)
			}

			broker.up.Store(true)
			app.networkCheck(&st)

			clients := f.built()
			if len(clients) != 1 {
				t.Fatalf("network check with broker reachable built %d clients, want exactly 1: "+
					"offline mode never recovers", len(clients))
			}
			c := clients[0]
			if app.client != mqtt.Client(c) {
				t.Fatal("recovered client was not installed as app.client")
			}
			waitUntil(t, 2*time.Second, "recovered client to run connectHandler", func() bool {
				return c.onConnects.Load() == 1
			})
			assertHealthyConnect(t, c)
			if !app.isClientConnected() {
				t.Error("app does not report connected after recovery")
			}
		})
	}
}

// Recovery must not multiply anything, however many checks run and whatever
// the broker does in between: one client, one Connect, one connectHandler run,
// one subscription and one connect-time availability publish.
func TestOfflineRecoveryNeverDuplicatesClient(t *testing.T) {
	app, broker, f := offlineApp(t, baseConfig())
	var st connectivityState

	app.networkCheck(&st)
	broker.up.Store(true)
	for i := 0; i < 5; i++ {
		app.networkCheck(&st)
	}
	clients := f.built()
	if len(clients) != 1 {
		t.Fatalf("built %d clients, want 1", len(clients))
	}
	c := clients[0]
	waitUntil(t, 2*time.Second, "connectHandler", func() bool { return c.onConnects.Load() == 1 })

	// Flap reachability while the client exists: Paho owns reconnection from
	// here, so no check may build or reconnect anything.
	for i := 0; i < 5; i++ {
		broker.up.Store(i%2 == 0)
		app.networkCheck(&st)
	}
	broker.up.Store(true)
	app.networkCheck(&st)
	time.Sleep(50 * time.Millisecond) // let any stray goroutine surface

	if n := len(f.built()); n != 1 {
		t.Errorf("built %d clients across repeated checks, want 1", n)
	}
	if n := c.connects.Load(); n != 1 {
		t.Errorf("Connect called %d times, want 1: a second Connect races Paho's own reconnect", n)
	}
	if n := c.onConnects.Load(); n != 1 {
		t.Errorf("connectHandler ran %d times, want 1", n)
	}
	ops := c.snapshot()
	if n := countOps(ops, "subscribe", commandWild); n != 1 {
		t.Errorf("subscribed to command/# %d times, want 1", n)
	}
	if n := countOps(ops, "publish", aliveTopic); n != 1 {
		t.Errorf("published connect-time availability %d times, want 1", n)
	}
	if n := countOps(ops, "publish", discoveryDest); n != 1 {
		t.Errorf("published discovery %d times, want 1", n)
	}
}

// A client that already exists, as after a healthy startup, must never be
// replaced by the network check, reachable or not.
func TestNetworkCheckLeavesExistingClientAlone(t *testing.T) {
	app, broker, f := offlineApp(t, baseConfig())
	existing := &fakeClient{}
	app.client = existing
	st := connectivityState{networkReachable: true, lastConnected: true}

	for i := 0; i < 4; i++ {
		broker.up.Store(i%2 == 1)
		app.networkCheck(&st)
	}
	if n := len(f.built()); n != 0 {
		t.Errorf("built %d clients with one already present, want 0", n)
	}
	if app.client != mqtt.Client(existing) {
		t.Error("existing client was replaced")
	}
	if app.initialSetupPending {
		t.Error("initial setup marked pending for a client that did not come from recovery")
	}
}

// A broker that comes back inside the startup grace period is connected at
// startup, without going through offline mode at all.
func TestStartupRetriesBrieflyUnreachableBroker(t *testing.T) {
	app, broker, f := offlineApp(t, baseConfig())
	app.startupRetryBudget = 2 * time.Second
	app.startupRetryBackoff = 5 * time.Millisecond

	var probes atomic.Int32
	app.reachableFn = func() bool {
		// Unreachable for the first attempt, the fatal-or-offline check and
		// two retries, then back: the Local Network block clearing.
		if probes.Add(1) > 4 {
			broker.up.Store(true)
		}
		return broker.up.Load()
	}

	if err := app.connectAtStartup(); err != nil {
		t.Fatalf("connectAtStartup returned %v, want nil", err)
	}
	clients := f.built()
	if len(clients) != 1 {
		t.Fatalf("startup built %d clients, want 1: a briefly unreachable broker sent startup offline", len(clients))
	}
	c := clients[0]
	waitUntil(t, 2*time.Second, "connectHandler after startup retry", func() bool { return c.onConnects.Load() == 1 })
	assertHealthyConnect(t, c)
	if !app.initialSetupPending {
		t.Error("initial setup not left pending for the loop")
	}
}

// The startup grace period is bounded: a broker that never comes back must
// leave startup in offline mode within the budget, having actually retried.
func TestStartupRetryIsBounded(t *testing.T) {
	app, _, f := offlineApp(t, baseConfig())
	const budget = 300 * time.Millisecond
	app.startupRetryBudget = budget
	app.startupRetryBackoff = 10 * time.Millisecond

	var probes atomic.Int32
	app.reachableFn = func() bool { probes.Add(1); return false }

	start := time.Now()
	if err := app.connectAtStartup(); err != nil {
		t.Fatalf("connectAtStartup returned %v, want nil (offline mode)", err)
	}
	if d := time.Since(start); d > budget+200*time.Millisecond {
		t.Errorf("startup took %v with a %v budget: the retry is not bounded", d, budget)
	}
	// Two probes are the initial attempt and the fatal-or-offline check.
	if n := probes.Load(); n <= 2 {
		t.Errorf("probed %d times, want more than 2: startup did not retry before going offline", n)
	}
	if n := len(f.built()); n != 0 || app.client != nil {
		t.Errorf("built %d clients for a broker that never came up, want 0", n)
	}
}

// ============================================================================
// Against the real Paho client
//
// The fakes above prove what mac2mqtt does. This proves the part it delegates:
// that once recovery has built a real Paho client, Paho alone brings it back
// through repeated broker outages, re-running connectHandler each time, with
// no second client. It uses a minimal MQTT broker on loopback.
// ============================================================================

type brokerConn struct {
	connect *packets.ConnectPacket
	ops     []op
}

type loopbackBroker struct {
	t    *testing.T
	addr string

	mu    sync.Mutex
	ln    net.Listener
	open  []net.Conn
	conns []*brokerConn
}

func newLoopbackBroker(t *testing.T) *loopbackBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // reserved; nothing listens until start
	b := &loopbackBroker{t: t, addr: addr}
	t.Cleanup(b.stop)
	return b
}

func (b *loopbackBroker) start() {
	b.t.Helper()
	var ln net.Listener
	var err error
	for i := 0; i < 50; i++ { // the port can sit in TIME_WAIT briefly
		if ln, err = net.Listen("tcp", b.addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		b.t.Fatalf("relisten on %s: %v", b.addr, err)
	}
	b.mu.Lock()
	b.ln = ln
	b.mu.Unlock()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			b.mu.Lock()
			b.open = append(b.open, conn)
			b.mu.Unlock()
			go b.serve(conn)
		}
	}()
}

// stop takes the broker down hard: no listener, every connection dropped.
func (b *loopbackBroker) stop() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ln != nil {
		b.ln.Close()
		b.ln = nil
	}
	for _, c := range b.open {
		c.Close()
	}
	b.open = nil
}

func (b *loopbackBroker) serve(conn net.Conn) {
	var bc *brokerConn
	for {
		pkt, err := packets.ReadPacket(conn)
		if err != nil {
			return // probe dials, outages and client disconnects all end here
		}
		b.mu.Lock()
		switch p := pkt.(type) {
		case *packets.ConnectPacket:
			bc = &brokerConn{connect: p}
			b.conns = append(b.conns, bc)
			b.mu.Unlock()
			ack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
			ack.Write(conn)
			continue
		case *packets.PublishPacket:
			if bc != nil {
				bc.ops = append(bc.ops, op{kind: "publish", topic: p.TopicName, qos: p.Qos, retained: p.Retain, payload: p.Payload})
			}
			b.mu.Unlock()
			if p.Qos == 1 {
				ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
				ack.MessageID = p.MessageID
				ack.Write(conn)
			}
			continue
		case *packets.SubscribePacket:
			if bc != nil {
				for i, topic := range p.Topics {
					bc.ops = append(bc.ops, op{kind: "subscribe", topic: topic, qos: p.Qoss[i]})
				}
			}
			b.mu.Unlock()
			ack := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
			ack.MessageID = p.MessageID
			ack.ReturnCodes = p.Qoss
			ack.Write(conn)
			continue
		case *packets.PingreqPacket:
			b.mu.Unlock()
			packets.NewControlPacket(packets.Pingresp).Write(conn)
			continue
		}
		b.mu.Unlock()
	}
}

// connections returns a copy of what each MQTT session did, in order.
func (b *loopbackBroker) connections() []brokerConn {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]brokerConn, len(b.conns))
	for i, c := range b.conns {
		out[i] = brokerConn{connect: c.connect, ops: append([]op(nil), c.ops...)}
	}
	return out
}

// sessionComplete reports whether connection i has done the full connect path.
func (b *loopbackBroker) sessionComplete(i int) bool {
	conns := b.connections()
	return len(conns) > i &&
		indexOf(conns[i].ops, "publish", aliveTopic) >= 0 &&
		indexOf(conns[i].ops, "subscribe", commandWild) >= 0 &&
		indexOf(conns[i].ops, "publish", discoveryDest) >= 0
}

func TestOfflineRecoveryWithRealPahoAcrossRepeatedOutages(t *testing.T) {
	broker := newLoopbackBroker(t)
	host, port, _ := net.SplitHostPort(broker.addr)

	cfg := baseConfig()
	cfg.IP, cfg.Port = host, port
	app := pinMediaControl(connectTestApp(cfg), false)

	var built atomic.Int32
	var client mqtt.Client
	app.newClientFn = func(opts *mqtt.ClientOptions) mqtt.Client {
		built.Add(1)
		client = mqtt.NewClient(opts)
		return client
	}
	t.Cleanup(func() {
		if client != nil {
			client.Disconnect(0)
		}
	})

	// Offline start: the real probe is refused.
	var st connectivityState
	app.networkCheck(&st)
	if built.Load() != 0 {
		t.Fatal("built a client while the broker was down")
	}

	// Up: the next check recovers.
	broker.start()
	app.networkCheck(&st)
	if built.Load() != 1 {
		t.Fatalf("built %d clients after the broker came up, want 1", built.Load())
	}
	waitUntil(t, 10*time.Second, "first session to complete the connect path", func() bool {
		return broker.sessionComplete(0)
	})

	// Down, up, down, up. Only Paho may bring these back.
	const outages = 2
	for i := 1; i <= outages; i++ {
		broker.stop()
		waitUntil(t, 10*time.Second, "Paho to notice the outage", func() bool { return !client.IsConnectionOpen() })
		app.networkCheck(&st)
		broker.start()
		app.networkCheck(&st)
		waitUntil(t, 20*time.Second, "Paho to reconnect and rerun connectHandler", func() bool {
			return broker.sessionComplete(i)
		})
		app.networkCheck(&st)
	}

	if n := built.Load(); n != 1 {
		t.Errorf("built %d clients across %d outages, want 1", n, outages)
	}
	conns := broker.connections()
	if len(conns) != outages+1 {
		t.Errorf("broker saw %d MQTT sessions, want %d", len(conns), outages+1)
	}
	for i, c := range conns {
		cp := c.connect
		if !cp.WillFlag || cp.WillTopic != aliveTopic || string(cp.WillMessage) != "offline" || !cp.WillRetain || cp.WillQos != 0 {
			t.Errorf("session %d CONNECT will = flag %v topic %q payload %q retain %v qos %d, want %q \"offline\" retained qos 0",
				i, cp.WillFlag, cp.WillTopic, cp.WillMessage, cp.WillRetain, cp.WillQos, aliveTopic)
		}
		aliveIdx := indexOf(c.ops, "publish", aliveTopic)
		subIdx := indexOf(c.ops, "subscribe", commandWild)
		discIdx := indexOf(c.ops, "publish", discoveryDest)
		if aliveIdx < 0 || subIdx < 0 || discIdx < 0 || aliveIdx > discIdx || subIdx > discIdx {
			t.Errorf("session %d on-wire order alive %d, subscribe %d, discovery %d: want both before discovery",
				i, aliveIdx, subIdx, discIdx)
		}
		if aliveIdx >= 0 {
			if o := c.ops[aliveIdx]; !o.retained || o.qos != 0 || string(o.payload) != "online" {
				t.Errorf("session %d availability = retained %v qos %d payload %q", i, o.retained, o.qos, o.payload)
			}
		}
		if n := countOps(c.ops, "subscribe", commandWild); n != 1 {
			t.Errorf("session %d subscribed to command/# %d times, want 1", i, n)
		}
	}
}
