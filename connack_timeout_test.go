package main

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
)

// ============================================================================
// A broker that accepts TCP, reads the CONNECT and never answers, and never
// closes the connection either
//
// Before Paho 1.4.2 nothing bounded the wait for CONNACK, so one such
// connection stalled connecting or reconnecting until the socket happened to
// drop: with keepalive not yet running, that could be never. ConnectTimeout
// now also bounds the MQTT handshake, so Paho gives up on the stalled attempt,
// retries, and connects as soon as the broker behaves. Nothing in this test
// closes a stalled connection from the broker side: only the client's
// timeout can end it.
// ============================================================================

// testConnectTimeout replaces the 15s production ConnectTimeout so the stalls
// resolve inside the test. connackSlack is scheduling headroom under -race and
// load; it is far below KeepAlive (60s), so an attempt ended by anything other
// than the connect timeout would still fail the bound.
const (
	testConnectTimeout = 250 * time.Millisecond
	connackSlack       = 3 * time.Second
)

// blackholeBroker stalls every connection accepted while stalling: it reads the
// CONNECT, never replies, and waits for the client to hang up. Connections
// accepted otherwise are served by a working loopbackBroker.
type blackholeBroker struct {
	*loopbackBroker
	mu       sync.Mutex
	stalling bool
	held     []net.Conn
	connects int             // CONNECTs received on stalled connections
	hangups  []time.Duration // CONNECT received to client closing, per stalled connection
}

func newBlackholeBroker(t *testing.T) *blackholeBroker {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	b := &blackholeBroker{loopbackBroker: &loopbackBroker{t: t, addr: ln.Addr().String()}, stalling: true}
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
			if stalling {
				b.held = append(b.held, conn)
			}
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

func (b *blackholeBroker) hold(conn net.Conn) {
	if _, ok := mustReadConnect(conn); !ok {
		return
	}
	got := time.Now()
	b.mu.Lock()
	b.connects++
	b.mu.Unlock()
	io.Copy(io.Discard, conn) // returns only when the client closes
	b.mu.Lock()
	b.hangups = append(b.hangups, time.Since(got))
	b.mu.Unlock()
}

func mustReadConnect(conn net.Conn) (*packets.ConnectPacket, bool) {
	pkt, err := packets.ReadPacket(conn)
	cp, ok := pkt.(*packets.ConnectPacket)
	return cp, err == nil && ok
}

func (b *blackholeBroker) setStalling(stalling bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.stalling = stalling
}

// stalled returns how many CONNECTs went unanswered and how many of those the
// client has since abandoned.
func (b *blackholeBroker) stalled() (connects, hungUp int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.connects, len(b.hangups)
}

func (b *blackholeBroker) hangupTimes() []time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]time.Duration(nil), b.hangups...)
}

// dropSessions cuts every served connection, as a broker restart would.
func (b *blackholeBroker) dropSessions() {
	lb := b.loopbackBroker
	lb.mu.Lock()
	defer lb.mu.Unlock()
	for _, c := range lb.open {
		c.Close()
	}
	lb.open = nil
}

func TestRealPahoConnectTimeoutEndsStalledConnack(t *testing.T) {
	broker := newBlackholeBroker(t)
	host, port, _ := net.SplitHostPort(broker.addr)
	cfg := baseConfig()
	cfg.IP, cfg.Port = host, port
	app := pinHost(connectTestApp(cfg))
	app.reachableFn = func() bool { return true }
	app.startupConnectWait = 50 * time.Millisecond

	var built atomic.Int32
	var client mqtt.Client
	app.newClientFn = func(opts *mqtt.ClientOptions) mqtt.Client {
		opts.SetConnectTimeout(testConnectTimeout)
		opts.SetConnectRetryInterval(20 * time.Millisecond)
		built.Add(1)
		client = mqtt.NewClient(opts)
		return client
	}
	t.Cleanup(func() {
		if client != nil {
			client.Disconnect(0)
		}
	})

	// Phase 1, first connect: several attempts must stall and be abandoned by
	// the client. On a library with no CONNACK timeout the first one never is.
	if err := app.connectAtStartup(); err != nil {
		t.Fatalf("connectAtStartup returned %v", err)
	}
	waitUntil(t, connectDeadline, "the client to abandon 2 stalled connects on the first connect", func() bool {
		_, hungUp := broker.stalled()
		return hungUp >= 2
	})
	if app.isClientConnected() {
		t.Error("reports connected while every CONNECT is unanswered")
	}
	broker.setStalling(false)
	waitUntil(t, connectDeadline, "the first session once the broker answers", func() bool {
		return broker.sessionComplete(0)
	})

	// Phase 2, reconnect: the broker drops the session and stalls every new
	// CONNECT. AutoReconnect must abandon those too, then reconnect.
	waitUntil(t, connectDeadline, "every first-connect stall to be abandoned", func() bool {
		connects, hungUp := broker.stalled()
		return hungUp == connects
	})
	_, before := broker.stalled()
	broker.setStalling(true)
	broker.dropSessions()
	waitUntil(t, 20*time.Second, "the client to abandon 2 stalled connects while reconnecting", func() bool {
		_, hungUp := broker.stalled()
		return hungUp >= before+2
	})
	broker.setStalling(false)
	waitUntil(t, 20*time.Second, "the reconnected session once the broker answers", func() bool {
		return broker.sessionComplete(1)
	})

	for i, d := range broker.hangupTimes() {
		if d > testConnectTimeout+connackSlack {
			t.Errorf("stalled connect %d abandoned after %v, want within ConnectTimeout %v (+%v slack)", i, d, testConnectTimeout, connackSlack)
		}
	}
	if n := built.Load(); n != 1 {
		t.Errorf("built %d clients, want 1", n)
	}
	conns := broker.connections()
	if len(conns) != 2 {
		t.Fatalf("broker saw %d answered MQTT sessions, want 2", len(conns))
	}
	for i, c := range conns {
		cp := c.connect
		if !cp.WillFlag || cp.WillTopic != aliveTopic || string(cp.WillMessage) != "offline" || !cp.WillRetain || cp.WillQos != 0 {
			t.Errorf("session %d CONNECT will = flag %v topic %q payload %q retain %v qos %d, want %q \"offline\" retained qos 0",
				i, cp.WillFlag, cp.WillTopic, cp.WillMessage, cp.WillRetain, cp.WillQos, aliveTopic)
		}
		assertConnectOrder(t, c.ops)
		if n := countOps(c.ops, "subscribe", commandWild); n != 1 {
			t.Errorf("session %d subscribed to command/# %d times, want 1", i, n)
		}
	}
}

// The production options must set a connect timeout that bounds the CONNACK
// wait and fits inside the startup wait, so a healthy broker still connects
// before the main loop starts.
func TestConnectTimeoutIsBounded(t *testing.T) {
	opts := connectTestApp(baseConfig()).mqttClientOptions()
	if opts.ConnectTimeout <= 0 || opts.ConnectTimeout >= StartupConnectWait {
		t.Errorf("ConnectTimeout = %v, want > 0 and < StartupConnectWait (%v)", opts.ConnectTimeout, StartupConnectWait)
	}
}
