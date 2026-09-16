package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/yaml.v2"
)

const (
	testPrefix    = "mac2mqtt/test-host"
	aliveTopic    = testPrefix + "/status/alive"
	commandWild   = testPrefix + "/command/#"
	commandSet    = testPrefix + "/command/set"
	discoveryDest = "homeassistant/device/test-host/config"
)

// legacyConfigYAML is a config file exactly as it looks today: the nine fields
// that existed before the optional-sensor keys were added, and nothing else.
const legacyConfigYAML = `
mqtt_ip: 192.168.10.1
mqtt_port: 1883
mqtt_user: someuser
mqtt_password: somepassword
mqtt_ssl: false
hostname: test-host
mqtt_topic: mac2mqtt
discovery_prefix: homeassistant
idle_activity_time: 42
`

// legacyConfig mirrors the config struct as it was before this change. Parsing
// the same YAML into both and comparing field by field is what proves an
// existing config file still means exactly what it meant before.
type legacyConfig struct {
	IP               string `yaml:"mqtt_ip"`
	Port             string `yaml:"mqtt_port"`
	User             string `yaml:"mqtt_user"`
	Password         string `yaml:"mqtt_password"`
	SSL              bool   `yaml:"mqtt_ssl"`
	Hostname         string `yaml:"hostname"`
	Topic            string `yaml:"mqtt_topic"`
	DiscoveryPrefix  string `yaml:"discovery_prefix"`
	IdleActivityTime int    `yaml:"idle_activity_time"`
}

// --- fake MQTT client -------------------------------------------------------

type op struct {
	kind     string // "publish" or "subscribe"
	topic    string
	qos      byte
	retained bool
	payload  []byte
	at       time.Time
}

type fakeToken struct {
	err   error
	delay time.Duration
}

func (t fakeToken) Wait() bool {
	if t.delay > 0 {
		time.Sleep(t.delay)
	}
	return true
}
func (t fakeToken) WaitTimeout(time.Duration) bool { return t.Wait() }
func (t fakeToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (t fakeToken) Error() error { return t.err }

// fakeClient records every operation in order, with timestamps, so that the
// relative ordering of availability, subscription and discovery is observable.
// Discovery publishes can be made to fail or to block, which is how the tests
// prove the availability path does not depend on them.
type fakeClient struct {
	mu             sync.Mutex
	ops            []op
	failDiscovery  error
	discoveryDelay time.Duration
}

func (c *fakeClient) record(o op) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o.at = time.Now()
	c.ops = append(c.ops, o)
}

func (c *fakeClient) snapshot() []op {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]op(nil), c.ops...)
}

func (c *fakeClient) IsConnected() bool      { return true }
func (c *fakeClient) IsConnectionOpen() bool { return true }
func (c *fakeClient) Connect() mqtt.Token    { return fakeToken{} }
func (c *fakeClient) Disconnect(uint)        {}

func (c *fakeClient) Publish(topic string, qos byte, retained bool, payload interface{}) mqtt.Token {
	var b []byte
	switch v := payload.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	}
	c.record(op{kind: "publish", topic: topic, qos: qos, retained: retained, payload: b})

	if topic == discoveryDest {
		return fakeToken{err: c.failDiscovery, delay: c.discoveryDelay}
	}
	return fakeToken{}
}

func (c *fakeClient) Subscribe(topic string, qos byte, _ mqtt.MessageHandler) mqtt.Token {
	c.record(op{kind: "subscribe", topic: topic, qos: qos})
	return fakeToken{}
}
func (c *fakeClient) SubscribeMultiple(map[string]byte, mqtt.MessageHandler) mqtt.Token {
	return fakeToken{}
}
func (c *fakeClient) Unsubscribe(...string) mqtt.Token        { return fakeToken{} }
func (c *fakeClient) AddRoute(string, mqtt.MessageHandler)    {}
func (c *fakeClient) OptionsReader() mqtt.ClientOptionsReader { return mqtt.ClientOptionsReader{} }

// indexOf returns the position of the first matching op, or -1.
func indexOf(ops []op, kind, topic string) int {
	for i, o := range ops {
		if o.kind == kind && o.topic == topic {
			return i
		}
	}
	return -1
}

// --- fake MQTT message ------------------------------------------------------

type fakeMessage struct {
	topic   string
	payload []byte
}

func (m fakeMessage) Duplicate() bool   { return false }
func (m fakeMessage) Qos() byte         { return 0 }
func (m fakeMessage) Retained() bool    { return false }
func (m fakeMessage) Topic() string     { return m.topic }
func (m fakeMessage) MessageID() uint16 { return 0 }
func (m fakeMessage) Payload() []byte   { return m.payload }
func (m fakeMessage) Ack()              {}

// --- helpers ----------------------------------------------------------------

func testApp(cfg *config) *Application {
	return &Application{config: cfg, hostname: "test-host", topic: testPrefix}
}

// connectTestApp neutralises the two background workers using their own
// production CAS guards, so connectHandler does not spawn a media-control
// subprocess or an endless polling goroutine during a test.
func connectTestApp(cfg *config) *Application {
	app := testApp(cfg)
	app.activityMonitorStarted.Store(true)
	app.mediaStreamStarted.Store(true)
	return app
}

func baseConfig() *config {
	return &config{
		IP:              "192.168.10.1",
		Port:            "1883",
		Hostname:        "test-host",
		Topic:           "mac2mqtt",
		DiscoveryPrefix: "homeassistant",
	}
}

// allKeyCombinations returns the four combinations of the two new keys. The
// availability and command paths must behave identically in all of them.
func allKeyCombinations() []struct {
	name string
	cfg  *config
} {
	mk := func(media, bright bool) *config {
		c := baseConfig()
		c.DisableMediaDevices = media
		c.DisableDisplayBrightness = bright
		return c
	}
	return []struct {
		name string
		cfg  *config
	}{
		{"defaults", mk(false, false)},
		{"media disabled", mk(true, false)},
		{"brightness disabled", mk(false, true)},
		{"both disabled", mk(true, true)},
	}
}

// ============================================================================
// Config parsing and backwards compatibility
// ============================================================================

func TestLegacyConfigLeavesSensorsEnabled(t *testing.T) {
	var c config
	if err := yaml.Unmarshal([]byte(legacyConfigYAML), &c); err != nil {
		t.Fatalf("unmarshal legacy config: %v", err)
	}
	if c.DisableMediaDevices || c.DisableDisplayBrightness {
		t.Fatalf("absent keys must leave sensors enabled, got %+v", c)
	}

	app := testApp(&c)
	if !app.mediaDevicesEnabled() || !app.displayBrightnessEnabled() {
		t.Error("legacy config must leave both sensors enabled")
	}
}

func TestLegacyConfigParsesIdentically(t *testing.T) {
	var got config
	if err := yaml.Unmarshal([]byte(legacyConfigYAML), &got); err != nil {
		t.Fatalf("unmarshal into config: %v", err)
	}
	var want legacyConfig
	if err := yaml.Unmarshal([]byte(legacyConfigYAML), &want); err != nil {
		t.Fatalf("unmarshal into legacyConfig: %v", err)
	}

	if got.IP != want.IP || got.Port != want.Port || got.User != want.User ||
		got.Password != want.Password || got.SSL != want.SSL ||
		got.Hostname != want.Hostname || got.Topic != want.Topic ||
		got.DiscoveryPrefix != want.DiscoveryPrefix ||
		got.IdleActivityTime != want.IdleActivityTime {
		t.Errorf("legacy fields changed meaning:\n got  %+v\n want %+v", got, want)
	}
}

func TestDisableKeysParse(t *testing.T) {
	tests := []struct {
		name                                             string
		yaml                                             string
		wantMedia, wantBright, wantMediaOn, wantBrightOn bool
	}{
		{"both absent", "", false, false, true, true},
		{"media true", "disable_media_devices: true", true, false, false, true},
		{"media false", "disable_media_devices: false", false, false, true, true},
		{"brightness true", "disable_display_brightness: true", false, true, true, false},
		{"brightness false", "disable_display_brightness: false", false, false, true, true},
		{"both true", "disable_media_devices: true\ndisable_display_brightness: true", true, true, false, false},
		{"both false", "disable_media_devices: false\ndisable_display_brightness: false", false, false, true, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var c config
			if err := yaml.Unmarshal([]byte(legacyConfigYAML+tc.yaml+"\n"), &c); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if c.DisableMediaDevices != tc.wantMedia {
				t.Errorf("DisableMediaDevices = %v, want %v", c.DisableMediaDevices, tc.wantMedia)
			}
			if c.DisableDisplayBrightness != tc.wantBright {
				t.Errorf("DisableDisplayBrightness = %v, want %v", c.DisableDisplayBrightness, tc.wantBright)
			}

			app := testApp(&c)
			if app.mediaDevicesEnabled() != tc.wantMediaOn {
				t.Errorf("mediaDevicesEnabled() = %v, want %v", app.mediaDevicesEnabled(), tc.wantMediaOn)
			}
			if app.displayBrightnessEnabled() != tc.wantBrightOn {
				t.Errorf("displayBrightnessEnabled() = %v, want %v", app.displayBrightnessEnabled(), tc.wantBrightOn)
			}
			if c.IP != "192.168.10.1" || c.Port != "1883" || c.IdleActivityTime != 42 {
				t.Errorf("new keys disturbed legacy fields: %+v", c)
			}
		})
	}
}

func TestNilConfigEnablesEverything(t *testing.T) {
	app := &Application{}
	if !app.mediaDevicesEnabled() || !app.displayBrightnessEnabled() {
		t.Error("nil config must leave sensors enabled")
	}
}

// ============================================================================
// The death signal: will, connect-time availability, subscription, heartbeat
// ============================================================================

// The last will is what tells Home Assistant this host died. It must be
// present, retained, and identical in every sensor configuration.
func TestWillIsConfiguredInEveryConfiguration(t *testing.T) {
	for _, tc := range allKeyCombinations() {
		t.Run(tc.name, func(t *testing.T) {
			opts := testApp(tc.cfg).mqttClientOptions()

			if !opts.WillEnabled {
				t.Fatal("will is not enabled; the death signal would never fire")
			}
			if opts.WillTopic != aliveTopic {
				t.Errorf("will topic = %q, want %q", opts.WillTopic, aliveTopic)
			}
			if string(opts.WillPayload) != "offline" {
				t.Errorf("will payload = %q, want \"offline\"", opts.WillPayload)
			}
			if !opts.WillRetained {
				t.Error("will must be retained, or a late subscriber never sees the death")
			}
		})
	}
}

// ORDERING. Availability and the command subscription must both happen before
// any discovery publish, in every configuration. This is the regression test
// for discovery work sitting in front of the death signal.
func TestConnectPublishesAliveAndSubscribesBeforeDiscovery(t *testing.T) {
	for _, tc := range allKeyCombinations() {
		t.Run(tc.name, func(t *testing.T) {
			c := &fakeClient{}
			connectTestApp(tc.cfg).connectHandler(c)
			ops := c.snapshot()

			aliveIdx := indexOf(ops, "publish", aliveTopic)
			subIdx := indexOf(ops, "subscribe", commandWild)
			discIdx := indexOf(ops, "publish", discoveryDest)

			if aliveIdx < 0 {
				t.Fatal("connect did not publish availability")
			}
			if subIdx < 0 {
				t.Fatal("connect did not subscribe to command/#")
			}
			if discIdx < 0 {
				t.Fatal("connect did not publish discovery (test setup problem)")
			}

			if aliveIdx > discIdx {
				t.Errorf("availability published AFTER discovery (alive at %d, discovery at %d): "+
					"the death signal is gated on discovery", aliveIdx, discIdx)
			}
			if subIdx > discIdx {
				t.Errorf("command/# subscribed AFTER discovery (sub at %d, discovery at %d): "+
					"the shutdown path is gated on discovery", subIdx, discIdx)
			}

			if !ops[aliveIdx].retained {
				t.Error("availability must be published retained")
			}
			if string(ops[aliveIdx].payload) != "online" {
				t.Errorf("availability payload = %q, want \"online\"", ops[aliveIdx].payload)
			}
		})
	}
}

// A discovery publish that fails outright must not prevent availability or the
// subscription, and must not abort the connect handler.
func TestAliveSurvivesDiscoveryFailure(t *testing.T) {
	c := &fakeClient{failDiscovery: errors.New("broker rejected discovery")}
	connectTestApp(baseConfig()).connectHandler(c)
	ops := c.snapshot()

	aliveIdx := indexOf(ops, "publish", aliveTopic)
	subIdx := indexOf(ops, "subscribe", commandWild)
	discIdx := indexOf(ops, "publish", discoveryDest)

	if aliveIdx < 0 {
		t.Fatal("availability was not published when discovery failed")
	}
	if subIdx < 0 {
		t.Fatal("command/# was not subscribed when discovery failed")
	}
	// Presence alone would still pass with discovery ordered first, so assert
	// the ordering here too: neither may be downstream of a failing publish.
	if discIdx >= 0 && (aliveIdx > discIdx || subIdx > discIdx) {
		t.Errorf("availability/subscription came after the failing discovery publish (alive %d, sub %d, discovery %d)",
			aliveIdx, subIdx, discIdx)
	}
	if !ops[aliveIdx].retained || string(ops[aliveIdx].payload) != "online" {
		t.Errorf("availability = retained %v payload %q, want retained true payload \"online\"",
			ops[aliveIdx].retained, ops[aliveIdx].payload)
	}
}

// A slow broker must not delay availability. The discovery publish blocks for
// well over the tolerance; availability must already have happened.
func TestAliveNotDelayedBySlowDiscovery(t *testing.T) {
	const discoveryBlocksFor = 400 * time.Millisecond
	const tolerance = 150 * time.Millisecond

	c := &fakeClient{discoveryDelay: discoveryBlocksFor}
	start := time.Now()
	connectTestApp(baseConfig()).connectHandler(c)
	ops := c.snapshot()

	aliveIdx := indexOf(ops, "publish", aliveTopic)
	subIdx := indexOf(ops, "subscribe", commandWild)
	if aliveIdx < 0 || subIdx < 0 {
		t.Fatal("availability or subscription missing")
	}

	if d := ops[aliveIdx].at.Sub(start); d > tolerance {
		t.Errorf("availability took %v to publish, want under %v: it is waiting on discovery", d, tolerance)
	}
	if d := ops[subIdx].at.Sub(start); d > tolerance {
		t.Errorf("subscription took %v, want under %v: it is waiting on discovery", d, tolerance)
	}
}

// The 60s heartbeat must republish availability in every configuration.
// Disabling a sensor must never stop the host saying it is alive.
func TestPeriodicStatusAlwaysPublishesAlive(t *testing.T) {
	for _, tc := range allKeyCombinations() {
		t.Run(tc.name, func(t *testing.T) {
			c := &fakeClient{}
			testApp(tc.cfg).publishPeriodicStatus(c)

			idx := indexOf(c.snapshot(), "publish", aliveTopic)
			if idx < 0 {
				t.Fatal("periodic tick did not publish availability")
			}
			ops := c.snapshot()
			if !ops[idx].retained || string(ops[idx].payload) != "online" {
				t.Errorf("heartbeat = retained %v payload %q, want retained true payload \"online\"",
					ops[idx].retained, ops[idx].payload)
			}
		})
	}
}

// ============================================================================
// Command dispatch through the normal routing path
// ============================================================================

// A 'shutdown' payload on command/set must reach the shutdown action, in every
// sensor configuration, routed the way a real broker message is routed. The
// action is swapped out so the machine is not actually powered off.
func TestShutdownDispatchesThroughNormalPath(t *testing.T) {
	for _, tc := range allKeyCombinations() {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			original := systemShutdown
			systemShutdown = func() { calls++ }
			t.Cleanup(func() { systemShutdown = original })

			app := testApp(tc.cfg)
			app.messagePubHandler(&fakeClient{}, fakeMessage{topic: commandSet, payload: []byte("shutdown")})

			if calls != 1 {
				t.Errorf("shutdown action called %d times, want 1: the manual shutdown path is broken", calls)
			}
		})
	}
}

// Every system command must reach its own action and nothing else. This also
// proves no other payload can trigger a shutdown.
func TestSystemCommandDispatchIsExact(t *testing.T) {
	payloads := []string{"sleep", "displaysleep", "displaywake", "shutdown", "screensaver", "notacommand"}

	for _, payload := range payloads {
		t.Run(payload, func(t *testing.T) {
			fired := map[string]int{}
			seams := map[string]*func(){
				"sleep":        &systemSleep,
				"displaysleep": &systemDisplaySleep,
				"displaywake":  &systemDisplayWake,
				"shutdown":     &systemShutdown,
				"screensaver":  &systemScreensaver,
			}
			for name, seam := range seams {
				original := *seam
				n := name
				*seam = func() { fired[n]++ }
				t.Cleanup(func() { *seam = original })
			}

			app := testApp(baseConfig())
			app.messagePubHandler(&fakeClient{}, fakeMessage{topic: commandSet, payload: []byte(payload)})

			for name := range seams {
				want := 0
				if name == payload {
					want = 1
				}
				if fired[name] != want {
					t.Errorf("payload %q fired %q %d times, want %d", payload, name, fired[name], want)
				}
			}
		})
	}
}

// A command on a topic we do not own must not trigger anything.
func TestForeignTopicDoesNotDispatch(t *testing.T) {
	var calls int
	original := systemShutdown
	systemShutdown = func() { calls++ }
	t.Cleanup(func() { systemShutdown = original })

	app := testApp(baseConfig())
	app.messagePubHandler(&fakeClient{}, fakeMessage{
		topic:   "mac2mqtt/some-other-host/command/set",
		payload: []byte("shutdown"),
	})

	if calls != 0 {
		t.Errorf("shutdown fired %d times for another host's topic, want 0", calls)
	}
}

// ============================================================================
// Polling
// ============================================================================

func TestUpdateMediaDevicesRespectsDisable(t *testing.T) {
	cfg := baseConfig()
	cfg.DisableMediaDevices = true
	c := &fakeClient{}
	testApp(cfg).updateMediaDevices(c)
	if got := len(c.snapshot()); got != 0 {
		t.Errorf("disabled media devices published %d messages, want 0", got)
	}

	c2 := &fakeClient{}
	testApp(baseConfig()).updateMediaDevices(c2)
	if len(c2.snapshot()) == 0 {
		t.Error("enabled media devices published nothing; default behaviour changed")
	}
}

func TestUpdateDisplayBrightnessRespectsDisable(t *testing.T) {
	cfg := baseConfig()
	cfg.DisableDisplayBrightness = true
	app := testApp(cfg)
	// Even with a populated display list, disabling must short-circuit before
	// any betterdisplaycli call.
	app.displays = []Display{{DisplayID: "1", Name: "Test Display"}}

	c := &fakeClient{}
	app.updateDisplayBrightness(c)
	if got := len(c.snapshot()); got != 0 {
		t.Errorf("disabled brightness published %d messages, want 0", got)
	}
}

func TestBrightnessCommandDegradesWhenDisabled(t *testing.T) {
	cfg := baseConfig()
	cfg.DisableDisplayBrightness = true
	app := testApp(cfg)
	app.displays = []Display{{DisplayID: "1", Name: "Test Display"}}

	c := &fakeClient{}
	handled := app.handleDisplayBrightnessCommand(c, testPrefix+"/command/display_1_brightness", "50")
	if handled {
		t.Error("disabled brightness command reported as handled; want false")
	}
	if got := len(c.snapshot()); got != 0 {
		t.Errorf("disabled brightness command published %d messages, want 0", got)
	}
}

// ============================================================================
// Discovery
// ============================================================================

type discoveryPayload struct {
	Components        map[string]map[string]interface{} `json:"cmps"`
	AvailabilityTopic string                            `json:"availability_topic"`
}

// discoveryFrom runs setDevice and returns every discovery payload published.
func discoveryFrom(t *testing.T, app *Application) []discoveryPayload {
	t.Helper()
	c := &fakeClient{}
	app.setDevice(c)

	var out []discoveryPayload
	for _, o := range c.snapshot() {
		if o.kind != "publish" || o.topic != discoveryDest {
			continue
		}
		if !o.retained {
			t.Error("discovery config must be published retained")
		}
		// QoS 0, as main published it. Raising this is a default-path
		// behaviour change; TestDefaultPathMatchesLegacyOnWireBehaviour is
		// the authority, this is a fast local guard.
		if o.qos != 0 {
			t.Errorf("discovery published at QoS %d, want 0 (main's value)", o.qos)
		}
		var d discoveryPayload
		if err := json.Unmarshal(o.payload, &d); err != nil {
			t.Fatalf("discovery payload is not valid JSON: %v", err)
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		t.Fatal("no discovery config published")
	}
	return out
}

// With a populated display list, the brightness entity is offered when the
// sensor is on and absent when it is off.
func TestDiscoveryBrightnessWithNonEmptyDisplayList(t *testing.T) {
	displays := []Display{
		{DisplayID: "1", Name: "Studio Display"},
		{DisplayID: "2", Name: "Default Group"},
	}

	t.Run("enabled", func(t *testing.T) {
		app := testApp(baseConfig())
		app.displays = displays
		msgs := discoveryFrom(t, app)
		final := msgs[len(msgs)-1]

		for _, d := range displays {
			key := "display_" + d.DisplayID + "_brightness"
			comp, ok := final.Components[key]
			if !ok {
				t.Fatalf("brightness component %q missing with displays present", key)
			}
			if comp["command_topic"] != testPrefix+"/command/"+key {
				t.Errorf("%s command_topic = %v", key, comp["command_topic"])
			}
			if comp["state_topic"] != testPrefix+"/status/"+key {
				t.Errorf("%s state_topic = %v", key, comp["state_topic"])
			}
		}
	})

	t.Run("disabled", func(t *testing.T) {
		cfg := baseConfig()
		cfg.DisableDisplayBrightness = true
		app := testApp(cfg)
		// A stale display list must not be able to reintroduce the entity.
		app.displays = displays

		final := discoveryFrom(t, app)
		for key := range final[len(final)-1].Components {
			if len(key) > 8 && key[:8] == "display_" {
				t.Errorf("brightness component %q published while disabled", key)
			}
		}
	})
}

func TestDiscoveryIncludesMediaSensorsByDefault(t *testing.T) {
	app := testApp(baseConfig())
	app.displays = []Display{{DisplayID: "1", Name: "Test Display"}}
	msgs := discoveryFrom(t, app)

	if len(msgs) != 1 {
		t.Errorf("published %d discovery messages, want exactly 1", len(msgs))
	}
	for _, key := range []string{"microphone", "camera"} {
		if _, ok := msgs[len(msgs)-1].Components[key]; !ok {
			t.Errorf("default discovery is missing component %q", key)
		}
	}
}

// A disabled sensor is simply absent, and exactly one payload is published.
func TestDiscoveryOmitsDisabledMediaSensors(t *testing.T) {
	cfg := baseConfig()
	cfg.DisableMediaDevices = true
	msgs := discoveryFrom(t, testApp(cfg))

	if len(msgs) != 1 {
		t.Errorf("published %d discovery messages, want exactly 1", len(msgs))
	}
	for _, key := range []string{"microphone", "camera"} {
		if _, ok := msgs[len(msgs)-1].Components[key]; ok {
			t.Errorf("discovery still contains %q while disabled", key)
		}
	}
}

// The availability topic and the shutdown command topic must be byte-identical
// in the discovery payload in every configuration.
func TestDiscoveryAliveAndCommandTopicsUnaffected(t *testing.T) {
	for _, tc := range allKeyCombinations() {
		t.Run(tc.name, func(t *testing.T) {
			app := testApp(tc.cfg)
			app.displays = []Display{{DisplayID: "1", Name: "Test Display"}}

			if got := app.getTopicPrefix(); got != testPrefix {
				t.Fatalf("topic prefix = %q, want %q", got, testPrefix)
			}

			for _, msg := range discoveryFrom(t, app) {
				if msg.AvailabilityTopic != aliveTopic {
					t.Errorf("availability_topic = %q, want %q", msg.AvailabilityTopic, aliveTopic)
				}
				shutdown, ok := msg.Components["shutdown"]
				if !ok {
					t.Fatal("shutdown component missing from discovery")
				}
				if shutdown["command_topic"] != commandSet {
					t.Errorf("shutdown command_topic = %v, want %q", shutdown["command_topic"], commandSet)
				}
			}
		})
	}
}

// ============================================================================
// Backwards compatibility: absent keys vs explicit false vs explicit true
// ============================================================================

// parseConfig builds a config from the nine legacy fields plus whatever extra
// YAML lines a case supplies, so that "key absent" is genuinely absent from the
// document rather than merely left at its zero value in a struct literal.
func parseConfig(t *testing.T, extra string) *config {
	t.Helper()
	var c config
	if err := yaml.Unmarshal([]byte(legacyConfigYAML+extra+"\n"), &c); err != nil {
		t.Fatalf("unmarshal config: %v", err)
	}
	return &c
}

// keyStates is the three states the contract names explicitly: the keys absent
// from the file entirely, present and false, present and true.
func keyStates() []struct {
	name  string
	extra string
} {
	return []struct {
		name  string
		extra string
	}{
		{"keys absent", ""},
		{"keys explicitly false", "disable_media_devices: false\ndisable_display_brightness: false"},
		{"keys explicitly true", "disable_media_devices: true\ndisable_display_brightness: true"},
	}
}

// The death signal and the shutdown path must be identical in all three key
// states: same topic, same retained payload, same subscription, and both still
// ahead of discovery.
func TestAliveAndCommandUnaffectedInAllThreeKeyStates(t *testing.T) {
	for _, tc := range keyStates() {
		t.Run(tc.name, func(t *testing.T) {
			c := &fakeClient{}
			connectTestApp(parseConfig(t, tc.extra)).connectHandler(c)
			ops := c.snapshot()

			aliveIdx := indexOf(ops, "publish", aliveTopic)
			subIdx := indexOf(ops, "subscribe", commandWild)
			discIdx := indexOf(ops, "publish", discoveryDest)

			if aliveIdx < 0 {
				t.Fatal("availability was not published")
			}
			if subIdx < 0 {
				t.Fatal("command/# was not subscribed")
			}
			if !ops[aliveIdx].retained || string(ops[aliveIdx].payload) != "online" {
				t.Errorf("availability = retained %v payload %q, want retained true payload \"online\"",
					ops[aliveIdx].retained, ops[aliveIdx].payload)
			}
			if discIdx >= 0 && (aliveIdx > discIdx || subIdx > discIdx) {
				t.Errorf("availability/subscription landed after discovery (alive %d, sub %d, discovery %d)",
					aliveIdx, subIdx, discIdx)
			}

			// The shutdown path must still dispatch.
			var calls int
			original := systemShutdown
			systemShutdown = func() { calls++ }
			t.Cleanup(func() { systemShutdown = original })

			testApp(parseConfig(t, tc.extra)).messagePubHandler(
				&fakeClient{}, fakeMessage{topic: commandSet, payload: []byte("shutdown")})
			if calls != 1 {
				t.Errorf("shutdown dispatched %d times, want 1", calls)
			}
		})
	}
}

// ============================================================================
// Backwards compatibility against the LEGACY fixture
//
// testdata/legacy-onwire-b5a986a.json records what main (b5a986a) actually put
// on the wire, captured by running that commit's own connectHandler under a
// recording client. The generator is kept beside it as
// legacy-capture-generator.go.txt.
//
// This is the point of the fixture: comparing the new code's default path
// against another run of the new code cannot detect a behaviour change, because
// both sides move together. Only a baseline taken from main can fail when the
// default path drifts, which is exactly what a QoS change on discovery did.
// ============================================================================

type legacyOp struct {
	Kind     string `json:"kind"`
	Topic    string `json:"topic"`
	QoS      byte   `json:"qos"`
	Retained bool   `json:"retained"`
	Payload  string `json:"payload,omitempty"`
}

type legacyFixture struct {
	Source                 string            `json:"source_commit"`
	MediaControlAvailable  bool              `json:"media_control_available"`
	ConnectOps             []legacyOp        `json:"connect_ops"`
	DiscoveryComponentKeys []string          `json:"discovery_component_keys"`
	DiscoveryAttrs         map[string]string `json:"discovery_attrs"`
	PeriodicOps            []legacyOp        `json:"periodic_ops"`
	Will                   legacyOp          `json:"will"`
}

func loadLegacyFixture(t *testing.T) legacyFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "legacy-onwire-b5a986a.json"))
	if err != nil {
		t.Fatalf("read legacy fixture: %v", err)
	}
	var f legacyFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse legacy fixture: %v", err)
	}
	// The fixture records which optional external tools were present when it
	// was captured. Comparing against it on a machine configured differently
	// would compare the environment, not the code.
	if f.MediaControlAvailable != isMediaControlAvailable() {
		t.Skipf("fixture captured with media-control available=%v, this machine has %v",
			f.MediaControlAvailable, isMediaControlAvailable())
	}
	return f
}

// signature reduces an op to the on-wire attributes the compatibility contract
// covers. Payloads carrying live system readings are excluded; the two that are
// config-derived (availability and discovery) are handled separately.
func signature(o op) legacyOp {
	l := legacyOp{Kind: o.kind, Topic: o.topic, QoS: o.qos, Retained: o.retained}
	if o.topic == aliveTopic {
		l.Payload = string(o.payload)
	}
	return l
}

func sortOps(in []legacyOp) []legacyOp {
	out := append([]legacyOp(nil), in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

func diffOps(t *testing.T, label string, got, want []legacyOp) {
	t.Helper()
	g, w := sortOps(got), sortOps(want)
	if len(g) != len(w) {
		t.Errorf("%s: produced %d operations, main produced %d\n got:  %+v\n main: %+v",
			label, len(g), len(w), g, w)
		return
	}
	for i := range g {
		if g[i] != w[i] {
			t.Errorf("%s: operation differs from main\n got:  %+v\n main: %+v", label, g[i], w[i])
		}
	}
}

// With the keys absent, every on-wire attribute of the connect path must match
// what main produced: same topics, same QoS, same retained flags, same
// availability payload.
//
// Ordering is deliberately NOT compared here. Moving availability and the
// subscription ahead of discovery is the one intentional deviation from main,
// and it has its own dedicated tests
// (TestConnectPublishesAliveAndSubscribesBeforeDiscovery and
// TestAliveNotDelayedBySlowDiscovery). Everything else must be identical.
func TestDefaultPathMatchesLegacyOnWireBehaviour(t *testing.T) {
	f := loadLegacyFixture(t)

	c := &fakeClient{}
	connectTestApp(parseConfig(t, "")).connectHandler(c)

	var got []legacyOp
	for _, o := range c.snapshot() {
		got = append(got, signature(o))
	}
	diffOps(t, "connect path with keys absent", got, f.ConnectOps)
}

// The discovery payload must offer main's exact component set and the same
// availability topic when the keys are absent.
func TestDefaultDiscoveryMatchesLegacyComponents(t *testing.T) {
	f := loadLegacyFixture(t)

	msgs := discoveryFrom(t, testApp(parseConfig(t, "")))
	final := msgs[len(msgs)-1]

	var got []string
	for k := range final.Components {
		got = append(got, k)
	}
	sort.Strings(got)

	want := append([]string(nil), f.DiscoveryComponentKeys...)
	sort.Strings(want)

	if len(got) != len(want) {
		t.Errorf("component count %d, main had %d\n got:  %v\n main: %v", len(got), len(want), got, want)
	}
	missing := map[string]bool{}
	for _, k := range want {
		missing[k] = true
	}
	for _, k := range got {
		delete(missing, k)
	}
	for k := range missing {
		t.Errorf("component %q was published by main but is missing with the keys absent", k)
	}

	if final.AvailabilityTopic != f.DiscoveryAttrs["availability_topic"] {
		t.Errorf("availability_topic = %q, main had %q",
			final.AvailabilityTopic, f.DiscoveryAttrs["availability_topic"])
	}
}

// The 60s heartbeat must put the same operations on the wire as main's inline
// tick body did, at the same QoS and retained flags.
func TestPeriodicTickMatchesLegacyOnWireBehaviour(t *testing.T) {
	f := loadLegacyFixture(t)

	c := &fakeClient{}
	testApp(parseConfig(t, "")).publishPeriodicStatus(c)

	var got []legacyOp
	for _, o := range c.snapshot() {
		got = append(got, signature(o))
	}
	diffOps(t, "periodic tick with keys absent", got, f.PeriodicOps)
}

// The last will must match main exactly, in every key state: it is the signal
// the broker sends on this host's behalf when it dies.
func TestWillMatchesLegacyInEveryKeyState(t *testing.T) {
	f := loadLegacyFixture(t)

	for _, tc := range keyStates() {
		t.Run(tc.name, func(t *testing.T) {
			opts := testApp(parseConfig(t, tc.extra)).mqttClientOptions()
			got := legacyOp{
				Kind: "will", Topic: opts.WillTopic, QoS: opts.WillQos,
				Retained: opts.WillRetained, Payload: string(opts.WillPayload),
			}
			if !opts.WillEnabled {
				t.Fatal("will is not enabled")
			}
			if got != f.Will {
				t.Errorf("will differs from main\ngot:  %+v\nmain: %+v", got, f.Will)
			}
		})
	}
}

// Absent keys must publish exactly the discovery components an explicit-false
// config does, including the camera and microphone that the disable keys would
// otherwise remove.
func TestAbsentKeysPublishSameDiscoveryComponents(t *testing.T) {
	displays := []Display{{DisplayID: "1", Name: "Studio Display"}}

	componentsFor := func(extra string) map[string]map[string]interface{} {
		app := testApp(parseConfig(t, extra))
		app.displays = displays
		msgs := discoveryFrom(t, app)
		return msgs[len(msgs)-1].Components
	}

	absent := componentsFor("")
	explicitFalse := componentsFor("disable_media_devices: false\ndisable_display_brightness: false")

	if len(absent) != len(explicitFalse) {
		t.Errorf("component count differs: absent %d, explicit false %d", len(absent), len(explicitFalse))
	}
	for key := range explicitFalse {
		if _, ok := absent[key]; !ok {
			t.Errorf("component %q present with explicit false but missing with keys absent", key)
		}
	}

	// And the sensors this change can disable are present in both.
	for _, key := range []string{"microphone", "camera", "display_1_brightness"} {
		if _, ok := absent[key]; !ok {
			t.Errorf("component %q missing when keys are absent; upgrading would change behaviour", key)
		}
	}
}
