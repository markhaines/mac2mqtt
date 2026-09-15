package main

import (
	"encoding/json"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"gopkg.in/yaml.v2"
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

type recordedPublish struct {
	topic    string
	retained bool
	payload  []byte
}

type fakeToken struct{}

func (fakeToken) Wait() bool                     { return true }
func (fakeToken) WaitTimeout(time.Duration) bool { return true }
func (fakeToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}
func (fakeToken) Error() error { return nil }

type fakeClient struct {
	publishes []recordedPublish
}

func (c *fakeClient) IsConnected() bool      { return true }
func (c *fakeClient) IsConnectionOpen() bool { return true }
func (c *fakeClient) Connect() mqtt.Token    { return fakeToken{} }
func (c *fakeClient) Disconnect(uint)        {}
func (c *fakeClient) Publish(topic string, _ byte, retained bool, payload interface{}) mqtt.Token {
	var b []byte
	switch v := payload.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	}
	c.publishes = append(c.publishes, recordedPublish{topic: topic, retained: retained, payload: b})
	return fakeToken{}
}
func (c *fakeClient) Subscribe(string, byte, mqtt.MessageHandler) mqtt.Token { return fakeToken{} }
func (c *fakeClient) SubscribeMultiple(map[string]byte, mqtt.MessageHandler) mqtt.Token {
	return fakeToken{}
}
func (c *fakeClient) Unsubscribe(...string) mqtt.Token        { return fakeToken{} }
func (c *fakeClient) AddRoute(string, mqtt.MessageHandler)    {}
func (c *fakeClient) OptionsReader() mqtt.ClientOptionsReader { return mqtt.ClientOptionsReader{} }

// testApp builds an Application without touching the filesystem or the broker.
func testApp(cfg *config) *Application {
	return &Application{
		config:   cfg,
		hostname: "test-host",
		topic:    "mac2mqtt/test-host",
	}
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

// --- config parsing ---------------------------------------------------------

// A config with none of the new keys must leave every sensor enabled.
func TestLegacyConfigLeavesSensorsEnabled(t *testing.T) {
	var c config
	if err := yaml.Unmarshal([]byte(legacyConfigYAML), &c); err != nil {
		t.Fatalf("unmarshal legacy config: %v", err)
	}

	if c.DisableMediaDevices {
		t.Error("disable_media_devices defaulted to true; absent key must leave the sensor enabled")
	}
	if c.DisableDisplayBrightness {
		t.Error("disable_display_brightness defaulted to true; absent key must leave the sensor enabled")
	}

	app := testApp(&c)
	if !app.mediaDevicesEnabled() {
		t.Error("mediaDevicesEnabled() = false for a legacy config")
	}
	if !app.displayBrightnessEnabled() {
		t.Error("displayBrightnessEnabled() = false for a legacy config")
	}
}

// A legacy config must parse into exactly the same nine values as before.
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

// Each new key, explicitly set to true and to false.
func TestDisableKeysParse(t *testing.T) {
	tests := []struct {
		name         string
		yaml         string
		wantMedia    bool // expected DisableMediaDevices
		wantBright   bool // expected DisableDisplayBrightness
		wantMediaOn  bool // expected mediaDevicesEnabled()
		wantBrightOn bool // expected displayBrightnessEnabled()
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
			doc := legacyConfigYAML + tc.yaml + "\n"
			var c config
			if err := yaml.Unmarshal([]byte(doc), &c); err != nil {
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

			// The legacy fields must survive the new keys untouched.
			if c.IP != "192.168.10.1" || c.Port != "1883" || c.IdleActivityTime != 42 {
				t.Errorf("new keys disturbed legacy fields: %+v", c)
			}
		})
	}
}

// A nil config (defensive) must report every sensor as enabled.
func TestNilConfigEnablesEverything(t *testing.T) {
	app := &Application{}
	if !app.mediaDevicesEnabled() || !app.displayBrightnessEnabled() {
		t.Error("nil config must leave sensors enabled")
	}
}

// --- polling ----------------------------------------------------------------

func TestUpdateMediaDevicesRespectsDisable(t *testing.T) {
	cfg := baseConfig()
	cfg.DisableMediaDevices = true
	c := &fakeClient{}
	testApp(cfg).updateMediaDevices(c)
	if len(c.publishes) != 0 {
		t.Errorf("disabled media devices published %d messages, want 0: %+v", len(c.publishes), c.publishes)
	}

	// Enabled, it publishes state either way (success or the error fallback).
	c2 := &fakeClient{}
	testApp(baseConfig()).updateMediaDevices(c2)
	if len(c2.publishes) == 0 {
		t.Error("enabled media devices published nothing; default behaviour changed")
	}
}

func TestUpdateDisplayBrightnessRespectsDisable(t *testing.T) {
	cfg := baseConfig()
	cfg.DisableDisplayBrightness = true
	app := testApp(cfg)
	// Even if a display list somehow exists, disabling must short-circuit
	// before any betterdisplaycli call.
	app.displays = []Display{{DisplayID: "1", Name: "Test Display"}}

	c := &fakeClient{}
	app.updateDisplayBrightness(c)
	if len(c.publishes) != 0 {
		t.Errorf("disabled brightness published %d messages, want 0: %+v", len(c.publishes), c.publishes)
	}
}

// An inbound brightness command must not panic or act when brightness is off.
func TestBrightnessCommandDegradesWhenDisabled(t *testing.T) {
	cfg := baseConfig()
	cfg.DisableDisplayBrightness = true
	app := testApp(cfg)
	app.displays = nil

	c := &fakeClient{}
	handled := app.handleDisplayBrightnessCommand(c, "mac2mqtt/test-host/command/display_1_brightness", "50")
	if handled {
		t.Error("disabled brightness command reported as handled; want false so other handlers still see it")
	}
	if len(c.publishes) != 0 {
		t.Errorf("disabled brightness command published %d messages, want 0", len(c.publishes))
	}
}

// --- discovery --------------------------------------------------------------

type discoveryPayload struct {
	Components        map[string]map[string]interface{} `json:"cmps"`
	AvailabilityTopic string                            `json:"availability_topic"`
}

func discoveryMessages(t *testing.T, app *Application) []discoveryPayload {
	t.Helper()
	c := &fakeClient{}
	app.setDevice(c)

	wantTopic := "homeassistant/device/test-host/config"
	var out []discoveryPayload
	for _, p := range c.publishes {
		if p.topic != wantTopic {
			continue
		}
		var d discoveryPayload
		if err := json.Unmarshal(p.payload, &d); err != nil {
			t.Fatalf("discovery payload is not valid JSON: %v", err)
		}
		if !p.retained {
			t.Error("discovery config must be published retained")
		}
		out = append(out, d)
	}
	if len(out) == 0 {
		t.Fatal("no discovery config published")
	}
	return out
}

func TestDiscoveryIncludesSensorsByDefault(t *testing.T) {
	msgs := discoveryMessages(t, testApp(baseConfig()))
	if len(msgs) != 1 {
		t.Errorf("default config published %d discovery messages, want 1", len(msgs))
	}
	final := msgs[len(msgs)-1]

	for _, key := range []string{"microphone", "camera"} {
		if _, ok := final.Components[key]; !ok {
			t.Errorf("default discovery is missing component %q", key)
		}
	}
}

func TestDiscoveryOmitsAndRetiresDisabledSensors(t *testing.T) {
	cfg := baseConfig()
	cfg.DisableMediaDevices = true
	msgs := discoveryMessages(t, testApp(cfg))

	if len(msgs) != 2 {
		t.Fatalf("got %d discovery messages, want 2 (removal stub then clean payload)", len(msgs))
	}

	// First message carries the removal stubs so HA deletes the entities.
	for _, key := range []string{"microphone", "camera"} {
		stub, ok := msgs[0].Components[key]
		if !ok {
			t.Errorf("removal message is missing stub for %q", key)
			continue
		}
		if len(stub) != 1 || stub["p"] != "binary_sensor" {
			t.Errorf("stub for %q = %v, want only {\"p\":\"binary_sensor\"}", key, stub)
		}
	}

	// Final retained payload must not mention them at all.
	for _, key := range []string{"microphone", "camera"} {
		if _, ok := msgs[len(msgs)-1].Components[key]; ok {
			t.Errorf("final discovery payload still contains %q", key)
		}
	}
}

// The availability topic and the shutdown command topic are the death signal
// and the manual shutdown path. They must be identical in every configuration.
func TestAliveAndCommandTopicsUnaffected(t *testing.T) {
	configs := []struct {
		name string
		cfg  *config
	}{
		{"default", baseConfig()},
		{"media disabled", func() *config { c := baseConfig(); c.DisableMediaDevices = true; return c }()},
		{"brightness disabled", func() *config { c := baseConfig(); c.DisableDisplayBrightness = true; return c }()},
		{"both disabled", func() *config {
			c := baseConfig()
			c.DisableMediaDevices = true
			c.DisableDisplayBrightness = true
			return c
		}()},
	}

	for _, tc := range configs {
		t.Run(tc.name, func(t *testing.T) {
			app := testApp(tc.cfg)
			if got := app.getTopicPrefix(); got != "mac2mqtt/test-host" {
				t.Fatalf("topic prefix = %q", got)
			}

			for _, msg := range discoveryMessages(t, app) {
				if msg.AvailabilityTopic != "mac2mqtt/test-host/status/alive" {
					t.Errorf("availability_topic = %q, want mac2mqtt/test-host/status/alive", msg.AvailabilityTopic)
				}
				shutdown, ok := msg.Components["shutdown"]
				if !ok {
					t.Fatal("shutdown component missing from discovery")
				}
				if shutdown["command_topic"] != "mac2mqtt/test-host/command/set" {
					t.Errorf("shutdown command_topic = %v, want mac2mqtt/test-host/command/set", shutdown["command_topic"])
				}
			}
		})
	}
}

// The shutdown payload itself must keep working regardless of sensor config.
func TestShutdownCommandUnaffectedBySensorConfig(t *testing.T) {
	cfg := baseConfig()
	cfg.DisableMediaDevices = true
	cfg.DisableDisplayBrightness = true
	app := testApp(cfg)

	// A non-shutdown payload on command/set must not be treated as a command,
	// and the topic must still be routed to the system command handler.
	if handled := app.handleSystemCommand("mac2mqtt/test-host/command/set", "notacommand"); !handled {
		t.Error("command/set was not routed to the system command handler")
	}
}
