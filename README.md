# Mac2MQTT (updated)

`mac2mqtt` is a program that allows viewing and controlling some aspects of computers running macOS via MQTT.

This repo is a fork of bessarabov/mac2mqtt, that add MQTT Autodiscovery and a KeepAwake function for the mac.

It publishes to MQTT:

 * current volume
 * volume mute state
 * battery charge percent
 * **media player information (title, artist, album, app name, state)**
 * **user activity status (active/inactive with 10-second timeout)**

You can send topics to:

 * change volume
 * mute/unmute
 * put the computer to sleep
 * shutdown computer
 * turn off the display
 * wake display
 * run macOS shortcuts

## Dependencies

### Required
- macOS (for system commands)
- MQTT broker

### Optional
- **BetterDisplay CLI** - for display brightness control
  - Install BetterDisplay from https://github.com/waydabber/BetterDisplay
  - Enable CLI access in BetterDisplay settings
- **Media Control** - for media player information
  - Install via npm: `npm install -g media-control`
  - Or install via Homebrew: `brew install media-control`
  - Provides current media playback information (title, artist, album, app name, state, duration, position)
r:



## Installation

### Quick Installation (Recommended)

The easiest way to install Mac2MQTT is using the provided installer script:

```bash
./install.sh
```

The installer will guide you through the entire setup process, including:
- Building the application
- Configuring MQTT settings
- Installing optional dependencies
- Setting up the service to run automatically
- Creating management scripts

### Manual Installation

If you prefer to install manually, follow these steps:

### Running

To run this program you need to put 2 files in a directory (`/Users/USERNAME/mac2mqtt/`):

    mac2mqtt
    mac2mqtt.yaml

Edit `mac2mqtt.yaml` (the sample file is in this repository), make binary executable (`chmod +x mac2mqtt`) and run `./mac2mqtt`:

    $ ./mac2mqtt
    2021/04/12 10:37:28 Started
    2021/04/12 10:37:29 Connected to MQTT
    2021/04/12 10:37:29 Sending 'true' to topic: mac2mqtt/bessarabov-osx/status/alive

### Configuration

All settings live in `mac2mqtt.yaml`. Only `mqtt_ip` and `mqtt_port` are required.

| Key | Default | Description |
| --- | --- | --- |
| `mqtt_ip` | required | Broker address |
| `mqtt_port` | required | Broker port |
| `mqtt_user` | empty | Broker username |
| `mqtt_password` | empty | Broker password |
| `mqtt_ssl` | `false` | Connect over TLS |
| `hostname` | system hostname | Name used in topics and in Home Assistant |
| `mqtt_topic` | `mac2mqtt` | Topic prefix, the hostname is appended to it |
| `discovery_prefix` | `homeassistant` | Home Assistant discovery prefix |
| `idle_activity_time` | `10` | Seconds of inactivity before user activity flips to inactive |
| `disable_media_devices` | `false` | Switch off the camera and microphone sensors |
| `disable_display_brightness` | `false` | Switch off the display brightness controls |

#### Disabling sensors on headless and server installs

`disable_media_devices` and `disable_display_brightness` exist for Macs that run
as always-on servers: a machine in a rack with no camera, or one whose display is
not worth controlling from Home Assistant. On those machines the two sensors poll
hardware that is not really there, fail every cycle, and write a steady stream of
errors into the log for no benefit.

    # Headless server: no camera, and the display is not controlled from HA
    disable_media_devices: true
    disable_display_brightness: true

`disable_media_devices: true` stops the camera and microphone from being polled
and stops both binary sensors being offered to Home Assistant.

`disable_display_brightness: true` stops BetterDisplay CLI from being called at
all. No display is enumerated at startup, no brightness is polled, and no
brightness control is offered to Home Assistant.

Both keys are optional and both default to `false`. **Omitting them keeps the
current behaviour exactly**, so an existing installation can upgrade the binary
without touching its config file and nothing changes.

Everything else is unaffected in every configuration. In particular the
availability topic `PREFIX/status/alive` and the command topic
`PREFIX/command/set` (which carries `shutdown`) are identical in content and in
timing whether these sensors are on or off. On connect, the retained `online`
message is published and `PREFIX/command/#` is subscribed *before* any discovery
work, so a slow or failing discovery publish can never delay or suppress the
availability signal or the shutdown path.

#### Removing entities that already exist

A disabled sensor is left out of the discovery payload, so it is never created
on a fresh install. It is **not** deleted automatically if a previous version of
mac2mqtt already created it.

This is a deliberate choice rather than an oversight. The discovery topic is
retained, which means Home Assistant only ever sees the most recent payload.
Publishing a removal instruction and then a clean payload would be honoured only
if Home Assistant happened to be connected in the gap between the two, so it
would work most of the time and silently fail the rest, which is worse than
being told to do it yourself.

So if you switch a sensor off on an installation that was already running it,
remove the leftover entities once by hand. The order matters: dropping a
component from the discovery payload does not unload an entity that Home
Assistant has already set up, and until Home Assistant reloads it the entity
keeps its last state and the device's availability. Its **Delete** button stays
greyed out in that state, so deleting has to come last.

1. Add the key to `mac2mqtt.yaml` and restart mac2mqtt.
2. Confirm the new **retained** discovery payload no longer lists the sensor:

   ```sh
   mosquitto_sub -h <broker> -t '<discovery_prefix>/device/<hostname>/config' \
       --retained-only -W 5
   ```

   `--retained-only` is what makes this meaningful: it ignores live traffic, so
   anything printed really is the retained payload a newly-started Home
   Assistant would receive. `-W 5` exits after five seconds, so nothing printed
   means nothing is retained on that topic.

   Substitute `<discovery_prefix>` with your `discovery_prefix` setting
   (`homeassistant` unless you changed it) and `<hostname>` with your
   `hostname`. Add `-p <port>` for a non-default port, `-u <user> -P <pass>` if
   the broker needs credentials, and for TLS `-p 8883 --cafile <ca.crt>`.
3. Reload the MQTT integration (**Settings > Devices & Services > MQTT >
   ... > Reload**), or restart Home Assistant. This is the step that unloads
   the entity; skip it and the Delete button stays disabled.
4. Now delete the leftover entities: **Settings > Devices & Services > MQTT**,
   pick the device, open each stale entity and delete it.

This is a one-time step and they will not come back. It applies to both keys:
`disable_media_devices` leaves a `Camera` and a `Microphone` binary sensor
behind, and `disable_display_brightness` leaves one `<display> Brightness`
number entity per display that was previously detected.

### Running in the background

You need `mac2mqtt.yaml` and `mac2mqtt` to be placed in the directory `/Users/USERNAME/mac2mqtt/`,
then you need edit the file `com.hagak.mac2mqtt.plist`
and replace `USERNAME` with your username. Then put the file in `/Library/LaunchAgents/`.

And run:

    launchctl load /Library/LaunchAgents/com.hagak.mac2mqtt.plist

(To stop you need to run `launchctl unload /Library/LaunchAgents/com.hagak.mac2mqtt.plist`)

## Home Assistant sample config

![](https://user-images.githubusercontent.com/47263/114361105-753c4200-9b7e-11eb-833c-c26a2b7d0e00.png)

### Autodiscovery

The application supports Home Assistant MQTT autodiscovery. When connected to Home Assistant, it will automatically create:

- **Media Player** - Shows current playing media (requires Media Control)
- **Volume Control** - Number slider for system volume
- **Mute Switch** - Toggle for system mute
- **Battery Sensor** - Battery percentage (laptops only)
- **Keep Awake Switch** - Toggle to prevent system sleep
- **System Buttons** - Sleep, shutdown, display sleep/wake, screensaver
- **Display Brightness Controls** - Individual brightness sliders for each display (requires BetterDisplay CLI)
- **User Activity Sensor** - Binary sensor showing active/inactive state with 10-second timeout

### Manual Configuration

If you prefer manual configuration, here's a sample:

`configuration.yaml`:

```yaml
script:
  air2_sleep:
    icon: mdi:laptop
    sequence:
      - service: mqtt.publish
        data:
          topic: "mac2mqtt/bessarabov-osx/command/sleep"
          payload: "sleep"

  air2_shutdown:
    icon: mdi:laptop
    sequence:
      - service: mqtt.publish
        data:
          topic: "mac2mqtt/bessarabov-osx/command/shutdown"
          payload: "shutdown"

  air2_displaysleep:
    icon: mdi:laptop
    sequence:
      - service: mqtt.publish
        data:
          topic: "mac2mqtt/bessarabov-osx/command/displaysleep"
          payload: "displaysleep"

mqtt:
  sensor:
    - name: air2_alive
      icon: mdi:laptop
      state_topic: "mac2mqtt/bessarabov-osx/status/alive"

    - name: "air2_battery"
      icon: mdi:battery-high
      unit_of_measurement: "%"
      state_topic: "mac2mqtt/bessarabov-osx/status/battery"

  media_player:
    - name: "air2_media_player"
      icon: mdi:music
      state_topic: "mac2mqtt/bessarabov-osx/status/media_player"
      value_template: "{{ value_json.state }}"
      json_attributes_topic: "mac2mqtt/bessarabov-osx/status/media_player"
      json_attributes_template: "{{ {'title': value_json.title, 'artist': value_json.artist, 'album': value_json.album, 'app_name': value_json.app_name, 'duration': value_json.duration, 'position': value_json.position} | tojson }}"
      availability_topic: "mac2mqtt/bessarabov-osx/status/alive"
      payload_available: "online"
      payload_not_available: "offline"
```

## MQTT topics structure

The program is working with several MQTT topics. All topics are prefixed with `mac2mqtt` + `COMPUTER_NAME`.
For example, the topic with the current volume on my machine is `mac2mqtt/bessarabov-osx/status/volume`

`mac2mqtt` send info to the topics `mac2mqtt/COMPUTER_NAME/status/#` and listen for commands in topics
`mac2mqtt/COMPUTER_NAME/command/#`.

### PREFIX + `/status/alive`

There can be `true` or `false` in this topic. If `mac2mqtt` is connected to MQTT server there is `true`.
If `mac2mqtt` is disconnected from MQTT there is `false`. This is the standard MQTT thing called Last Will and Testament.

### PREFIX + `/status/volume`

The value ranges from 0 (inclusive) to 100 (inclusive)—the current volume of the computer.

The value of this topic is updated every 60 seconds.

### PREFIX + `/status/mute`

There can be `true` or `false` in this topic. `true` means that the computer volume is muted (no sound),
`false` means that it is not muted.

### PREFIX + `/status/battery`

The value ranges from 0 (inclusive) to 100 (inclusive) and represents the current level of the battery. Returns empty if there is no battery.

The value of this topic is updated every 60 seconds.

### PREFIX + `/status/media_player`

Contains JSON with current media player information. Only available if Media Control is installed.

Example:
```json
{
  "state": "playing",
  "title": "Song Title",
  "artist": "Artist Name",
  "album": "Album Name",
  "app_name": "Spotify",
  "duration": 180,
  "position": 45,
  "media_title": "Song Title",
  "media_artist": "Artist Name",
  "media_album": "Album Name"
}
```

States: `playing`, `paused`, `idle`

### PREFIX + `/status/media_state`

The current state of media playback: `playing`, `paused`, or `idle`.

### PREFIX + `/status/media_title`

The title of the currently playing media.

### PREFIX + `/status/media_artist`

The artist of the currently playing media.

### PREFIX + `/status/media_album`

The album of the currently playing media.

### PREFIX + `/status/media_app`

The name of the application playing media (e.g., "Spotify", "Apple Music").

### PREFIX + `/status/media_duration`

The total duration of the media in seconds.

### PREFIX + `/status/media_position`

The current position in the media in seconds.

### PREFIX + `/status/user_activity`

The current user activity state: `active` or `inactive`.

This sensor monitors system idle time and provides instant updates when user interaction is detected (mouse movement, keyboard input, etc.). The state changes to `active` immediately upon any user interaction and automatically switches to `inactive` after 10 seconds of no activity.

**Features:**
- **Instant detection**: No polling delays - activity is detected immediately
- **Automatic timeout**: Switches to inactive after exactly 10 seconds of inactivity  
- **System-level monitoring**: Uses macOS IOHIDSystem to track all user input
- **Event-driven**: Updates are published only when state changes occur
- **Home Assistant integration**: Appears as an occupancy sensor with device class `occupancy`

This is perfect for automation scenarios like:
- Turning off lights when user is away from computer
- Pausing media when user steps away
- Triggering screensaver or sleep modes
- Presence detection for home automation

The current position in the media in seconds.

### PREFIX + `/command/volume`

You can send integer numbers from 0 (inclusive) to 100 (inclusive) to this topic. It will set the volume on the computer.

### PREFIX + `/command/mute`

You can send `true` or `false` to this topic. When you send `true` the computer is muted. When you send `false` the computer
is unmuted.

### PREFIX + `/command/runshortcut`

You can send the name of a shortcut to this topic. It will run this shortcut in the Shortcuts app.

### PREFIX + `/command/set`

You can send `screensaver` to this topic. It will turn start your screensaver. Sending some other value will do nothing.

You can send `displaywake` to this topic. It will turn on the display. Sending some other value will do nothing.

You can send  `sleep` to this topic, and it will put the computer to sleep. Sending other values will do nothing.

You can send `shutdown` to this topic. It will try to shut down the computer. The way it is done depends on the user who ran the program. If the program is run by `root` the computer will shut down, but if it is run by an ordinary user the computer will not shut down if there is another user who logged in. Sending some other value but `shutdown` will do nothing.

You can send `displaysleep` to this topic. It will turn off the display. Sending some other value will do nothing.


## Management Scripts

After installation, you can use these helpful scripts to manage Mac2MQTT:

### Using Make (Recommended)
```bash
make status       # Check service status
make logs         # Show recent logs
make configure    # Reconfigure settings
make test         # Test MQTT connection
make uninstall    # Uninstall mac2mqtt
```

### Using Scripts Directly
```bash
./status.sh       # Check service status
./status.sh --logs      # Show recent logs
./status.sh --follow    # Follow logs in real-time
./configure.sh    # Reconfigure settings
./uninstall.sh    # Uninstall mac2mqtt
```

### Check Status
Shows if the service is running, configuration details, and dependency status.

### View Logs
View recent logs or follow them in real-time.

### Reconfigure Settings
Allows you to change MQTT settings without reinstalling.

### Uninstall
Completely removes Mac2MQTT from your system.

## Building

To build this program yourself, follow these steps:

1. Clone this repo
2. Make sure you have installed go, for example with `brew install go`
3. Install its dependencies with `go install`
4. Build with `go build mac2mqtt.go`

It outputs a file `mac2mqtt`. Make the binary executable (`chmod +x mac2mqtt`) and run `./mac2mqtt`.
