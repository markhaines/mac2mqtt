# Legacy on-wire fixtures

`legacy-onwire-b5a986a-*.json` record what main (`b5a986a`) actually put on the
wire, captured by running **that commit's own `connectHandler`** under a
recording MQTT client. They are the baseline the compatibility tests compare
against, so that a default-path behaviour change fails the build instead of
moving both sides of the comparison together.

There are two variants because `isMediaControlAvailable()` probes `PATH` with
`exec.LookPath`, and its result changes which components main publishes
(`playpause` and `now_playing`). Capturing both means the comparison runs on a
machine with media-control and on one without, rather than being skipped.

## Regenerating

`legacy-capture-generator.go.txt` is the generator. It is kept as `.txt` so the
Go tool does not build it, and `testdata/` is ignored by the toolchain anyway.

```sh
git archive b5a986a | tar -x -C /tmp/legacy
cp testdata/legacy-capture-generator.go.txt /tmp/legacy/legacy_capture_test.go
cd /tmp/legacy

# with media-control present
MAC2MQTT_FIXTURE_VARIANT=with-media-control go test -run TestGenerateLegacyFixture -v

# with media-control hidden from PATH
env PATH=/usr/bin:/bin:/usr/sbin:/sbin MAC2MQTT_FIXTURE_VARIANT=no-media-control \
  "$(command -v go)" test -run TestGenerateLegacyFixture -v
```

The generator refuses to run if the environment does not match the requested
variant, so a fixture cannot be captured under the wrong conditions.

## Normalisation

Only the two genuinely per-host values are replaced, with `<SERIAL>` and
`<MODEL>`:

- `dev.ids` — `getSerialnumber()`
- `dev.mdl` — `getModel()`

Everything else is compared verbatim: component keys, `state_topic`,
`command_topic`, payload mappings, `unit_of_measurement`, `device_class`,
`state_class`, `icon`, `enabled_by_default`, the discovery-level `qos`, and the
availability wiring.
