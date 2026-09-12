# hindsight-forwarder

Consumes `whatsapp-bridge`'s event stream and retains every message into a
Hindsight memory bank. Purely mechanical: no LLM, no polling. The cursor
(last event id handed to Hindsight) is a file in `FORWARDER_STATE_DIR`, so a
restart or a Hindsight outage replays rather than loses.

Voice notes arrive already transcribed (the bridge publishes them only once
the transcript is in), so each note becomes one memory.

Configuration is via environment variables; see the module docstring in
`src/hindsight_forwarder/main.py`. In the nix deployment the secret-bearing
ones come from `/data/hindsight.env`.
