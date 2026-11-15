# VocalFlow: Real-Time Voice AI Turn & Barge-In Orchestration Engine

[![Go Reference](https://pkg.go.dev/badge/github.com/yoohooshantanu/vocalflow.svg)](https://pkg.go.dev/github.com/yoohooshantanu/vocalflow)
[![Go Report Card](https://goreportcard.com/badge/github.com/yoohooshantanu/vocalflow)](https://goreportcard.com/report/github.com/yoohooshantanu/vocalflow)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**VocalFlow** is a low-latency, full-duplex conversational turn orchestration engine written in Go. Designed for mission-critical AI voice agents (telephony and WebRTC), VocalFlow solves the two hardest real-time voice challenges: **sub-250ms Time-to-First-Audio (TTFA)** and **sub-millisecond atomic barge-in interruption cancellation** with **turn-generation race elimination**, **binary PCM audio streaming**, and **in-memory session reconnect recovery**.

---

## Architecture Overview

```
                                      VOCALFLOW ENGINE PIPELINE
                                      
  Telephony In ────► [ Binary PCM / RMS ] ────► [ Turn FSM ] 
   (Audio Frames)     (VAD + Hangover)           │ (Strict Thread-Safe States)
                                                 ▼
                                       [ State: THINKING ]
                                                 │
                                                 ▼
                                   [ Streaming Token Ingestion ]
                                                 │
                                                 ▼
                                 [ Clause-Boundary Chunker ]
                                       (First-Chunk Opt)
                                          /             \
                                  Chunk 1                Chunk 2...
                                (3-5 words)              (Sentences)
                                     │                       │
                                     ▼                       ▼
                             [ Streaming TTS ]       [ Streaming TTS ]
                                     │                       │
                                     └───────────┬───────────┘
                                                 ▼
                                        [ State: SPEAKING ]
                                                 │
                                                 ▼
  Telephony Out ◄────────────────── [ Audio Out Bounded Ring Buffer ]
                                                 ▲
                                                 │ [ Interruption Detected ]
  [ User Barge-In ] ─────────────────────────────┴─► [ Atomic Cancellation ]
   (Caller Speaks)                                     1. Bump turn generation (drop stale audio)
                                                       2. Cancel in-flight turn context
                                                       3. Drain unplayed audio queue
                                                       4. Signal client speaker flush
                                                       5. Transition to LISTENING (<1ms)
```

---

## Key Technical Features

1. **Full-Duplex Binary PCM Audio Streaming**:
   - WebSocket interface accepts raw 16-bit linear PCM audio frames (16kHz mono) over binary messages.
   - Calculates Root Mean Square (RMS) energy in real-time ($RMS = \sqrt{\frac{1}{N}\sum x_i^2}$) with zero disk allocations.
   - Streams synthesized binary PCM audio chunks directly to client audio players.
2. **Turn Generation Race Protection**:
   - Atomic turn generation counters (`turnGen`) invalidate in-flight synthesis tasks when barge-in occurs.
   - Guarantees that stale audio chunks from an interrupted turn cannot re-enter the output queue post-cancellation. Verified using deterministic Go channel synchronization barriers (`TestDeterministicStaleAudioBarrier`).
3. **Fail-Closed Security & Origin Validation**:
   - RFC 6454 `CheckOrigin` protects against Cross-Site WebSocket Hijacking (CSWSH).
   - Constant-time comparison (`subtle.ConstantTimeCompare`) on HTTP `Authorization: Bearer <token>` authenticates client identity.
   - Query string tokens (`?token=...`) are explicitly rejected to prevent token leakage in proxy logs and browser histories.
   - In production (`REQUIRE_AUTH=true`), unset token configuration fails closed (`TestWebSocketFailClosedSecurity`).
4. **Streaming Clause-Boundary Sentence Chunker**:
   - Rather than waiting for complete LLM responses (which incurs 2-3s delays), VocalFlow chunks streaming tokens at clause boundaries (`,`, `;`, `:`) after 3–5 words for the first chunk, kicking off neural TTS synthesis in parallel.
   - Handles honorific abbreviations (`Dr.`, `Mr.`, `Ms.`, `Prof.`, `vs.`, `etc.`), currency amounts (`$10,000`), and ellipses (`...`).
5. **Sub-Millisecond In-Process Interruption Scheduling**:
   - When a caller speaks while the AI is thinking or speaking, VocalFlow cancels the active `context.Context`, dumps pending audio playback packets, and emits a `BARGE_IN_TRIGGERED` control event to immediately clear client speaker buffers.
6. **Turn-Taking Finite State Machine (FSM)**:
   - Governs conversational states: `IDLE` $\rightarrow$ `LISTENING` $\rightarrow$ `THINKING` $\rightarrow$ `SPEAKING` $\rightarrow$ `DRAINING` $\rightarrow$ `INTERRUPTED`.
   - Thread-safe state transition validation protects against race conditions during rapid turn-taking.
7. **Session Reconnect & Recovery Grace Period**:
   - Telephony client disconnects mark the session detached with a 60-second recovery grace window (`ShouldExpire`).
   - Reconnecting with the same `session_id` reattaches the audio queue without losing dialogue history.
8. **Real-Time Telemetry & Prometheus Sidecar**:
   - Prometheus metrics tracking Time-to-First-Audio (TTFA), Time-to-First-Token (TTFT), sub-millisecond barge-in reaction latency (`vocalflow_avg_barge_in_reaction_ms`), active sessions, and sentences synthesized.

---

## Audio Synthesis & Streaming TTS Architecture

VocalFlow decouples turn orchestration from speech synthesis providers via the `StreamingTTSProvider` interface:

1. **Cartesia Sonic WebSocket Streaming Adapter (`CartesiaStreamingTTS`)**:
   - Production WebSocket streaming client connecting to `wss://api.cartesia.ai/tts/websocket?cartesia_version=2024-06-10`.
   - Sends structured chunk request frames (`model_id`, `transcript`, `voice.id`, `output_format.container="raw", encoding="pcm_s16le", sample_rate=16000`).
   - Streams incoming binary linear PCM frames and JSON base64 frames until completion (`type: "done"`).
   - In production with `CARTESIA_API_KEY` configured, connection and API errors explicitly surface structured failures rather than masking errors.
2. **Acoustic Test Harness Waveform Synthesizer (`FormantTestHarnessTTS`)**:
   - Generates a synthetic 16kHz 16-bit linear PCM acoustic test waveform ($F_0=150\text{Hz}, F_1=500\text{Hz}, F_2=1500\text{Hz}$) with measurable RMS acoustic energy.
   - Provides deterministic, offline verification of audio buffering, VAD, and barge-in scheduling without cloud API keys, network jitter, or third-party usage costs.
   - Automatically used when `CARTESIA_API_KEY` is unconfigured.

### Environment Configuration

| Variable | Default | Description |
| :--- | :--- | :--- |
| `TTS_PROVIDER` | `formant` | Active TTS provider (`cartesia`, `formant`) |
| `CARTESIA_API_KEY` | `""` | Cartesia Sonic API key (falls back to test harness if empty) |
| `CARTESIA_MODEL_ID` | `sonic-english` | Cartesia voice model ID |
| `CARTESIA_VOICE_ID` | `a0e998e3-182d-4d96-8d1a-0e624b66ffc2` | Cartesia voice identifier |
| `REQUIRE_AUTH` | `false` | When `true`, enforces fail-closed Bearer token authentication |
| `AUTH_TOKEN` | `""` | Expected secret Bearer token for HTTP & WebSocket clients |

---

## API & WebSocket Specification

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/v1/sessions` | Initializes a new conversational voice session |
| `WS` | `/ws/voice/{session_id}` | Full-duplex binary & text WebSocket stream for audio and turn control |
| `GET` | `/healthz` | Liveness & readiness probe |
| `GET` | `/metrics` | Prometheus-compatible metrics exposition |
| `GET` | `/debug/pprof/*` | Go runtime execution & memory profiling |

---

## Verification & Testing

Run all 17 unit, integration, and concurrency tests:

```bash
$env:CGO_ENABLED="0"; $env:GOPROXY="off"; $env:GOTOOLCHAIN="local"
go test -v -mod=readonly ./tests/...
```

### Verified Benchmark Performance

Benchmarks executed on AMD Ryzen 5 (Offline Go 1.27.0):

```text
BenchmarkRMSEnergyCalculation-8            1,919,305 ops/sec    665.7 ns/op        0 B/op    0 allocs/op
BenchmarkSentenceChunker-8                   229,180 ops/sec    6,768 ns/op      672 B/op   17 allocs/op
BenchmarkBargeInCancellationLatency-8         10,000 ops/sec  188,442 ns/op   16,817 B/op   63 allocs/op
BenchmarkEndToEndBargeInPipeline-8             2,323 ops/sec  431,558 ns/op   16,892 B/op   68 allocs/op
```

> **Latency Budget & SLA Context:**
> - **In-Process Cancellation Benchmark:** **188.4 microseconds** (0.188 ms) — *Measures context cancellation, atomic turn-generation invalidation, queue draining of 5 buffered audio packets, and FSM transition from `SPEAKING` $\rightarrow$ `INTERRUPTED` $\rightarrow$ `LISTENING`.*
> - **Full End-to-End Interruption Pipeline:** **431.5 microseconds** (0.431 ms) — *Measures the complete internal chain from incoming raw 16kHz PCM audio bytes $\rightarrow$ RMS calculation $\rightarrow$ VAD voiced threshold detection $\rightarrow$ Barge-in callback $\rightarrow$ Context cancellation $\rightarrow$ Audio buffer drain $\rightarrow$ FSM transition $\rightarrow$ WebSocket event emission.*
> - **Telephony SLA Budget:** In telephony voice agents, human conversational perception expects barge-in interruption within **40 milliseconds**. VocalFlow's internal engine scheduling, cancellation, and buffer drain completes in **0.43 ms (431 µs)**. This sub-millisecond in-process execution leaves over **39.5 ms of latency headroom** for network transit, PSTN/SIP carrier packetization, and client audio player buffer flushes.

---

## Production Deployment & Docker

Deploy VocalFlow and Prometheus sidecar with Docker Compose:

```bash
docker-compose up -d
```

- **VocalFlow Gateway:** `http://localhost:8085` (WS: `ws://localhost:8085/ws/voice/{session_id}`)
- **Prometheus Metrics:** `http://localhost:8085/metrics`
- **Prometheus UI:** `http://localhost:9090`
- **pprof Profiling:** `http://localhost:8085/debug/pprof/`

Compile binary:

```bash
go build -o bin/vocalflow.exe ./cmd/server
```

---

## Author

- **Shantanu Juvekar** ([@yoohooshantanu](https://github.com/yoohooshantanu))
- Email: `santanujuvekar@gmail.com`
- License: MIT
