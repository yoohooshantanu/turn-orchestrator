package tests

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yoohooshantanu/vocalflow/internal/model"
	"github.com/yoohooshantanu/vocalflow/internal/pipeline"
	"github.com/yoohooshantanu/vocalflow/internal/server"
	"github.com/yoohooshantanu/vocalflow/internal/vad"
)

func TestFormantWaveformSynthesis(t *testing.T) {
	pcm := pipeline.GenerateFormantAudioPCM(0)
	if len(pcm) != 640 {
		t.Fatalf("expected 640 bytes for 20ms of audio, got %d", len(pcm))
	}

	// Verify acoustic energy
	energy := vad.ComputeRMSEnergy(pcm)
	if energy < 0.01 {
		t.Fatalf("expected audible non-zero RMS energy from formant speech waveform, got %f", energy)
	}
}

// TestDeterministicStaleAudioBarrier forces an exact interleaved race window using channel synchronization barriers:
// 1. Worker enters synthesis and generates audio for Turn 1.
// 2. Barrier pauses worker right before guarded enqueue.
// 3. Main thread triggers Barge-In (incrementing turnGen and canceling turnCtx).
// 4. Barrier unblocks worker.
// 5. Worker attempts guarded enqueue under mutex: must be rejected due to mismatched generation and canceled context.
func TestDeterministicStaleAudioBarrier(t *testing.T) {
	session := pipeline.NewVoiceSession("sess_deterministic_barrier")
	defer session.Close()

	// Advance to SPEAKING
	session.StartSpeakingTurnForBench()
	if session.State() != model.StateSpeaking {
		t.Fatalf("expected state SPEAKING, got %s", session.State())
	}

	// Drain any initial benchmark chunks
DrainBench:
	for {
		select {
		case <-session.AudioOut():
		default:
			break DrainBench
		}
	}

	workerInsideSynthesis := make(chan struct{})
	allowWorkerEnqueue := make(chan struct{})

	// Register barrier hook to deterministically pause worker right before guarded queue admission
	session.SetSynthesisBarrierHook(func(turnGen uint64) {
		close(workerInsideSynthesis) // Step A: Signal main test that worker is actively synthesizing
		<-allowWorkerEnqueue         // Step B: Hold worker paused until barge-in has executed
	})

	tokenCh := make(chan string, 1)
	session.StartLLMResponseStream(tokenCh)
	tokenCh <- "Deterministic sentence boundary."
	close(tokenCh)

	// Step 1: Wait until worker is actively inside synthesis and paused at the barrier
	<-workerInsideSynthesis

	// Step 2: Trigger barge-in while worker is blocked with chunk in hand
	session.TriggerBargeIn()

	if session.State() != model.StateListening {
		t.Fatalf("expected state LISTENING after barge-in, got %s", session.State())
	}

	// Step 3: Release worker to attempt enqueueing into audioOut
	close(allowWorkerEnqueue)

	// Step 4: Worker unblocks and executes guarded enqueue under mutex.
	// Because turnGen was incremented and context canceled, chunk MUST be rejected!
	select {
	case chunk := <-session.AudioOut():
		t.Fatalf("stale audio chunk detected in output queue: turn=%s, idx=%d", chunk.TurnID, chunk.ChunkIdx)
	case <-time.After(50 * time.Millisecond):
		// Clean! Zero stale chunks admitted to audio queue
	}
}

func TestWebSocketOriginSecurityValidation(t *testing.T) {
	mgr := pipeline.NewSessionManager(5 * time.Minute)
	defer mgr.Stop()

	handler := server.NewServerHandler(mgr)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws/voice/sess_security_test"

	// Case 1: Disallowed foreign origin -> MUST be rejected with 403 Forbidden
	reqHeaderDisallowed := http.Header{}
	reqHeaderDisallowed.Set("Origin", "https://malicious-attacker.com")
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, reqHeaderDisallowed)
	if err == nil {
		t.Fatalf("expected connection to fail with 403 for disallowed cross-origin")
	}
	if resp != nil && resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected status 403 Forbidden, got %d", resp.StatusCode)
	}

	// Case 2: Direct telephony client with no Origin header -> Accepted
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("expected direct client with no Origin header to connect successfully, got: %v", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Errorf("expected 101 Switching Protocols, got %d", resp.StatusCode)
	}
	_ = conn.Close()

	// Case 3: Same-Origin / Localhost request -> Accepted
	serverURL, _ := url.Parse(ts.URL)
	reqHeaderAllowed := http.Header{}
	reqHeaderAllowed.Set("Origin", "http://"+serverURL.Host)
	conn2, resp2, err := websocket.DefaultDialer.Dial(wsURL, reqHeaderAllowed)
	if err != nil {
		t.Fatalf("expected same-origin request to connect successfully, got: %v", err)
	}
	if resp2.StatusCode != http.StatusSwitchingProtocols {
		t.Errorf("expected 101 Switching Protocols, got %d", resp2.StatusCode)
	}
	_ = conn2.Close()
}

func TestWebSocketAuthentication(t *testing.T) {
	os.Setenv("AUTH_TOKEN", "vocalflow_secret_token_99")
	defer os.Unsetenv("AUTH_TOKEN")

	mgr := pipeline.NewSessionManager(5 * time.Minute)
	defer mgr.Stop()

	handler := server.NewServerHandler(mgr)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws/voice/sess_auth_test"

	// 1. Connection without token -> 401 Unauthorized
	_, respUnauth, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatalf("expected connection without auth to be rejected")
	}
	if respUnauth != nil && respUnauth.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", respUnauth.StatusCode)
	}

	// 2. Connection with Bearer token header -> Accepted (101)
	headerAuth := http.Header{}
	headerAuth.Set("Authorization", "Bearer vocalflow_secret_token_99")
	connBearer, respBearer, err := websocket.DefaultDialer.Dial(wsURL, headerAuth)
	if err != nil {
		t.Fatalf("expected valid Bearer token to connect, got: %v", err)
	}
	if respBearer.StatusCode != http.StatusSwitchingProtocols {
		t.Errorf("expected 101, got %d", respBearer.StatusCode)
	}
	_ = connBearer.Close()

	// 3. Query string token (?token=...) MUST BE REJECTED (prevent credential leaks into logs)
	wsURLQuery := wsURL + "?token=vocalflow_secret_token_99"
	_, respQuery, err := websocket.DefaultDialer.Dial(wsURLQuery, nil)
	if err == nil {
		t.Fatalf("query-string token must be rejected with 401")
	}
	if respQuery != nil && respQuery.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized for query-string token, got %d", respQuery.StatusCode)
	}
}

func TestWebSocketFailClosedSecurity(t *testing.T) {
	// In production or when REQUIRE_AUTH is true, missing AUTH_TOKEN configuration MUST FAIL CLOSED
	os.Setenv("REQUIRE_AUTH", "true")
	os.Unsetenv("AUTH_TOKEN")
	defer os.Unsetenv("REQUIRE_AUTH")

	mgr := pipeline.NewSessionManager(5 * time.Minute)
	defer mgr.Stop()

	handler := server.NewServerHandler(mgr)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws/voice/sess_fail_closed"

	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatalf("expected fail-closed mode to reject connections when AUTH_TOKEN is unset")
	}
	if resp != nil && resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized in fail-closed mode, got %d", resp.StatusCode)
	}
}

func TestWebSocketFullDuplexLiveAudio(t *testing.T) {
	mgr := pipeline.NewSessionManager(5 * time.Minute)
	defer mgr.Stop()

	handler := server.NewServerHandler(mgr)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	sessionID := "sess_full_duplex_e2e"
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws/voice/" + sessionID

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed connecting websocket: %v", err)
	}
	defer conn.Close()

	session := mgr.GetOrCreate(sessionID)

	// 1. Ingest raw linear PCM audio frame from client (inbound audio)
	pcmAudio := pipeline.GenerateFormantAudioPCM(0)
	if err := conn.WriteMessage(websocket.BinaryMessage, pcmAudio); err != nil {
		t.Fatalf("failed sending binary PCM audio: %v", err)
	}

	// 2. Ingest JSON control frame to simulate voiced speech
	controlMsg := map[string]interface{}{
		"type":      "audio_frame",
		"energy":    0.08,
		"is_voiced": true,
	}
	if err := conn.WriteJSON(controlMsg); err != nil {
		t.Fatalf("failed sending JSON control frame: %v", err)
	}

	// 3. Trigger speaking turn with outbound PCM audio chunks
	session.StartSpeakingTurnForBench()

	// 4. Assert client receives BOTH text control event AND outbound binary PCM audio chunk!
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	receivedTextMessage := false
	receivedBinaryPCM := false
	var receivedBinaryLen int

	readDone := make(chan bool)
	go func() {
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if msgType == websocket.TextMessage {
				receivedTextMessage = true
			}
			if msgType == websocket.BinaryMessage {
				receivedBinaryPCM = true
				receivedBinaryLen = len(data)
			}
			if receivedTextMessage && receivedBinaryPCM {
				readDone <- true
				return
			}
		}
	}()

	select {
	case <-readDone:
		// Succeeded in receiving both text and outbound binary audio!
	case <-ctx.Done():
		t.Fatalf("timeout waiting for full-duplex exchange: text=%v, binary=%v", receivedTextMessage, receivedBinaryPCM)
	}

	if !receivedBinaryPCM || receivedBinaryLen != 640 {
		t.Errorf("expected 640-byte outbound binary PCM audio frame, got len=%d (received=%v)", receivedBinaryLen, receivedBinaryLen)
	}

	// 5. Send playback finished
	_ = conn.WriteJSON(map[string]string{"type": "playback_finished"})
}

func TestCartesiaProviderLifecycleAndFallback(t *testing.T) {
	// 1. Fallback when API key is unconfigured
	fallbackProvider := pipeline.NewCartesiaStreamingTTS("", "sonic-english", "voice-default")
	pcm, err := fallbackProvider.SynthesizeChunk(context.Background(), "Hello Cartesia", 0)
	if err != nil {
		t.Fatalf("expected fallback synthesis to succeed without error, got %v", err)
	}
	if len(pcm) != 640 {
		t.Errorf("expected 640 bytes of fallback PCM audio, got %d", len(pcm))
	}

	// 2. Cancellation handling
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = fallbackProvider.SynthesizeChunk(canceledCtx, "Should cancel", 1)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got %v", err)
	}

	// 3. Provider closure lifecycle
	if err := fallbackProvider.Close(); err != nil {
		t.Errorf("expected clean provider close, got %v", err)
	}
	_, err = fallbackProvider.SynthesizeChunk(context.Background(), "After close", 2)
	if err == nil {
		t.Errorf("expected error when synthesizing with closed provider")
	}

	// 4. Live streaming with mock Cartesia WebSocket server streaming multiple chunks followed by done frame
	upgrader := websocket.Upgrader{}
	chunk1 := pipeline.GenerateFormantAudioPCM(100)
	chunk2 := pipeline.GenerateFormantAudioPCM(500)
	expectedTotalLen := len(chunk1) + len(chunk2)

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify Cartesia auth headers
		if r.Header.Get("X-API-Key") != "test-cartesia-key" || r.Header.Get("Cartesia-Version") != "2024-06-10" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()

		// Read request frame
		var req map[string]interface{}
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send stream chunk 1
		b64Chunk1 := base64.StdEncoding.EncodeToString(chunk1)
		_ = conn.WriteJSON(map[string]interface{}{
			"type": "chunk",
			"data": b64Chunk1,
			"done": false,
		})

		// Send stream chunk 2
		b64Chunk2 := base64.StdEncoding.EncodeToString(chunk2)
		_ = conn.WriteJSON(map[string]interface{}{
			"type": "chunk",
			"data": b64Chunk2,
			"done": false,
		})

		// Send completion done frame
		_ = conn.WriteJSON(map[string]interface{}{
			"type": "done",
			"done": true,
		})
	}))
	defer mockServer.Close()

	liveCartesia := pipeline.NewCartesiaStreamingTTS("test-cartesia-key", "sonic-english", "test-voice")
	liveCartesia.SetEndpoint(mockServer.URL)

	audio, err := liveCartesia.SynthesizeChunk(context.Background(), "Live Cartesia stream", 0)
	if err != nil {
		t.Fatalf("expected successful multi-frame synthesis from mock Cartesia, got %v", err)
	}
	if len(audio) != expectedTotalLen {
		t.Errorf("expected %d total accumulated bytes from all Cartesia chunks, got %d", expectedTotalLen, len(audio))
	}

	// 5. Explicit error propagation when real API key is configured but connection fails
	failingCartesia := pipeline.NewCartesiaStreamingTTS("bad-key", "sonic-english", "test-voice")
	failingCartesia.SetEndpoint("ws://127.0.0.1:59999/nonexistent")
	_, err = failingCartesia.SynthesizeChunk(context.Background(), "Should fail", 0)
	if err == nil {
		t.Errorf("expected explicit error when Cartesia connection fails with configured API key")
	}
}

func TestTTSProviderEnvironmentConfiguration(t *testing.T) {
	// Test default fallback
	os.Setenv("TTS_PROVIDER", "")
	pDefault := pipeline.NewTTSProviderFromEnv()
	if _, ok := pDefault.(*pipeline.FormantTestHarnessTTS); !ok {
		t.Errorf("expected FormantTestHarnessTTS as default")
	}

	// Test Cartesia config
	os.Setenv("TTS_PROVIDER", "cartesia")
	os.Setenv("CARTESIA_API_KEY", "cartesia-key-123")
	pCartesia := pipeline.NewTTSProviderFromEnv()
	if _, ok := pCartesia.(*pipeline.CartesiaStreamingTTS); !ok {
		t.Errorf("expected CartesiaStreamingTTS when TTS_PROVIDER=cartesia")
	}

	os.Unsetenv("TTS_PROVIDER")
	os.Unsetenv("CARTESIA_API_KEY")
}
