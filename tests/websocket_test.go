package tests

import (
	"context"
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

	// 3. Connection with URL query token -> Accepted (101)
	wsURLQuery := wsURL + "?token=vocalflow_secret_token_99"
	connQuery, respQuery, err := websocket.DefaultDialer.Dial(wsURLQuery, nil)
	if err != nil {
		t.Fatalf("expected valid query token to connect, got: %v", err)
	}
	if respQuery.StatusCode != http.StatusSwitchingProtocols {
		t.Errorf("expected 101, got %d", respQuery.StatusCode)
	}
	_ = connQuery.Close()
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
