package tests

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestDeterministicStaleAudioBarrier(t *testing.T) {
	session := pipeline.NewVoiceSession("sess_deterministic_barrier")
	defer session.Close()

	// 1. Advance to SPEAKING
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

	// 2. Start streaming sentence with synthesis delay
	tokenCh := make(chan string, 10)
	session.StartLLMResponseStream(tokenCh)

	tokenCh <- "First deterministic sentence."
	close(tokenCh)

	// 3. Immediately trigger barge-in while synthesis is in-flight
	time.Sleep(5 * time.Millisecond)
	session.TriggerBargeIn()

	if session.State() != model.StateListening {
		t.Fatalf("expected state LISTENING after barge-in, got %s", session.State())
	}

	// 4. Wait for any delayed synthesis routines to complete
	time.Sleep(60 * time.Millisecond)

	// Assert no stale chunk from the interrupted turn leaked into audioOut
	select {
	case chunk := <-session.AudioOut():
		t.Fatalf("stale audio chunk detected in output queue: turn=%s, idx=%d", chunk.TurnID, chunk.ChunkIdx)
	default:
		// Queue is strictly clean
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

func TestWebSocketFullDuplexLiveAudio(t *testing.T) {
	mgr := pipeline.NewSessionManager(5 * time.Minute)
	defer mgr.Stop()

	handler := server.NewServerHandler(mgr)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws/voice/sess_live_audio_test"

	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed connecting websocket: %v", err)
	}
	defer conn.Close()

	// 1. Ingest raw linear PCM audio frame (binary message)
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

	// 3. Receive emitted TurnEvent from server
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	readDone := make(chan bool)
	go func() {
		for {
			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if msgType == websocket.TextMessage && len(data) > 0 {
				readDone <- true
				return
			}
		}
	}()

	select {
	case <-readDone:
		// Event received successfully over full-duplex socket
	case <-ctx.Done():
		t.Fatalf("timed out waiting for event on websocket connection")
	}

	// 4. Send playback finished
	_ = conn.WriteJSON(map[string]string{"type": "playback_finished"})
}
