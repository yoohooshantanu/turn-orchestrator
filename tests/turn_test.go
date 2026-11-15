package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/yoohooshantanu/vocalflow/internal/chunker"
	"github.com/yoohooshantanu/vocalflow/internal/fsm"
	"github.com/yoohooshantanu/vocalflow/internal/model"
	"github.com/yoohooshantanu/vocalflow/internal/pipeline"
	"github.com/yoohooshantanu/vocalflow/internal/vad"
)

func TestFSMStateTransitions(t *testing.T) {
	stateMachine := fsm.NewTurnFSM("sess_test_fsm")

	if stateMachine.Current() != model.StateIdle {
		t.Fatalf("expected initial state IDLE, got %s", stateMachine.Current())
	}

	// Valid sequence: IDLE -> LISTENING -> THINKING -> SPEAKING -> DRAINING -> LISTENING
	transitions := []model.ConversationState{
		model.StateListening,
		model.StateThinking,
		model.StateSpeaking,
		model.StateDraining,
		model.StateListening,
	}

	for _, state := range transitions {
		if err := stateMachine.Transition(state); err != nil {
			t.Fatalf("unexpected error transitioning to %s: %v", state, err)
		}
		if stateMachine.Current() != state {
			t.Fatalf("expected state %s, got %s", state, stateMachine.Current())
		}
	}

	// Invalid transition: LISTENING -> SPEAKING directly
	err := stateMachine.Transition(model.StateSpeaking)
	if err == nil {
		t.Fatalf("expected error for invalid transition LISTENING -> SPEAKING, got nil")
	}
}

func TestVADSilenceDetection(t *testing.T) {
	stateMachine := fsm.NewTurnFSM("sess_test_vad")
	_ = stateMachine.Transition(model.StateListening)

	speechStartFired := false
	speechEndFired := false
	var speechEndDuration time.Duration

	cb := vad.VADCallbacks{
		OnSpeechStart: func(t time.Time) {
			speechStartFired = true
		},
		OnSpeechEnd: func(t time.Time, duration time.Duration) {
			speechEndFired = true
			speechEndDuration = duration
		},
	}

	cfg := vad.VADConfig{
		EnergyThreshold:  0.02,
		SilenceHangover:  50 * time.Millisecond,
		BargeInMinFrames: 2,
	}

	v := vad.NewVoiceActivityDetector("sess_test_vad", cfg, stateMachine, cb)

	// Feed 3 voiced frames
	for i := 0; i < 3; i++ {
		v.ProcessFrame(&model.AudioFrame{
			Energy:   0.05,
			IsVoiced: true,
		})
	}

	if !speechStartFired {
		t.Fatalf("expected speech start callback to fire")
	}

	// Feed unvoiced silence frames until hangover duration expires
	time.Sleep(60 * time.Millisecond)
	v.ProcessFrame(&model.AudioFrame{
		Energy:   0.001,
		IsVoiced: false,
	})

	if !speechEndFired {
		t.Fatalf("expected speech end callback to fire after silence hangover")
	}

	if speechEndDuration < 0 {
		t.Fatalf("invalid speech duration: %v", speechEndDuration)
	}
}

func TestSentenceChunkerFirstChunkOptimization(t *testing.T) {
	var chunks []*model.SentenceChunk
	var mu sync.Mutex

	c := chunker.NewSentenceChunker("sess_chunk", "turn_1", func(chunk *model.SentenceChunk) {
		mu.Lock()
		defer mu.Unlock()
		chunks = append(chunks, chunk)
	})

	tokens := []string{
		"Sure", ", ", "I", " can", " definitely", " help", " you", " with", " that", ". ",
		"Could", " you", " please", " provide", " your", " account", " number", "?",
	}

	for _, token := range tokens {
		c.IngestToken(token)
	}
	c.Flush()

	mu.Lock()
	defer mu.Unlock()

	if len(chunks) < 2 {
		t.Fatalf("expected at least 2 sentence chunks, got %d", len(chunks))
	}

	// First chunk must have is_first_chunk = true
	if !chunks[0].IsFirstChunk {
		t.Errorf("expected first chunk to have IsFirstChunk = true")
	}

	// Verify first chunk text
	if chunks[0].Text == "" {
		t.Errorf("expected non-empty text in first chunk")
	}

	// Verify final chunk
	if !chunks[len(chunks)-1].IsFinal {
		t.Errorf("expected last chunk to have IsFinal = true")
	}
}

func TestSub40msBargeInCancellation(t *testing.T) {
	session := pipeline.NewVoiceSession("sess_bargein_test")
	defer session.Close()

	// Feed audio frames to simulate user turn completion
	for i := 0; i < 3; i++ {
		_ = session.IngestAudio(&model.AudioFrame{Energy: 0.05, IsVoiced: true})
	}
	time.Sleep(360 * time.Millisecond) // Trigger silence hangover
	_ = session.IngestAudio(&model.AudioFrame{Energy: 0.001, IsVoiced: false})

	if session.State() != model.StateThinking {
		t.Fatalf("expected state THINKING, got %s", session.State())
	}

	// Stream LLM tokens to trigger speech synthesis
	tokenCh := make(chan string, 10)
	tokenCh <- "Hello, "
	tokenCh <- "how are you today? "
	close(tokenCh)

	session.StartLLMResponseStream(tokenCh)

	// Wait for synthesis to transition into SPEAKING state
	select {
	case <-session.AudioOut():
		// Received audio chunk
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for first audio chunk")
	}

	if session.State() != model.StateSpeaking {
		t.Fatalf("expected state SPEAKING, got %s", session.State())
	}

	// Now trigger Barge-In (caller speaks while AI is speaking)
	start := time.Now()
	session.TriggerBargeIn()
	latency := time.Since(start)

	if latency > 40*time.Millisecond {
		t.Errorf("barge-in latency exceeded 40ms SLA: took %v", latency)
	}

	// State must immediately return to LISTENING
	if session.State() != model.StateListening {
		t.Fatalf("expected state LISTENING after barge-in, got %s", session.State())
	}

	// Audio out queue must be completely drained
	select {
	case chunk := <-session.AudioOut():
		t.Fatalf("audio out queue was not drained: found leftover chunk %v", chunk)
	default:
		// Queue cleanly empty
	}
}

func TestConcurrentVoiceSessions(t *testing.T) {
	mgr := pipeline.NewSessionManager(5 * time.Minute)
	defer mgr.Stop()

	var wg sync.WaitGroup
	workers := 25

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			sessID := "session_concurrent_" + string(rune('A'+workerID))
			sess := mgr.GetOrCreate(sessID)

			// Ingest voiced frame
			_ = sess.IngestAudio(&model.AudioFrame{
				Energy:   0.08,
				IsVoiced: true,
			})

			// Start LLM stream
			toks := make(chan string, 2)
			toks <- "Welcome. "
			close(toks)
			sess.StartLLMResponseStream(toks)

			// Trigger barge in
			sess.TriggerBargeIn()
			sess.FinishPlayback()
		}(i)
	}

	wg.Wait()
}

func TestVADAudioStreamBargeInDeadlockFree(t *testing.T) {
	session := pipeline.NewVoiceSession("sess_vad_bargein_deadlock")
	defer session.Close()

	// 1. User speaks
	for i := 0; i < 3; i++ {
		_ = session.IngestAudio(&model.AudioFrame{Energy: 0.05, IsVoiced: true})
	}
	time.Sleep(360 * time.Millisecond) // Silence hangover triggers thinking
	_ = session.IngestAudio(&model.AudioFrame{Energy: 0.001, IsVoiced: false})

	// 2. Stream tokens to enter speaking state
	tokens := make(chan string, 5)
	tokens <- "Hello, "
	tokens <- "I can help you with your reservation today. "
	close(tokens)
	session.StartLLMResponseStream(tokens)

	// Wait for audio
	select {
	case <-session.AudioOut():
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for audio chunk")
	}

	if session.State() != model.StateSpeaking {
		t.Fatalf("expected state SPEAKING, got %s", session.State())
	}

	// 3. User interrupts by sending voiced audio frames (triggering VAD barge-in inside ProcessFrame)
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Send 2 consecutive voiced frames to trigger OnBargeIn from within ProcessFrame
		_ = session.IngestAudio(&model.AudioFrame{Energy: 0.08, IsVoiced: true})
		_ = session.IngestAudio(&model.AudioFrame{Energy: 0.08, IsVoiced: true})
	}()

	select {
	case <-done:
		// Success: no deadlock!
	case <-time.After(1 * time.Second):
		t.Fatalf("DEADLOCK DETECTED: IngestAudio hung while triggering VAD barge-in")
	}

	if session.State() != model.StateListening {
		t.Errorf("expected state LISTENING after audio barge-in, got %s", session.State())
	}
}
