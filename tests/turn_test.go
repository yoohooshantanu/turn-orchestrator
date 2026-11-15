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

func TestSentenceChunkerAbbreviationAndEllipsis(t *testing.T) {
	var chunks []*model.SentenceChunk
	var mu sync.Mutex

	sc := chunker.NewSentenceChunker("sess_abbrev", "turn_1", func(chunk *model.SentenceChunk) {
		mu.Lock()
		defer mu.Unlock()
		chunks = append(chunks, chunk)
	})

	// Stream text containing honorific abbreviation "Dr." and currency with comma "$5,000"
	tokens := []string{"Hello, ", "please ", "meet ", "Dr. ", "Smith ", "at ", "the ", "clinic. ", "The total cost is $5,000 in cash."}
	for _, tok := range tokens {
		sc.IngestToken(tok)
	}
	sc.Flush()

	mu.Lock()
	defer mu.Unlock()

	// It should NOT have split on "Dr." or "$5,000"
	for _, c := range chunks {
		if c.Text == "Dr." || c.Text == "please meet Dr." {
			t.Errorf("unexpected split at honorific abbreviation: %s", c.Text)
		}
		if c.Text == "$5" || c.Text == "000 in cash." {
			t.Errorf("unexpected split on currency comma: %s", c.Text)
		}
	}
}

func TestStrictAudioChunkSequentialOrdering(t *testing.T) {
	session := pipeline.NewVoiceSession("sess_seq_order")
	defer session.Close()

	// Simulate turn
	for i := 0; i < 3; i++ {
		_ = session.IngestAudio(&model.AudioFrame{Energy: 0.05, IsVoiced: true})
	}
	time.Sleep(360 * time.Millisecond)
	_ = session.IngestAudio(&model.AudioFrame{Energy: 0.001, IsVoiced: false})

	tokenCh := make(chan string, 10)
	tokenCh <- "First sentence is short. "
	tokenCh <- "Second sentence is slightly longer with more details. "
	tokenCh <- "Third sentence concludes the message completely."
	close(tokenCh)

	session.StartLLMResponseStream(tokenCh)

	var receivedIndices []int
	timeout := time.After(2 * time.Second)

ReceiveLoop:
	for len(receivedIndices) < 3 {
		select {
		case chunk := <-session.AudioOut():
			receivedIndices = append(receivedIndices, chunk.ChunkIdx)
		case <-timeout:
			break ReceiveLoop
		}
	}

	if len(receivedIndices) < 2 {
		t.Fatalf("expected at least 2 chunks, got %d", len(receivedIndices))
	}

	// Verify monotonic index ordering: [0, 1, 2...]
	for i := 1; i < len(receivedIndices); i++ {
		if receivedIndices[i] <= receivedIndices[i-1] {
			t.Errorf("out-of-order audio chunk detected: received %v", receivedIndices)
		}
	}
}

func TestEndToEndBargeInPipelineLatency(t *testing.T) {
	session := pipeline.NewVoiceSession("sess_e2e_bargein")
	defer session.Close()

	// 1. Advance session to SPEAKING state
	for i := 0; i < 3; i++ {
		_ = session.IngestAudio(&model.AudioFrame{Energy: 0.05, IsVoiced: true})
	}
	time.Sleep(360 * time.Millisecond)
	_ = session.IngestAudio(&model.AudioFrame{Energy: 0.001, IsVoiced: false})

	tokens := make(chan string, 5)
	tokens <- "Your appointment is confirmed for "
	tokens <- "tomorrow afternoon at 2 PM. Please let us know if you need to reschedule."
	close(tokens)
	session.StartLLMResponseStream(tokens)

	// Wait for audio synthesis
	select {
	case <-session.AudioOut():
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for audio chunk")
	}

	if session.State() != model.StateSpeaking {
		t.Fatalf("expected state SPEAKING, got %s", session.State())
	}

	// 2. Synthesize raw 16-bit linear PCM audio frame (20ms @ 16kHz = 640 bytes)
	pcm := make([]byte, 640)
	for i := 0; i < len(pcm); i += 2 {
		pcm[i] = 0x00
		pcm[i+1] = 0x25 // Generates RMS energy ~0.07 > 0.02 threshold
	}

	// 3. Measure complete end-to-end barge-in latency from caller speech frame arrival
	// through RMS -> VAD -> FSM Interruption -> Context Cancel -> Audio Buffer Drain -> Event Emission
	t0 := time.Now()

	// First voiced frame increments consecutive counter
	_ = session.IngestAudio(&model.AudioFrame{Data: pcm})
	// Second voiced frame triggers Barge-In
	_ = session.IngestAudio(&model.AudioFrame{Data: pcm})

	// Wait for barge-in event confirmation
	var bargeInEvt *model.TurnEvent
	timeout := time.After(500 * time.Millisecond)
EventLoop:
	for {
		select {
		case evt := <-session.EventOut():
			if evt.Type == model.EventBargeInTriggered {
				bargeInEvt = evt
				break EventLoop
			}
		case <-timeout:
			t.Fatalf("timed out waiting for barge in event")
		}
	}

	totalDuration := time.Since(t0)

	t.Logf(">>> VERIFIED END-TO-END BARGE-IN PIPELINE LATENCY: %v (Telephony SLA < 40ms)", totalDuration)

	if totalDuration > 40*time.Millisecond {
		t.Errorf("end-to-end barge-in exceeded 40ms SLA: took %v", totalDuration)
	}

	if session.State() != model.StateListening {
		t.Errorf("expected state LISTENING, got %s", session.State())
	}

	if bargeInEvt == nil {
		t.Errorf("expected EventBargeInTriggered event")
	}

	// Verify audio output queue was completely drained
	select {
	case leftover := <-session.AudioOut():
		t.Errorf("audio queue was not drained: found chunk %v", leftover)
	default:
		// Queue cleanly empty
	}
}

func TestWebSocketSessionReconnectRecovery(t *testing.T) {
	mgr := pipeline.NewSessionManager(5 * time.Minute)
	defer mgr.Stop()

	sessionID := "sess_reconnect_test_123"
	sess := mgr.GetOrCreate(sessionID)

	// Ingest audio
	_ = sess.IngestAudio(&model.AudioFrame{Energy: 0.06, IsVoiced: true})

	// 1. Simulate mobile network drop: client disconnects
	sess.MarkDetached()

	isDetached, dur := sess.IsDetached()
	if !isDetached {
		t.Errorf("expected session to be marked detached")
	}
	if dur > 1*time.Second {
		t.Errorf("expected detachment duration near 0, got %v", dur)
	}

	// Session must still be available in manager for reconnection
	savedSess, ok := mgr.Get(sessionID)
	if !ok || savedSess == nil {
		t.Fatalf("expected session to be preserved in manager during grace window")
	}

	// 2. Client reconnects within grace window
	reconnectedSess := mgr.GetOrCreate(sessionID)
	reconnectedSess.MarkAttached()

	if reconnectedSess.ReconnectCount() != 1 {
		t.Errorf("expected reconnect count 1, got %d", reconnectedSess.ReconnectCount())
	}

	detachedNow, _ := reconnectedSess.IsDetached()
	if detachedNow {
		t.Errorf("expected session to be active, not detached")
	}

	// Conversation state must be seamlessly preserved
	if reconnectedSess.State() != sess.State() {
		t.Errorf("expected state %s, got %s", sess.State(), reconnectedSess.State())
	}
}

func TestStaleAudioDroppedAfterBargeIn(t *testing.T) {
	session := pipeline.NewVoiceSession("sess_stale_audio_test")
	defer session.Close()

	tokenCh := make(chan string, 10)

	// Start speaking turn with initial audio
	session.StartSpeakingTurnForBench()

	// Start LLM stream with delayed synthesis
	session.StartLLMResponseStream(tokenCh)
	tokenCh <- "First"
	tokenCh <- " sentence"
	tokenCh <- " being"
	tokenCh <- " generated."

	// Interrupt immediately
	session.TriggerBargeIn()

	if session.State() != model.StateListening {
		t.Errorf("expected state LISTENING after barge-in, got %s", session.State())
	}

	close(tokenCh)

	// Allow background workers to finish
	time.Sleep(50 * time.Millisecond)

	// Stale audio chunks from the cancelled turn generation must NOT be in audioOut
	select {
	case chunk := <-session.AudioOut():
		t.Fatalf("stale audio chunk detected in output queue after barge-in: turn=%s, idx=%d", chunk.TurnID, chunk.ChunkIdx)
	default:
		// Cleanly empty
	}
}

