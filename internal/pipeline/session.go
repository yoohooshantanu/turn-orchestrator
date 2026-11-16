package pipeline

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/yoohooshantanu/vocalflow/internal/chunker"
	"github.com/yoohooshantanu/vocalflow/internal/fsm"
	"github.com/yoohooshantanu/vocalflow/internal/logger"
	"github.com/yoohooshantanu/vocalflow/internal/metrics"
	"github.com/yoohooshantanu/vocalflow/internal/model"
	"github.com/yoohooshantanu/vocalflow/internal/vad"
)


// GenerateFormantAudioPCM synthesizes 20ms of 16kHz 16-bit mono linear PCM speech waveform
// with fundamental pitch F0=150Hz and acoustic vocal tract formants F1=500Hz, F2=1500Hz.
func GenerateFormantAudioPCM(sampleOffset int) []byte {
	const sampleRate = 16000
	const numSamples = 320 // 20ms @ 16kHz
	buf := make([]byte, numSamples*2)

	for i := 0; i < numSamples; i++ {
		t := float64(sampleOffset+i) / float64(sampleRate)
		// Fundamental voice frequency (150Hz) + Formant 1 (500Hz) + Formant 2 (1500Hz)
		sampleFloat := 0.5*math.Sin(2*math.Pi*150.0*t) +
			0.3*math.Sin(2*math.Pi*500.0*t) +
			0.2*math.Sin(2*math.Pi*1500.0*t)

		// 16-bit signed PCM scaling (-32768 to 32767)
		val := int16(sampleFloat * 16000.0)
		buf[2*i] = byte(val & 0xFF)
		buf[2*i+1] = byte((val >> 8) & 0xFF)
	}
	return buf
}

type EventListener func(event *model.TurnEvent)

type queuedSentence struct {
	chunk   *model.SentenceChunk
	turnGen uint64
}

// VoiceSession coordinates the real-time full-duplex conversational turn loop
type VoiceSession struct {
	mu                sync.RWMutex
	sessionID         string
	fsm               *fsm.TurnFSM
	vad               *vad.VoiceActivityDetector
	currentTurnID     string
	turnCount         uint64
	turnGen           uint64
	activeTurnGen     uint64
	activeTurnCtx     context.Context
	activeTurnCancel  context.CancelFunc
	activeTurnMetrics *model.TurnMetrics
	turnHistory       map[string]*model.TurnMetrics
	audioOut          chan *model.AudioChunk
	eventOut          chan *model.TurnEvent
	stopCh            chan struct{}
	stopOnce          sync.Once
	isClosed          bool
	isDetached        bool
	detachedAt        time.Time
	reconnectCount    int
	graceWindow       time.Duration
	eventListeners    []EventListener
	synthesisBarrierHook func(turnGen uint64)
	ttsProvider          StreamingTTSProvider
}

func NewVoiceSession(sessionID string) *VoiceSession {
	if sessionID == "" {
		sessionID = "sess_" + uuid.NewString()[:8]
	}

	stateMachine := fsm.NewTurnFSM(sessionID)

	s := &VoiceSession{
		sessionID:      sessionID,
		fsm:            stateMachine,
		turnHistory:    make(map[string]*model.TurnMetrics),
		audioOut:       make(chan *model.AudioChunk, 512),
		eventOut:       make(chan *model.TurnEvent, 512),
		stopCh:         make(chan struct{}),
		graceWindow:    60 * time.Second,
		eventListeners: make([]EventListener, 0),
		ttsProvider:    NewFormantTestHarnessTTS(),
	}

	vadCallbacks := vad.VADCallbacks{
		OnSpeechStart: func(t time.Time) {
			s.handleUserSpeechStart(t)
		},
		OnSpeechEnd: func(t time.Time, duration time.Duration) {
			s.handleUserSpeechEnd(t, duration)
		},
		OnBargeIn: func(t time.Time) {
			s.TriggerBargeIn()
		},
	}

	s.vad = vad.NewVoiceActivityDetector(sessionID, vad.DefaultVADConfig(), stateMachine, vadCallbacks)
	_ = s.fsm.Transition(model.StateListening)

	metrics.DefaultMetrics.IncSession()
	logger.L.Info("voice_session_started", "session_id", sessionID)

	return s
}

func (s *VoiceSession) SessionID() string {
	return s.sessionID
}

func (s *VoiceSession) State() model.ConversationState {
	return s.fsm.Current()
}

func (s *VoiceSession) FSM() *fsm.TurnFSM {
	return s.fsm
}

func (s *VoiceSession) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isClosed
}

func (s *VoiceSession) AudioOut() <-chan *model.AudioChunk {
	return s.audioOut
}

func (s *VoiceSession) EventOut() <-chan *model.TurnEvent {
	return s.eventOut
}

func (s *VoiceSession) GetTurnMetrics(turnID string) (*model.TurnMetrics, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.turnHistory[turnID]
	if !ok || m == nil {
		return nil, false
	}
	copied := *m
	return &copied, true
}

func (s *VoiceSession) GetActiveTurnMetrics() *model.TurnMetrics {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.activeTurnMetrics == nil {
		return nil
	}
	copied := *s.activeTurnMetrics
	return &copied
}

// SetActiveTurnSilenceDetectedAt configures user silence detection timestamp for synthetic benchmark or test turns
func (s *VoiceSession) SetActiveTurnSilenceDetectedAt(t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeTurnMetrics != nil {
		s.activeTurnMetrics.UserSilenceDetectedAt = t
	}
}

// MarkDetached flags the session as disconnected while preserving conversation state during grace window
func (s *VoiceSession) MarkDetached() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isDetached = true
	s.detachedAt = time.Now()
	logger.L.Warn("session_client_detached", "session_id", s.sessionID, "reconnect_window_sec", s.graceWindow.Seconds())
}

// MarkAttached restores active streaming upon WebSocket client reconnection
func (s *VoiceSession) MarkAttached() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isDetached {
		s.isDetached = false
		s.detachedAt = time.Time{}
		s.reconnectCount++
		logger.L.Info("session_client_reconnected", "session_id", s.sessionID, "reconnect_count", s.reconnectCount)
	}
}

// IsDetached returns whether the session is currently awaiting client reconnection
func (s *VoiceSession) IsDetached() (bool, time.Duration) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.isDetached {
		return false, 0
	}
	return true, time.Since(s.detachedAt)
}

// ShouldExpire checks whether a detached session has exceeded the reconnect grace window
func (s *VoiceSession) ShouldExpire(window time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.isClosed {
		return true
	}
	if s.isDetached && time.Since(s.detachedAt) > window {
		return true
	}
	return false
}

func (s *VoiceSession) ReconnectCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.reconnectCount
}

// IngestAudio processes incoming telephony audio frames through VAD

// SetSynthesisBarrierHook registers an optional concurrency barrier hook invoked directly
// prior to guarded audio queue admission. Used for deterministic mathematical race testing.

// SetTTSProvider configures a streaming text-to-speech synthesis engine (test harness or cloud adapter)
func (s *VoiceSession) SetTTSProvider(provider StreamingTTSProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ttsProvider = provider
}

func (s *VoiceSession) SetSynthesisBarrierHook(hook func(turnGen uint64)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.synthesisBarrierHook = hook
}

func (s *VoiceSession) IngestAudio(frame *model.AudioFrame) error {
	s.mu.RLock()
	if s.isClosed {
		s.mu.RUnlock()
		return model.ErrSessionClosed
	}
	s.mu.RUnlock()

	frame.SessionID = s.sessionID
	s.vad.ProcessFrame(frame)
	return nil
}

func (s *VoiceSession) handleUserSpeechStart(t time.Time) {
	curr := s.fsm.Current()
	if curr == model.StateSpeaking || curr == model.StateThinking || curr == model.StateDraining {
		s.TriggerBargeIn()
	}

	s.emitEvent(&model.TurnEvent{
		Type:      model.EventUserSpeechStart,
		SessionID: s.sessionID,
		Timestamp: t.UnixMilli(),
	})
}

func (s *VoiceSession) handleUserSpeechEnd(silenceDetectedAt time.Time, speechDuration time.Duration) {
	s.mu.Lock()
	if s.isClosed {
		s.mu.Unlock()
		return
	}

	turnNum := atomic.AddUint64(&s.turnCount, 1)
	turnGen := atomic.AddUint64(&s.turnGen, 1)
	s.activeTurnGen = turnGen
	turnID := fmt.Sprintf("turn_%s_%d", s.sessionID, turnNum)
	s.currentTurnID = turnID

	turnCtx, cancel := context.WithCancel(context.Background())
	s.activeTurnCtx = turnCtx
	s.activeTurnCancel = cancel

	turnMetrics := &model.TurnMetrics{
		SessionID:             s.sessionID,
		TurnID:                turnID,
		UserSilenceDetectedAt: silenceDetectedAt,
	}
	s.activeTurnMetrics = turnMetrics

	// Cap turnHistory to 100 entries to prevent memory accumulation in prolonged calls
	if len(s.turnHistory) >= 100 {
		for k := range s.turnHistory {
			delete(s.turnHistory, k)
			break
		}
	}
	s.turnHistory[turnID] = turnMetrics
	s.mu.Unlock()

	_ = s.fsm.Transition(model.StateThinking)

	s.emitEvent(&model.TurnEvent{
		Type:      model.EventUserSpeechEnd,
		SessionID: s.sessionID,
		TurnID:    turnID,
		Timestamp: silenceDetectedAt.UnixMilli(),
	})
}

// StartSpeakingTurnForBench initializes an active turn in SPEAKING state with cancelable context and buffered audio chunks
func (s *VoiceSession) StartSpeakingTurnForBench() {
	s.mu.Lock()
	s.turnCount++
	turnGen := atomic.AddUint64(&s.turnGen, 1)
	s.activeTurnGen = turnGen
	turnID := fmt.Sprintf("turn_bench_%d", s.turnCount)
	s.currentTurnID = turnID

	turnCtx, cancel := context.WithCancel(context.Background())
	s.activeTurnCtx = turnCtx
	s.activeTurnCancel = cancel
	s.activeTurnMetrics = &model.TurnMetrics{
		SessionID:             s.sessionID,
		TurnID:                turnID,
		UserSilenceDetectedAt: time.Now(),
	}

	// Pre-fill audio buffer with speech chunks
	for i := 0; i < 5; i++ {
		chunk := &model.AudioChunk{
			SessionID:  s.sessionID,
			TurnID:     turnID,
			ChunkIdx:   i,
			Data:       GenerateFormantAudioPCM(i * 320),
			DurationMs: 20,
			Timestamp:  time.Now().UnixMilli(),
		}
		select {
		case s.audioOut <- chunk:
		default:
		}
	}
	s.mu.Unlock()

	if s.fsm.Is(model.StateListening) {
		_ = s.fsm.Transition(model.StateThinking)
	}
	_ = s.fsm.Transition(model.StateSpeaking)
}

// StartLLMResponseStream simulates/receives LLM streaming tokens for the current turn
func (s *VoiceSession) StartLLMResponseStream(tokenStream <-chan string) {
	s.mu.Lock()
	turnID := s.currentTurnID
	turnCtx := s.activeTurnCtx
	turnGen := s.activeTurnGen
	s.mu.Unlock()

	if turnID == "" || turnCtx == nil {
		return
	}

	// Sequential synthesis pipeline: guarantees strict in-order audio emission to telephony output
	sentenceCh := make(chan queuedSentence, 64)

	go func() {
		for {
			select {
			case <-turnCtx.Done():
				return
			case item, ok := <-sentenceCh:
				if !ok {
					return
				}
				s.synthesizeAndStream(turnCtx, item.chunk, item.turnGen)
			}
		}
	}()

	chunkerEngine := chunker.NewSentenceChunker(s.sessionID, turnID, func(chunk *model.SentenceChunk) {
		metrics.DefaultMetrics.IncSentence()
		s.handleSentenceReady(turnCtx, chunk)
		select {
		case <-turnCtx.Done():
		case sentenceCh <- queuedSentence{chunk: chunk, turnGen: turnGen}:
		}
	})

	go func() {
		defer close(sentenceCh)
		defer chunkerEngine.Flush()

		firstTokenRecorded := false
		for {
			select {
			case <-turnCtx.Done():
				logger.L.Debug("llm_stream_canceled", "session_id", s.sessionID, "turn_id", turnID)
				return
			case token, ok := <-tokenStream:
				if !ok {
					return
				}

				if !firstTokenRecorded {
					firstTokenRecorded = true
					s.recordFirstToken()
				}

				chunkerEngine.IngestToken(token)
			}
		}
	}()
}

func (s *VoiceSession) recordFirstToken() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeTurnMetrics != nil && s.activeTurnMetrics.FirstTokenAt.IsZero() {
		now := time.Now()
		s.activeTurnMetrics.FirstTokenAt = now
		if !s.activeTurnMetrics.UserSilenceDetectedAt.IsZero() {
			s.activeTurnMetrics.TTFT = now.Sub(s.activeTurnMetrics.UserSilenceDetectedAt)
		}
	}
}

func (s *VoiceSession) handleSentenceReady(ctx context.Context, chunk *model.SentenceChunk) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	s.emitEvent(&model.TurnEvent{
		Type:      model.EventSentenceReady,
		SessionID: s.sessionID,
		TurnID:    chunk.TurnID,
		Payload:   chunk.Text,
		Timestamp: time.Now().UnixMilli(),
	})
}

func (s *VoiceSession) synthesizeAndStream(ctx context.Context, chunk *model.SentenceChunk, turnGen uint64) {
	if atomic.LoadUint64(&s.turnGen) != turnGen || ctx.Err() != nil {
		return
	}

	// Thread-safe read of configured TTS provider under mutex read lock
	s.mu.RLock()
	tts := s.ttsProvider
	s.mu.RUnlock()
	if tts == nil {
		return
	}

	frameIdx := 0

	// Stream audio incrementally frame-by-frame as received from TTS provider
	err := tts.SynthesizeStream(ctx, chunk.Text, chunk.Index, func(pcmFrame []byte) error {
		// Guard against stale turn generation or barge-in interruption while streaming
		if atomic.LoadUint64(&s.turnGen) != turnGen || ctx.Err() != nil {
			return ctx.Err()
		}

		// Record accurate Time-to-First-Audio (TTFA) upon arrival of the very FIRST synthesized audio frame.
		// Note: TTFA measures backend pipeline latency (user silence detection -> first synthesized PCM frame from TTS ready for transmission).
		// Downstream network delivery and client jitter buffering are excluded.
		s.mu.Lock()
		if s.activeTurnMetrics != nil && s.activeTurnMetrics.FirstAudioReadyAt.IsZero() {
			now := time.Now()
			s.activeTurnMetrics.FirstAudioReadyAt = now
			if !s.activeTurnMetrics.UserSilenceDetectedAt.IsZero() {
				ttfa := now.Sub(s.activeTurnMetrics.UserSilenceDetectedAt)
				s.activeTurnMetrics.TTFA = ttfa
				metrics.DefaultMetrics.RecordTTFA(ttfa)
				logger.L.Info("time_to_first_audio_recorded",
					"session_id", s.sessionID,
					"turn_id", chunk.TurnID,
					"ttfa_ms", ttfa.Milliseconds(),
				)
			}
		}
		s.mu.Unlock()

		// Advance FSM to Speaking state upon arrival of first audio frame
		if s.fsm.Is(model.StateThinking) {
			_ = s.fsm.Transition(model.StateSpeaking)
		}

		// Calculate frame duration dynamically based on 16kHz, 16-bit mono linear PCM format (32 bytes per ms)
		durationMs := len(pcmFrame) / 32
		if durationMs == 0 && len(pcmFrame) > 0 {
			durationMs = 1
		}

		audioChunk := &model.AudioChunk{
			SessionID:  s.sessionID,
			TurnID:     chunk.TurnID,
			ChunkIdx:   chunk.Index*1000 + frameIdx,
			Data:       pcmFrame,
			DurationMs: int64(durationMs),
			Timestamp:  time.Now().UnixMilli(),
		}
		frameIdx++

		s.mu.RLock()
		hook := s.synthesisBarrierHook
		s.mu.RUnlock()
		if hook != nil {
			hook(turnGen)
		}

		// Guarded enqueue to guarantee no stale frame from an interrupted turn ever enters audioOut
		s.mu.Lock()
		if atomic.LoadUint64(&s.turnGen) != turnGen || ctx.Err() != nil {
			s.mu.Unlock()
			return ctx.Err()
		}

		// High watermark backpressure: drop oldest unconsumed packet if buffer exceeds 256 chunks
		if len(s.audioOut) >= 256 {
			select {
			case <-s.audioOut:
				logger.L.Warn("audio_buffer_high_watermark_dropped_oldest", "session_id", s.sessionID)
			default:
			}
		}

		select {
		case s.audioOut <- audioChunk:
			metrics.DefaultMetrics.IncFrame()
		default:
			logger.L.Warn("audio_out_queue_full_dropped_frame", "session_id", s.sessionID)
		}
		s.mu.Unlock()

		s.emitEvent(&model.TurnEvent{
			Type:      model.EventTTSAudioChunk,
			SessionID: s.sessionID,
			TurnID:    chunk.TurnID,
			Payload:   chunk.Text,
			Timestamp: time.Now().UnixMilli(),
		})

		return nil
	})

	if err != nil && ctx.Err() == nil {
		logger.L.Error("streaming_synthesis_failed", "session_id", s.sessionID, "turn_id", chunk.TurnID, "error", err)

		// Session-level mid-turn error handling: invalidate turn, drain output audio, reset to listening
		s.mu.Lock()
		atomic.AddUint64(&s.turnGen, 1)
		if s.activeTurnCancel != nil {
			s.activeTurnCancel()
			s.activeTurnCancel = nil
		}
	DrainErrorLoop:
		for {
			select {
			case <-s.audioOut:
			default:
				break DrainErrorLoop
			}
		}
		s.mu.Unlock()

		_ = s.fsm.Transition(model.StateListening)

		s.emitEvent(&model.TurnEvent{
			Type:      model.EventStreamError,
			SessionID: s.sessionID,
			TurnID:    chunk.TurnID,
			Payload:   err.Error(),
			Timestamp: time.Now().UnixMilli(),
		})
	}
}

// TriggerBargeIn instantly cancels in-flight LLM/TTS generation, drains output buffers, and resets state
func (s *VoiceSession) TriggerBargeIn() {
	start := time.Now()

	s.mu.Lock()
	if s.isClosed {
		s.mu.Unlock()
		return
	}

	curr := s.fsm.Current()
	if curr != model.StateSpeaking && curr != model.StateThinking && curr != model.StateDraining {
		s.mu.Unlock()
		return
	}

	// Invalidate in-flight and future audio chunks from this turn
	atomic.AddUint64(&s.turnGen, 1)

	// 1. Immediately cancel the active turn context
	if s.activeTurnCancel != nil {
		s.activeTurnCancel()
		s.activeTurnCancel = nil
	}

	if s.activeTurnMetrics != nil {
		s.activeTurnMetrics.Interrupted = true
	}

	// 2. Non-blocking drain of unconsumed audio packets in output queue
	drained := 0
DrainLoop:
	for {
		select {
		case <-s.audioOut:
			drained++
		default:
			break DrainLoop
		}
	}

	s.mu.Unlock()

	// 3. Atomically transition state to INTERRUPTED then LISTENING
	_ = s.fsm.Transition(model.StateInterrupted)
	_ = s.fsm.Transition(model.StateListening)

	bargeInLatency := time.Since(start)
	metrics.DefaultMetrics.RecordBargeIn(bargeInLatency)
	metrics.DefaultMetrics.IncTurn(true)

	logger.L.Warn("barge_in_executed",
		"session_id", s.sessionID,
		"reaction_latency_us", bargeInLatency.Microseconds(),
		"drained_packets", drained,
	)

	// 4. Emit BargeIn notification so client telephony player halts local audio playback
	s.emitEvent(&model.TurnEvent{
		Type:      model.EventBargeInTriggered,
		SessionID: s.sessionID,
		Payload:   fmt.Sprintf("drained:%d,latency_us:%d", drained, bargeInLatency.Microseconds()),
		Timestamp: time.Now().UnixMilli(),
	})

	s.vad.Reset()
}

// FinishPlayback marks the end of AI response playback, returning to LISTENING mode
func (s *VoiceSession) FinishPlayback() {
	s.mu.Lock()
	if s.activeTurnMetrics != nil {
		s.activeTurnMetrics.CompletedAt = time.Now()
		metrics.DefaultMetrics.IncTurn(s.activeTurnMetrics.Interrupted)
	}
	s.mu.Unlock()

	if s.fsm.Is(model.StateSpeaking) || s.fsm.Is(model.StateDraining) {
		_ = s.fsm.Transition(model.StateListening)
	}

	s.emitEvent(&model.TurnEvent{
		Type:      model.EventTurnCompleted,
		SessionID: s.sessionID,
		Timestamp: time.Now().UnixMilli(),
	})

	s.vad.Reset()
}

// AddEventListener registers an external callback for conversational turn events
func (s *VoiceSession) AddEventListener(listener EventListener) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eventListeners = append(s.eventListeners, listener)
}

func (s *VoiceSession) emitEvent(evt *model.TurnEvent) {
	s.mu.RLock()
	listeners := make([]EventListener, len(s.eventListeners))
	copy(listeners, s.eventListeners)
	s.mu.RUnlock()

	for _, l := range listeners {
		l(evt)
	}

	select {
	case <-s.stopCh:
		return
	case s.eventOut <- evt:
	default:
	}
}

// Close gracefully stops the voice session, closes the TTS provider, and releases channels
func (s *VoiceSession) Close() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.isClosed = true
		if s.activeTurnCancel != nil {
			s.activeTurnCancel()
		}
		tts := s.ttsProvider
		s.mu.Unlock()

		if tts != nil {
			_ = tts.Close()
		}

		_ = s.fsm.Transition(model.StateIdle)
		close(s.stopCh)
		metrics.DefaultMetrics.DecSession()
		logger.L.Info("voice_session_closed", "session_id", s.sessionID)
	})
}
