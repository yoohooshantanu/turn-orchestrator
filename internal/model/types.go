package model

import (
	"errors"
	"time"
)

var (
	ErrInvalidStateTransition = errors.New("invalid conversation state transition")
	ErrSessionClosed          = errors.New("vocal session is closed")
	ErrQueueFull              = errors.New("turn queue saturated")
	ErrTurnNotFound           = errors.New("turn not found")
)

type ConversationState string

const (
	StateIdle        ConversationState = "IDLE"
	StateListening   ConversationState = "LISTENING"
	StateThinking    ConversationState = "THINKING"
	StateSpeaking    ConversationState = "SPEAKING"
	StateInterrupted ConversationState = "INTERRUPTED"
	StateDraining    ConversationState = "DRAINING"
)

type EventType string

const (
	EventUserSpeechStart  EventType = "USER_SPEECH_START"
	EventUserSpeechEnd    EventType = "USER_SPEECH_END"
	EventLLMToken         EventType = "LLM_TOKEN"
	EventSentenceReady    EventType = "SENTENCE_READY"
	EventTTSAudioChunk    EventType = "TTS_AUDIO_CHUNK"
	EventBargeInTriggered EventType = "BARGE_IN_TRIGGERED"
	EventPlaybackFinished EventType = "PLAYBACK_FINISHED"
	EventTurnCompleted    EventType = "TURN_COMPLETED"
)

type TurnEvent struct {
	Type      EventType `json:"type"`
	SessionID string    `json:"session_id"`
	TurnID    string    `json:"turn_id,omitempty"`
	Payload   string    `json:"payload,omitempty"`
	Timestamp int64     `json:"timestamp"`
}

type AudioFrame struct {
	SessionID string  `json:"session_id"`
	Sequence  uint64  `json:"sequence"`
	Data      []byte  `json:"data"`
	Energy    float64 `json:"energy"`
	IsVoiced  bool    `json:"is_voiced"`
	Timestamp int64   `json:"timestamp"` // Unix millisecond
}

type SentenceChunk struct {
	SessionID    string    `json:"session_id"`
	TurnID       string    `json:"turn_id"`
	Index        int       `json:"index"`
	Text         string    `json:"text"`
	IsFirstChunk bool      `json:"is_first_chunk"`
	IsFinal      bool      `json:"is_final"`
	CreatedAt    time.Time `json:"created_at"`
}

type AudioChunk struct {
	SessionID string `json:"session_id"`
	TurnID    string `json:"turn_id"`
	ChunkIdx  int    `json:"chunk_idx"`
	Data      []byte `json:"data"`
	DurationMs int64 `json:"duration_ms"`
	Timestamp int64  `json:"timestamp"`
}

type TurnMetrics struct {
	SessionID             string        `json:"session_id"`
	TurnID                string        `json:"turn_id"`
	UserSilenceDetectedAt time.Time     `json:"user_silence_detected_at"`
	FirstTokenAt          time.Time     `json:"first_token_at"`
	FirstAudioReadyAt     time.Time     `json:"first_audio_ready_at"`
	TTFA                  time.Duration `json:"ttfa_ms"` // Time to First Audio
	TTFT                  time.Duration `json:"ttft_ms"` // Time to First Token
	BargeInLatency        time.Duration `json:"barge_in_latency_ms,omitempty"`
	Interrupted           bool          `json:"interrupted"`
	TotalTokens           int           `json:"total_tokens"`
	TotalSentences        int           `json:"total_sentences"`
	CompletedAt           time.Time     `json:"completed_at"`
}
