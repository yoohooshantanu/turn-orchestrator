package vad

import (
	"math"
	"sync"
	"time"

	"github.com/yoohooshantanu/vocalflow/internal/fsm"
	"github.com/yoohooshantanu/vocalflow/internal/logger"
	"github.com/yoohooshantanu/vocalflow/internal/metrics"
	"github.com/yoohooshantanu/vocalflow/internal/model"
)

type VADCallbacks struct {
	OnSpeechStart    func(t time.Time)
	OnSpeechEnd      func(t time.Time, duration time.Duration)
	OnBargeIn        func(t time.Time)
}

type VADConfig struct {
	EnergyThreshold    float64       // Energy threshold distinguishing voice from silence
	SilenceHangover    time.Duration // Trailing silence duration to finalize user turn (e.g. 350ms)
	BargeInMinFrames   int           // Consecutive voiced frames needed to confirm barge-in (e.g. 2 frames = 40ms)
}

func DefaultVADConfig() VADConfig {
	return VADConfig{
		EnergyThreshold:  0.02,
		SilenceHangover:  350 * time.Millisecond,
		BargeInMinFrames: 2,
	}
}

// VoiceActivityDetector processes incoming PCM audio chunks and detects turn boundaries & interruptions
type VoiceActivityDetector struct {
	mu                sync.Mutex
	cfg               VADConfig
	fsm               *fsm.TurnFSM
	callbacks         VADCallbacks
	isSpeaking        bool
	speechStartTime   time.Time
	lastVoicedTime    time.Time
	consecVoiced      int
	consecUnvoiced    int
	sessionID         string
}

func NewVoiceActivityDetector(
	sessionID string,
	cfg VADConfig,
	fsm *fsm.TurnFSM,
	cb VADCallbacks,
) *VoiceActivityDetector {
	return &VoiceActivityDetector{
		sessionID: sessionID,
		cfg:       cfg,
		fsm:       fsm,
		callbacks: cb,
	}
}

// ComputeRMSEnergy calculates normalized Root Mean Square energy of 16-bit linear PCM audio
func ComputeRMSEnergy(pcm []byte) float64 {
	if len(pcm) < 2 {
		return 0
	}

	samples := len(pcm) / 2
	var sumSquares float64

	for i := 0; i < samples; i++ {
		// Read 16-bit signed integer little-endian
		low := uint16(pcm[2*i])
		high := uint16(pcm[2*i+1])
		val := int16((high << 8) | low)
		normalized := float64(val) / 32768.0
		sumSquares += normalized * normalized
	}

	return math.Sqrt(sumSquares / float64(samples))
}

// ProcessFrame inspects an audio frame, updates energy tracking, and fires state transitions
func (v *VoiceActivityDetector) ProcessFrame(frame *model.AudioFrame) {
	v.mu.Lock()
	defer v.mu.Unlock()

	now := time.Now()
	metrics.DefaultMetrics.IncFrame()

	energy := frame.Energy
	if energy == 0 && len(frame.Data) > 0 {
		energy = ComputeRMSEnergy(frame.Data)
		frame.Energy = energy
	}

	isVoiced := energy >= v.cfg.EnergyThreshold
	frame.IsVoiced = isVoiced

	currState := v.fsm.Current()

	// 1. Barge-In Interruption Detection:
	// Caller speaks while AI is Thinking, Speaking, or Draining
	if (currState == model.StateSpeaking || currState == model.StateThinking || currState == model.StateDraining) && isVoiced {
		v.consecVoiced++
		if v.consecVoiced >= v.cfg.BargeInMinFrames {
			logger.L.Warn("barge_in_detected",
				"session_id", v.sessionID,
				"state", string(currState),
				"energy", energy,
				"consecutive_voiced", v.consecVoiced,
			)

			v.consecVoiced = 0
			v.isSpeaking = true
			v.speechStartTime = now
			v.lastVoicedTime = now

			if v.callbacks.OnBargeIn != nil {
				v.callbacks.OnBargeIn(now)
			}
			return
		}
	}

	// 2. Normal Turn-Taking Processing in LISTENING state
	if isVoiced {
		v.consecVoiced++
		v.consecUnvoiced = 0
		v.lastVoicedTime = now

		if !v.isSpeaking && v.consecVoiced >= 2 {
			v.isSpeaking = true
			v.speechStartTime = now
			if v.callbacks.OnSpeechStart != nil {
				v.callbacks.OnSpeechStart(now)
			}
		}
	} else {
		v.consecUnvoiced++
		v.consecVoiced = 0

		if v.isSpeaking {
			// Check if silence hangover exceeded
			silenceDuration := now.Sub(v.lastVoicedTime)
			if silenceDuration >= v.cfg.SilenceHangover {
				v.isSpeaking = false
				turnDuration := v.lastVoicedTime.Sub(v.speechStartTime)

				logger.L.Info("user_speech_ended",
					"session_id", v.sessionID,
					"speech_duration_ms", turnDuration.Milliseconds(),
					"silence_hangover_ms", silenceDuration.Milliseconds(),
				)

				if v.callbacks.OnSpeechEnd != nil {
					v.callbacks.OnSpeechEnd(now, turnDuration)
				}
			}
		}
	}
}

// Reset clears speech tracking for the next conversational turn
func (v *VoiceActivityDetector) Reset() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.isSpeaking = false
	v.consecVoiced = 0
	v.consecUnvoiced = 0
}
