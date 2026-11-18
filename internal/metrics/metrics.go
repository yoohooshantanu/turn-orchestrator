package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	mu sync.RWMutex

	TotalTurns          uint64
	InterruptedTurns    uint64
	TotalAudioFrames    uint64
	TotalTokensStreamed uint64
	TotalSentences      uint64

	// Sub-millisecond latency trackers in microseconds
	sumTTFAUs      uint64
	countTTFA      uint64
	sumBargeInUs   uint64
	countBargeIn   uint64
	activeSessions int64
}

var DefaultMetrics = NewMetrics()

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) IncSession() {
	atomic.AddInt64(&m.activeSessions, 1)
}

func (m *Metrics) DecSession() {
	atomic.AddInt64(&m.activeSessions, -1)
}

func (m *Metrics) IncTurn(interrupted bool) {
	atomic.AddUint64(&m.TotalTurns, 1)
	if interrupted {
		atomic.AddUint64(&m.InterruptedTurns, 1)
	}
}

func (m *Metrics) RecordTTFA(d time.Duration) {
	atomic.AddUint64(&m.sumTTFAUs, uint64(d.Microseconds()))
	atomic.AddUint64(&m.countTTFA, 1)
}

func (m *Metrics) RecordBargeIn(d time.Duration) {
	atomic.AddUint64(&m.sumBargeInUs, uint64(d.Microseconds()))
	atomic.AddUint64(&m.countBargeIn, 1)
}

func (m *Metrics) AddTokens(n int) {
	atomic.AddUint64(&m.TotalTokensStreamed, uint64(n))
}

func (m *Metrics) IncSentence() {
	atomic.AddUint64(&m.TotalSentences, 1)
}

func (m *Metrics) IncFrame() {
	atomic.AddUint64(&m.TotalAudioFrames, 1)
}

func (m *Metrics) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		turns := atomic.LoadUint64(&m.TotalTurns)
		interrupts := atomic.LoadUint64(&m.InterruptedTurns)
		tokens := atomic.LoadUint64(&m.TotalTokensStreamed)
		sentences := atomic.LoadUint64(&m.TotalSentences)
		frames := atomic.LoadUint64(&m.TotalAudioFrames)
		sessions := atomic.LoadInt64(&m.activeSessions)

		avgTTFAMs := float64(0)
		cTTFA := atomic.LoadUint64(&m.countTTFA)
		if cTTFA > 0 {
			avgTTFAMs = (float64(atomic.LoadUint64(&m.sumTTFAUs)) / float64(cTTFA)) / 1000.0
		}

		avgBargeInMs := float64(0)
		cBargeIn := atomic.LoadUint64(&m.countBargeIn)
		if cBargeIn > 0 {
			avgBargeInMs = (float64(atomic.LoadUint64(&m.sumBargeInUs)) / float64(cBargeIn)) / 1000.0
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP turn_orchestrator_active_sessions Active live voice conversations\n")
		fmt.Fprintf(w, "# TYPE turn_orchestrator_active_sessions gauge\n")
		fmt.Fprintf(w, "turn_orchestrator_active_sessions %d\n", sessions)

		fmt.Fprintf(w, "# HELP turn_orchestrator_total_turns Total conversation turns processed\n")
		fmt.Fprintf(w, "# TYPE turn_orchestrator_total_turns counter\n")
		fmt.Fprintf(w, "turn_orchestrator_total_turns %d\n", turns)

		fmt.Fprintf(w, "# HELP turn_orchestrator_interrupted_turns Total turns interrupted by user barge-in\n")
		fmt.Fprintf(w, "# TYPE turn_orchestrator_interrupted_turns counter\n")
		fmt.Fprintf(w, "turn_orchestrator_interrupted_turns %d\n", interrupts)

		fmt.Fprintf(w, "# HELP turn_orchestrator_avg_ttfa_ms Average Time to First Audio in milliseconds\n")
		fmt.Fprintf(w, "# TYPE turn_orchestrator_avg_ttfa_ms gauge\n")
		fmt.Fprintf(w, "turn_orchestrator_avg_ttfa_ms %.3f\n", avgTTFAMs)

		fmt.Fprintf(w, "# HELP turn_orchestrator_avg_barge_in_reaction_ms Average barge-in cancellation latency in milliseconds\n")
		fmt.Fprintf(w, "# TYPE turn_orchestrator_avg_barge_in_reaction_ms gauge\n")
		fmt.Fprintf(w, "turn_orchestrator_avg_barge_in_reaction_ms %.3f\n", avgBargeInMs)

		fmt.Fprintf(w, "# HELP turn_orchestrator_total_tokens_streamed Total LLM tokens streamed\n")
		fmt.Fprintf(w, "# TYPE turn_orchestrator_total_tokens_streamed counter\n")
		fmt.Fprintf(w, "turn_orchestrator_total_tokens_streamed %d\n", tokens)

		fmt.Fprintf(w, "# HELP turn_orchestrator_total_audio_frames Total PCM audio frames processed\n")
		fmt.Fprintf(w, "# TYPE turn_orchestrator_total_audio_frames counter\n")
		fmt.Fprintf(w, "turn_orchestrator_total_audio_frames %d\n", frames)

		fmt.Fprintf(w, "# HELP turn_orchestrator_total_sentences_synthesized Total sentences pipelined to TTS\n")
		fmt.Fprintf(w, "# TYPE turn_orchestrator_total_sentences_synthesized counter\n")
		fmt.Fprintf(w, "turn_orchestrator_total_sentences_synthesized %d\n", sentences)
	}
}
