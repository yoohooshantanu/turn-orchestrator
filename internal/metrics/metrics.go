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

	// Latency trackers in microseconds
	sumTTFAMs        uint64
	countTTFA        uint64
	sumBargeInMs     uint64
	countBargeIn     uint64
	activeSessions   int64
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
	atomic.AddUint64(&m.sumTTFAMs, uint64(d.Milliseconds()))
	atomic.AddUint64(&m.countTTFA, 1)
}

func (m *Metrics) RecordBargeIn(d time.Duration) {
	atomic.AddUint64(&m.sumBargeInMs, uint64(d.Milliseconds()))
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

		avgTTFA := float64(0)
		cTTFA := atomic.LoadUint64(&m.countTTFA)
		if cTTFA > 0 {
			avgTTFA = float64(atomic.LoadUint64(&m.sumTTFAMs)) / float64(cTTFA)
		}

		avgBargeIn := float64(0)
		cBargeIn := atomic.LoadUint64(&m.countBargeIn)
		if cBargeIn > 0 {
			avgBargeIn = float64(atomic.LoadUint64(&m.sumBargeInMs)) / float64(cBargeIn)
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP vocalflow_active_sessions Active live voice conversations\n")
		fmt.Fprintf(w, "# TYPE vocalflow_active_sessions gauge\n")
		fmt.Fprintf(w, "vocalflow_active_sessions %d\n", sessions)

		fmt.Fprintf(w, "# HELP vocalflow_total_turns Total conversation turns processed\n")
		fmt.Fprintf(w, "# TYPE vocalflow_total_turns counter\n")
		fmt.Fprintf(w, "vocalflow_total_turns %d\n", turns)

		fmt.Fprintf(w, "# HELP vocalflow_interrupted_turns Total turns interrupted by user barge-in\n")
		fmt.Fprintf(w, "# TYPE vocalflow_interrupted_turns counter\n")
		fmt.Fprintf(w, "vocalflow_interrupted_turns %d\n", interrupts)

		fmt.Fprintf(w, "# HELP vocalflow_avg_ttfa_ms Average Time to First Audio in milliseconds\n")
		fmt.Fprintf(w, "# TYPE vocalflow_avg_ttfa_ms gauge\n")
		fmt.Fprintf(w, "vocalflow_avg_ttfa_ms %.2f\n", avgTTFA)

		fmt.Fprintf(w, "# HELP vocalflow_avg_barge_in_reaction_ms Average barge-in cancellation latency\n")
		fmt.Fprintf(w, "# TYPE vocalflow_avg_barge_in_reaction_ms gauge\n")
		fmt.Fprintf(w, "vocalflow_avg_barge_in_reaction_ms %.2f\n", avgBargeIn)

		fmt.Fprintf(w, "# HELP vocalflow_total_tokens_streamed Total LLM tokens streamed\n")
		fmt.Fprintf(w, "# TYPE vocalflow_total_tokens_streamed counter\n")
		fmt.Fprintf(w, "vocalflow_total_tokens_streamed %d\n", tokens)

		fmt.Fprintf(w, "# HELP vocalflow_total_sentences_synthesized Total sentences pipelined to TTS\n")
		fmt.Fprintf(w, "# TYPE vocalflow_total_sentences_synthesized counter\n")
		fmt.Fprintf(w, "vocalflow_total_sentences_synthesized %d\n", frames)
		_ = sentences
	}
}
