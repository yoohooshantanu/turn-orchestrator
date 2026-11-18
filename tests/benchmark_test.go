package tests

import (
	"crypto/rand"
	"io"
	"log/slog"
	"testing"

	"github.com/yoohooshantanu/turn-orchestrator/internal/chunker"
	"github.com/yoohooshantanu/turn-orchestrator/internal/logger"
	"github.com/yoohooshantanu/turn-orchestrator/internal/model"
	"github.com/yoohooshantanu/turn-orchestrator/internal/pipeline"
	"github.com/yoohooshantanu/turn-orchestrator/internal/vad"
)

func init() {
	// Silence verbose logger output during benchmarks
	logger.L = slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func BenchmarkRMSEnergyCalculation(b *testing.B) {
	// Standard 20ms audio frame at 16kHz 16-bit mono = 640 bytes
	pcm := make([]byte, 640)
	_, _ = rand.Read(pcm)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vad.ComputeRMSEnergy(pcm)
	}
}

func BenchmarkSentenceChunker(b *testing.B) {
	tokens := []string{
		"Thank", " you", " for", " calling", " Cloud", " support", ". ",
		"How", " may", " I", " assist", " you", " today", "?",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := chunker.NewSentenceChunker("sess_bench", "turn_bench", func(chunk *model.SentenceChunk) {})
		for _, tok := range tokens {
			c.IngestToken(tok)
		}
		c.Flush()
	}
}

// BenchmarkSpeakingTurnSetupAlone measures the overhead of session creation,
// turn initialization, and audio chunk pre-buffering alone (no barge-in).
func BenchmarkSpeakingTurnSetupAlone(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess := pipeline.NewVoiceSession("sess_bench_setup")
		sess.StartSpeakingTurnForBench()
		sess.Close()
	}
}

// BenchmarkBargeInCancellationLatency measures in-process cancellation latency
// with session creation and turn setup excluded from the timed region.
func BenchmarkBargeInCancellationLatency(b *testing.B) {
	sess := pipeline.NewVoiceSession("sess_bench_barge")
	defer sess.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess.StartSpeakingTurnForBench()
		b.StartTimer()

		sess.TriggerBargeIn()
	}
}

// BenchmarkTriggerBargeInDirect isolates pure in-process cancellation latency (mutex acquisition,
// context cancellation, atomic generation increment, output queue drain, and FSM transition)
// from all setup overhead.
func BenchmarkTriggerBargeInDirect(b *testing.B) {
	sess := pipeline.NewVoiceSession("sess_bench_direct")
	defer sess.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess.StartSpeakingTurnForBench()
		b.StartTimer()

		sess.TriggerBargeIn()
	}
}

// BenchmarkEndToEndBargeInPipeline measures processing time after frames arrive:
// raw PCM ingestion -> RMS calculation -> VAD voiced detection -> Barge-in execution -> event emission.
// Session setup is excluded from the timed region via StopTimer.
func BenchmarkEndToEndBargeInPipeline(b *testing.B) {
	pcm := make([]byte, 640)
	for i := 0; i < len(pcm); i += 2 {
		pcm[i] = 0x00
		pcm[i+1] = 0x25 // RMS energy ~0.07 > 0.02 threshold
	}

	sess := pipeline.NewVoiceSession("sess_bench_e2e")
	defer sess.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		sess.StartSpeakingTurnForBench()
		b.StartTimer()

		// Feed consecutive voiced PCM frames through VAD to trigger Barge-In
		_ = sess.IngestAudio(&model.AudioFrame{Data: pcm})
		_ = sess.IngestAudio(&model.AudioFrame{Data: pcm})
	}
}
