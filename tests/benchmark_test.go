package tests

import (
	"crypto/rand"
	"io"
	"log/slog"
	"testing"

	"github.com/yoohooshantanu/vocalflow/internal/chunker"
	"github.com/yoohooshantanu/vocalflow/internal/logger"
	"github.com/yoohooshantanu/vocalflow/internal/model"
	"github.com/yoohooshantanu/vocalflow/internal/pipeline"
	"github.com/yoohooshantanu/vocalflow/internal/vad"
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

func BenchmarkBargeInCancellationLatency(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess := pipeline.NewVoiceSession("sess_bench_barge")
		// Put session into verified Speaking state with cancelable context and queued audio
		sess.StartSpeakingTurnForBench()
		sess.TriggerBargeIn()
		sess.Close()
	}
}

// BenchmarkTriggerBargeInDirect isolates pure in-process cancellation latency (mutex acquisition,
// context cancellation, atomic generation increment, output queue drain, and FSM transition)
// from session allocation and teardown overhead.
func BenchmarkTriggerBargeInDirect(b *testing.B) {
	sess := pipeline.NewVoiceSession("sess_bench_direct")
	defer sess.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess.StartSpeakingTurnForBench()
		sess.TriggerBargeIn()
	}
}

func BenchmarkEndToEndBargeInPipeline(b *testing.B) {
	pcm := make([]byte, 640)
	for i := 0; i < len(pcm); i += 2 {
		pcm[i] = 0x00
		pcm[i+1] = 0x25 // RMS energy ~0.07 > 0.02 threshold
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess := pipeline.NewVoiceSession("sess_bench_e2e")
		// Put session into active Speaking state with buffered audio
		sess.StartSpeakingTurnForBench()

		// Feed consecutive voiced PCM frames through VAD to trigger Barge-In
		_ = sess.IngestAudio(&model.AudioFrame{Data: pcm})
		_ = sess.IngestAudio(&model.AudioFrame{Data: pcm})

		sess.Close()
	}
}
