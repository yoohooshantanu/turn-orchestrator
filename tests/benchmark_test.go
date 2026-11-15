package tests

import (
	"crypto/rand"
	"testing"

	"github.com/yoohooshantanu/vocalflow/internal/chunker"
	"github.com/yoohooshantanu/vocalflow/internal/model"
	"github.com/yoohooshantanu/vocalflow/internal/pipeline"
	"github.com/yoohooshantanu/vocalflow/internal/vad"
)

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
		"Thank", " you", " for", " calling", " Callease", " support", ". ",
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
	sess := pipeline.NewVoiceSession("sess_bench_barge")
	defer sess.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess.TriggerBargeIn()
	}
}
