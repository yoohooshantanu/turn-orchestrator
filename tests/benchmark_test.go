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

func BenchmarkEndToEndBargeInPipeline(b *testing.B) {
	pcm := make([]byte, 640)
	for i := 0; i < len(pcm); i += 2 {
		pcm[i] = 0x00
		pcm[i+1] = 0x25
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sess := pipeline.NewVoiceSession("sess_bench_e2e")
		// Simulate active speech state
		_ = sess.IngestAudio(&model.AudioFrame{Energy: 0.05, IsVoiced: true})
		// Trigger barge-in via raw audio frames
		_ = sess.IngestAudio(&model.AudioFrame{Data: pcm})
		_ = sess.IngestAudio(&model.AudioFrame{Data: pcm})
		sess.Close()
	}
}
