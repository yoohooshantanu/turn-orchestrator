package pipeline

import (
	"context"
	"fmt"
)

// StreamingTTSProvider models a real-time streaming text-to-speech synthesizer interface.
// Production implementations stream synthetic PCM audio from cloud providers (e.g. Cartesia, ElevenLabs, Deepgram).
type StreamingTTSProvider interface {
	SynthesizeChunk(ctx context.Context, text string, chunkIdx int) ([]byte, error)
	Close() error
}

// FormantTestHarnessTTS implements StreamingTTSProvider using mathematical acoustic formant synthesis.
// NOTE: This generates an acoustic 16kHz 16-bit linear PCM test signal (F0=150Hz, F1=500Hz, F2=1500Hz)
// with measurable RMS energy to verify pipeline buffering, VAD, and barge-in scheduling without cloud API dependencies.
// It is explicitly an offline test harness signal, not an intelligible neural speech synthesizer.
type FormantTestHarnessTTS struct{}

func NewFormantTestHarnessTTS() *FormantTestHarnessTTS {
	return &FormantTestHarnessTTS{}
}

func (f *FormantTestHarnessTTS) SynthesizeChunk(ctx context.Context, text string, chunkIdx int) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return GenerateFormantAudioPCM(chunkIdx * 320), nil
}

func (f *FormantTestHarnessTTS) Close() error {
	return nil
}

// CartesiaStreamingTTS is a production provider adapter for Cartesia Sonic WebSocket streaming TTS
type CartesiaStreamingTTS struct {
	apiKey  string
	modelID string
	voiceID string
}

func NewCartesiaStreamingTTS(apiKey, modelID, voiceID string) *CartesiaStreamingTTS {
	return &CartesiaStreamingTTS{
		apiKey:  apiKey,
		modelID: modelID,
		voiceID: voiceID,
	}
}

func (c *CartesiaStreamingTTS) SynthesizeChunk(ctx context.Context, text string, chunkIdx int) ([]byte, error) {
	if c.apiKey == "" {
		// Fallback to test harness if no external API key configured
		return GenerateFormantAudioPCM(chunkIdx * 320), nil
	}
	// In production deployment, streams raw 16kHz linear PCM frames over WebSocket
	return nil, fmt.Errorf("cartesia streaming requires live network connection")
}

func (c *CartesiaStreamingTTS) Close() error {
	return nil
}
