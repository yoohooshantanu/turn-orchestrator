package pipeline

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yoohooshantanu/turn-orchestrator/internal/logger"
)

// AudioChunkHandler is called incrementally for each incoming synthesized audio frame
type AudioChunkHandler func(pcmFrame []byte) error

// StreamingTTSProvider models a real-time streaming text-to-speech synthesizer interface.
// Production implementations stream synthetic PCM audio from cloud providers (e.g. Cartesia Sonic).
type StreamingTTSProvider interface {
	SynthesizeStream(ctx context.Context, text string, chunkIdx int, onAudioFrame AudioChunkHandler) error
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

func (f *FormantTestHarnessTTS) SynthesizeStream(ctx context.Context, text string, chunkIdx int, onAudioFrame AudioChunkHandler) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	pcm := GenerateFormantAudioPCM(chunkIdx * 320)
	if onAudioFrame != nil {
		return onAudioFrame(pcm)
	}
	return nil
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

// CartesiaStreamingTTS is the production provider adapter for Cartesia Sonic WebSocket streaming TTS
type CartesiaStreamingTTS struct {
	mu         sync.Mutex
	apiKey     string
	modelID    string
	voiceID    string
	endpoint   string
	closed     bool
	activeConn *websocket.Conn
}

func NewCartesiaStreamingTTS(apiKey, modelID, voiceID string) *CartesiaStreamingTTS {
	if modelID == "" {
		modelID = "sonic-english"
	}
	if voiceID == "" {
		voiceID = "a0e998e3-182d-4d96-8d1a-0e624b66ffc2"
	}
	return &CartesiaStreamingTTS{
		apiKey:   apiKey,
		modelID:  modelID,
		voiceID:  voiceID,
		endpoint: "wss://api.cartesia.ai/tts/websocket?cartesia_version=2024-06-10",
	}
}

// SetEndpoint overrides the WebSocket endpoint (useful for tests and mock servers)
func (c *CartesiaStreamingTTS) SetEndpoint(endpoint string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.endpoint = endpoint
}

// SynthesizeStream connects to Cartesia Sonic WebSocket, streams the request, and emits audio frames incrementally as they arrive
func (c *CartesiaStreamingTTS) SynthesizeStream(ctx context.Context, text string, chunkIdx int, onAudioFrame AudioChunkHandler) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("cartesia tts provider closed")
	}
	endpoint := c.endpoint
	c.mu.Unlock()

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if c.apiKey == "" {
		// -------
		// Deterministic offline test harness fallback when API key is not configured
		// -------
		pcm := GenerateFormantAudioPCM(chunkIdx * 320)
		if onAudioFrame != nil {
			return onAudioFrame(pcm)
		}
		return nil
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 1500 * time.Millisecond,
	}

	headers := make(http.Header)
	headers.Set("X-API-Key", c.apiKey)
	headers.Set("Cartesia-Version", "2024-06-10")

	// -------
	// Convert http/https test server URLs to ws/wss if necessary
	// -------
	wsEndpoint := endpoint
	if strings.HasPrefix(wsEndpoint, "http://") {
		wsEndpoint = "ws://" + strings.TrimPrefix(wsEndpoint, "http://")
	} else if strings.HasPrefix(wsEndpoint, "https://") {
		wsEndpoint = "wss://" + strings.TrimPrefix(wsEndpoint, "https://")
	}

	// Connect to Cartesia Sonic WebSocket
	conn, resp, err := dialer.DialContext(ctx, wsEndpoint, headers)
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		logger.L.Error("cartesia_websocket_dial_failed", "endpoint", wsEndpoint, "error", err)
		return fmt.Errorf("cartesia websocket dial error: %w", err)
	}
	defer conn.Close()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return fmt.Errorf("cartesia tts provider closed")
	}
	c.activeConn = conn
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		if c.activeConn == conn {
			c.activeConn = nil
		}
		c.mu.Unlock()
	}()

	// -------
	// Asynchronous context cancellation watcher: immediately interrupts blocking WebSocket ReadMessage on cancellation / barge-in
	// -------
	cancelDone := make(chan struct{})
	defer close(cancelDone)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-cancelDone:
		}
	}()

	// -------
	// Structured Cartesia WebSocket request frame
	// -------
	reqPayload := map[string]interface{}{
		"model_id":   c.modelID,
		"transcript": text,
		"voice": map[string]interface{}{
			"mode": "id",
			"id":   c.voiceID,
		},
		"output_format": map[string]interface{}{
			"container":   "raw",
			"encoding":    "pcm_s16le",
			"sample_rate": 16000,
		},
		"context_id": fmt.Sprintf("ctx_%d", chunkIdx),
		"continue":   false,
	}

	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := conn.WriteJSON(reqPayload); err != nil {
		return fmt.Errorf("cartesia write request failed: %w", err)
	}

	receivedFrames := 0
	receivedDone := false

	// -------
	// Stream read loop: delivers audio frames incrementally to onAudioFrame callback
	// -------
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !receivedDone {
				return fmt.Errorf("cartesia stream terminated prematurely before done: %w", err)
			}
			break
		}

		if msgType == websocket.BinaryMessage && len(payload) > 0 {
			receivedFrames++
			if onAudioFrame != nil {
				if err := onAudioFrame(payload); err != nil {
					return err
				}
			}
		} else if msgType == websocket.TextMessage {
			var respFrame struct {
				Type  string `json:"type"`
				Data  string `json:"data"`
				Done  bool   `json:"done"`
				Error string `json:"error"`
			}
			if err := json.Unmarshal(payload, &respFrame); err == nil {
				if respFrame.Error != "" {
					return fmt.Errorf("cartesia api error: %s", respFrame.Error)
				}
				if respFrame.Data != "" {
					decoded, err := base64.StdEncoding.DecodeString(respFrame.Data)
					if err == nil && len(decoded) > 0 {
						receivedFrames++
						if onAudioFrame != nil {
							if err := onAudioFrame(decoded); err != nil {
								return err
							}
						}
					}
				}
				if respFrame.Type == "done" || respFrame.Done {
					receivedDone = true
					break
				}
			}
		}
	}

	if !receivedDone {
		return fmt.Errorf("cartesia stream ended without done confirmation")
	}
	if receivedFrames == 0 {
		return fmt.Errorf("cartesia stream returned zero audio frames")
	}
	return nil
}

// SynthesizeChunk connects to Cartesia Sonic WebSocket, streams the chunk request, and consumes all incoming audio frames
func (c *CartesiaStreamingTTS) SynthesizeChunk(ctx context.Context, text string, chunkIdx int) ([]byte, error) {
	var accumulatedAudio []byte
	err := c.SynthesizeStream(ctx, text, chunkIdx, func(pcmFrame []byte) error {
		accumulatedAudio = append(accumulatedAudio, pcmFrame...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return accumulatedAudio, nil
}

func (c *CartesiaStreamingTTS) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.activeConn != nil {
		_ = c.activeConn.Close()
		c.activeConn = nil
	}
	return nil
}

// NewTTSProviderFromEnv constructs the configured streaming TTS provider from environment variables
func NewTTSProviderFromEnv() StreamingTTSProvider {
	providerName := strings.ToLower(os.Getenv("TTS_PROVIDER"))
	switch providerName {
	case "cartesia":
		apiKey := os.Getenv("CARTESIA_API_KEY")
		modelID := os.Getenv("CARTESIA_MODEL_ID")
		voiceID := os.Getenv("CARTESIA_VOICE_ID")
		return NewCartesiaStreamingTTS(apiKey, modelID, voiceID)
	default:
		return NewFormantTestHarnessTTS()
	}
}
