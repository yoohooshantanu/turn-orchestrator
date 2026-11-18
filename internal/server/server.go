package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yoohooshantanu/turn-orchestrator/internal/logger"
	"github.com/yoohooshantanu/turn-orchestrator/internal/metrics"
	"github.com/yoohooshantanu/turn-orchestrator/internal/model"
	"github.com/yoohooshantanu/turn-orchestrator/internal/pipeline"
	"github.com/yoohooshantanu/turn-orchestrator/internal/vad"
)

// WSWriter defines the subset of websocket write methods used by the emitter
type WSWriter interface {
	WriteMessage(messageType int, data []byte) error
	WriteJSON(v interface{}) error
}

// RunEmitter streams binary audio frames and control events to the client.
func RunEmitter(ctx context.Context, session *pipeline.VoiceSession, writer WSWriter) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok := <-session.AudioOut():
			if !ok {
				return nil
			}
			// -------
			// Drop mismatched/stale turn generations at send time:
			// Invariant: after TriggerBargeIn returns, no frame from a superseded turn is delivered over WebSocket.
			// -------
			if chunk.Generation != session.TurnGeneration() {
				continue
			}
			// -------
			// Transmit raw PCM audio chunk bytes directly to client
			// -------
			if err := writer.WriteMessage(websocket.BinaryMessage, chunk.Data); err != nil {
				return err
			}

		case evt, ok := <-session.EventOut():
			if !ok {
				return nil
			}
			if err := writer.WriteJSON(evt); err != nil {
				return err
			}
		}
	}
}

// checkAuth enforces fail-closed Bearer token authentication.
// Query-string tokens are explicitly rejected to prevent token leakage in proxy logs and browser histories.
// In production (ENV=production or REQUIRE_AUTH=true), missing AUTH_TOKEN configuration fails closed.
func checkAuth(r *http.Request) bool {
	isProd := os.Getenv("ENV") == "production" || os.Getenv("VOCALFLOW_ENV") == "production" || os.Getenv("REQUIRE_AUTH") == "true"
	expectedToken := os.Getenv("AUTH_TOKEN")

	// -------
	// Fail-closed security guard: in production or when auth is required, an unset AUTH_TOKEN denies all access
	// -------
	if isProd && expectedToken == "" {
		logger.L.Error("auth_failed_closed_missing_auth_token_config")
		return false
	}

	// -------
	// Local development mode: allow unauthenticated traffic only if explicitly unconfigured and not in production
	// -------
	if expectedToken == "" {
		return true
	}

	authHeader := r.Header.Get("Authorization")
	if strings.HasPrefix(authHeader, "Bearer ") {
		token := strings.TrimPrefix(authHeader, "Bearer ")
		if subtle.ConstantTimeCompare([]byte(token), []byte(expectedToken)) == 1 {
			return true
		}
	}

	// -------
	// Query-string tokens (?token=...) are strictly rejected to prevent credential leaks into logs
	// -------
	return false
}

// MakeUpgrader builds a WebSocket upgrader with production-grade origin validation
func MakeUpgrader() websocket.Upgrader {
	return websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin: func(r *http.Request) bool {
			origin := r.Header.Get("Origin")
			// -------
			// Allow direct clients, telephony trunks, or tests with no Origin header
			// -------
			if origin == "" {
				return true
			}
			allowed := os.Getenv("ALLOWED_ORIGINS")
			if allowed != "" {
				for _, o := range strings.Split(allowed, ",") {
					o = strings.TrimSpace(o)
					if o == "*" || o == origin {
						return true
					}
				}
				return false
			}
			// -------
			// Secure default: enforce same-origin (RFC 6454) or standard localhost
			// -------
			u, err := url.Parse(origin)
			if err != nil {
				return false
			}
			if strings.EqualFold(u.Host, r.Host) {
				return true
			}
			if strings.HasPrefix(u.Host, "localhost:") || strings.HasPrefix(u.Host, "127.0.0.1:") || u.Host == "localhost" || u.Host == "127.0.0.1" {
				return true
			}
			return false
		},
	}
}

// NewServerHandler builds the full HTTP and WebSocket router
func NewServerHandler(mgr *pipeline.SessionManager) http.Handler {
	upgrader := MakeUpgrader()
	mux := http.NewServeMux()

	// -------
	// Session Lifecycle API
	// -------
	mux.HandleFunc("/v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}

		if !checkAuth(r) {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
			return
		}

		var req struct {
			SessionID string `json:"session_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)

		session := mgr.GetOrCreate(req.SessionID)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"session_id": session.SessionID(),
			"state":      session.State(),
			"created_at": time.Now().UTC().Format(time.RFC3339),
		})
	})

	// -------
	// Full-Duplex Binary & Text WebSocket Conversational Gateway
	// -------
	mux.HandleFunc("/ws/voice/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 || parts[0] != "ws" || parts[1] != "voice" {
			http.Error(w, "invalid path, expected /ws/voice/{session_id}", http.StatusBadRequest)
			return
		}
		sessionID := parts[2]

		if !checkAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.L.Error("ws_upgrade_failed", "error", err)
			return
		}
		defer conn.Close()

		session := mgr.GetOrCreate(sessionID)
		session.MarkAttached()

		// -------
		// On disconnect, mark detached with 60s grace period for reconnect recovery
		// -------
		defer session.MarkDetached()

		clientCtx, clientCancel := context.WithCancel(r.Context())
		defer clientCancel()

		// -------
		// Emitter loop: Push binary audio frames and control events to telephony client
		// -------
		go func() {
			_ = RunEmitter(clientCtx, session, conn)
		}()

		// -------
		// Ingestion loop: Receive raw PCM binary audio or control frames from telephony client
		// -------
		for {
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				break
			}

			// -------
			// Binary Frame: Ingest raw 16-bit linear PCM audio bytes (16kHz mono)
			// -------
			if messageType == websocket.BinaryMessage {
				if len(payload) > 0 {
					energy := vad.ComputeRMSEnergy(payload)
					_ = session.IngestAudio(&model.AudioFrame{
						SessionID: sessionID,
						Data:      payload,
						Energy:    energy,
						Timestamp: time.Now().UnixMilli(),
					})
				}
				continue
			}

			// Text Frame: Ingest JSON control commands
			var inMsg struct {
				Type     string  `json:"type"`
				Energy   float64 `json:"energy"`
				IsVoiced bool    `json:"is_voiced"`
			}
			if err := json.Unmarshal(payload, &inMsg); err != nil {
				continue
			}

			switch inMsg.Type {
			case "audio_frame":
				_ = session.IngestAudio(&model.AudioFrame{
					SessionID: sessionID,
					Energy:    inMsg.Energy,
					IsVoiced:  inMsg.IsVoiced,
					Timestamp: time.Now().UnixMilli(),
				})

			case "playback_finished":
				session.FinishPlayback()

			case "call_ended", "session_close":
				mgr.Delete(sessionID)
				return
			}
		}
	})

	// -------
	// Telemetry & Health Check Endpoints
	// -------
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":          "healthy",
			"active_sessions": mgr.ActiveCount(),
			"timestamp":       time.Now().UTC().Format(time.RFC3339),
		})
	})
	mux.HandleFunc("/metrics", metrics.DefaultMetrics.Handler())

	// -------
	// pprof profiling
	// -------
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)

	return mux
}
