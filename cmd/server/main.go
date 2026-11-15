package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/yoohooshantanu/vocalflow/internal/logger"
	"github.com/yoohooshantanu/vocalflow/internal/metrics"
	"github.com/yoohooshantanu/vocalflow/internal/model"
	"github.com/yoohooshantanu/vocalflow/internal/pipeline"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  2048,
	WriteBufferSize: 2048,
	CheckOrigin: func(r *http.Request) bool {
		return true // Demo & dashboard cross-origin support
	},
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", "8085")
	logger.L.Info("starting_vocalflow_engine", "port", port)

	mgr := pipeline.NewSessionManager(30 * time.Minute)

	mux := http.NewServeMux()

	// 1. Session Lifecycle API
	mux.HandleFunc("/v1/sessions", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
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

	// 2. Full-Duplex WebSocket Conversational Gateway
	mux.HandleFunc("/ws/voice/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) < 3 || parts[0] != "ws" || parts[1] != "voice" {
			http.Error(w, "invalid path, expected /ws/voice/{session_id}", http.StatusBadRequest)
			return
		}
		sessionID := parts[2]

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.L.Error("ws_upgrade_failed", "error", err)
			return
		}
		defer conn.Close()

		session := mgr.GetOrCreate(sessionID)
		session.MarkAttached()

		// On disconnect, mark detached with 60s grace period for reconnect recovery
		defer session.MarkDetached()

		clientCtx, clientCancel := context.WithCancel(r.Context())
		defer clientCancel()

		// Emitter loop: Push audio frames and events to telephony client
		go func() {
			for {
				select {
				case <-clientCtx.Done():
					return
				case <-session.AudioOut():
					// Transmit synthesized audio chunk
					msg := map[string]interface{}{
						"type":       "audio_chunk",
						"session_id": sessionID,
						"timestamp":  time.Now().UnixMilli(),
					}
					_ = conn.WriteJSON(msg)

				case evt, ok := <-session.EventOut():
					if !ok {
						return
					}
					_ = conn.WriteJSON(evt)
				}
			}
		}()

		// Ingestion loop: Receive audio or control frames from telephony client
		for {
			var inMsg struct {
				Type     string  `json:"type"`
				Energy   float64 `json:"energy"`
				Token    string  `json:"token"`
				IsVoiced bool    `json:"is_voiced"`
			}

			if err := conn.ReadJSON(&inMsg); err != nil {
				break
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

	// 3. Telemetry & Health Check Endpoints
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status":          "healthy",
			"active_sessions": mgr.ActiveCount(),
			"timestamp":       time.Now().UTC().Format(time.RFC3339),
		})
	})
	mux.HandleFunc("/metrics", metrics.DefaultMetrics.Handler())

	// 4. pprof profiling
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		logger.L.Info("vocalflow_listening", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L.Error("server_listen_failed", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.L.Info("shutting_down_vocalflow")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = server.Shutdown(ctx)
	mgr.Stop()
	logger.L.Info("vocalflow_terminated_cleanly")
}
