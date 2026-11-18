package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yoohooshantanu/turn-orchestrator/internal/logger"
	"github.com/yoohooshantanu/turn-orchestrator/internal/pipeline"
	"github.com/yoohooshantanu/turn-orchestrator/internal/server"
)

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	port := getEnv("PORT", "8085")
	logger.L.Info("starting_turn_orchestrator_engine", "port", port)

	mgr := pipeline.NewSessionManager(30 * time.Minute)
	mgr.SetTTSFactory(func() pipeline.StreamingTTSProvider {
		return pipeline.NewTTSProviderFromEnv()
	})
	handler := server.NewServerHandler(mgr)

	httpServer := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		logger.L.Info("turn_orchestrator_listening", "port", port)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.L.Error("server_listen_failed", "error", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.L.Info("shutting_down_turn-orchestrator")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = httpServer.Shutdown(ctx)
	mgr.Stop()
	logger.L.Info("turn_orchestrator_terminated_cleanly")
}
