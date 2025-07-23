package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/vmyroslav/kafka-pulse-go/pulse"
)

type HealthServer struct {
	monitor  pulse.Monitor
	consumer *Consumer
	logger   *slog.Logger
	server   *http.Server
}

type HealthResponse struct {
	Status    string         `json:"status"`
	Timestamp time.Time      `json:"timestamp"`
	Details   map[string]any `json:"details,omitempty"`
}

type ControlRequest struct {
	Delay string `json:"delay,omitempty"`
}

type ControlResponse struct {
	Status          string        `json:"status"`
	Paused          bool          `json:"paused"`
	ProcessingDelay time.Duration `json:"processing_delay"`
	Message         string        `json:"message"`
}

func NewHealthServer(monitor pulse.Monitor, consumer *Consumer, logger *slog.Logger, port string) *HealthServer {
	hs := &HealthServer{
		monitor:  monitor,
		consumer: consumer,
		logger:   logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", hs.healthHandler)
	mux.HandleFunc("/health/ready", hs.readinessHandler)
	mux.HandleFunc("/health/live", hs.livenessHandler)

	// Control endpoints
	mux.HandleFunc("/control/pause", hs.pauseHandler)
	mux.HandleFunc("/control/resume", hs.resumeHandler)
	mux.HandleFunc("/control/slow", hs.slowHandler)
	mux.HandleFunc("/control/rebalance", hs.rebalanceHandler)
	mux.HandleFunc("/control/status", hs.statusHandler)

	hs.server = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	return hs
}

func (hs *HealthServer) Start() error {
	hs.logger.Info("Starting health server", "addr", hs.server.Addr)
	return hs.server.ListenAndServe()
}

func (hs *HealthServer) Stop(ctx context.Context) error {
	hs.logger.Info("Stopping health server")
	return hs.server.Shutdown(ctx)
}

func (hs *HealthServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	isHealthy, err := hs.monitor.Healthy(ctx)
	details := map[string]any{
		"consumer_paused":  hs.consumer.IsPaused(),
		"processing_delay": hs.consumer.GetProcessingDelay().String(),
		"stuck_timeout":    "10s",
	}

	if err != nil {
		details["error"] = err.Error()
	}

	if hs.consumer.IsPaused() {
		details["warning"] = "Consumer is manually paused - simulating stuck state"
	}

	response := HealthResponse{
		Timestamp: time.Now(),
		Details:   details,
	}

	if isHealthy {
		response.Status = "healthy"
		w.WriteHeader(http.StatusOK)
	} else {
		response.Status = "unhealthy"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.Header().Set("Content-Type", "application/json")
	if err = json.NewEncoder(w).Encode(response); err != nil {
		hs.logger.Error("failed to encode health response", "error", err)
	}

	hs.logger.Debug("health check completed",
		"status", response.Status,
		"details", details)
}

func (hs *HealthServer) readinessHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	isHealthy, err := hs.monitor.Healthy(ctx)
	details := map[string]any{
		"consumer_paused":  hs.consumer.IsPaused(),
		"processing_delay": hs.consumer.GetProcessingDelay().String(),
	}

	if err != nil {
		details["error"] = err.Error()
	}

	response := HealthResponse{
		Timestamp: time.Now(),
		Details:   details,
	}

	if isHealthy {
		response.Status = "ready"
		w.WriteHeader(http.StatusOK)
	} else {
		response.Status = "not ready"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (hs *HealthServer) livenessHandler(w http.ResponseWriter, r *http.Request) {
	response := HealthResponse{
		Status:    "alive",
		Timestamp: time.Now(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// pauseHandler pauses the consumer (simulates stuck consumer)
func (hs *HealthServer) pauseHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hs.consumer.Pause()

	response := ControlResponse{
		Status:          "success",
		Paused:          true,
		ProcessingDelay: hs.consumer.GetProcessingDelay(),
		Message:         "Consumer paused - simulating stuck consumer",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// resumeHandler resumes the consumer
func (hs *HealthServer) resumeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hs.consumer.Resume()

	response := ControlResponse{
		Status:          "success",
		Paused:          false,
		ProcessingDelay: hs.consumer.GetProcessingDelay(),
		Message:         "Consumer resumed - recovering from stuck state",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// slowHandler sets the processing delay
func (hs *HealthServer) slowHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ControlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.Delay == "" {
		http.Error(w, "delay field is required", http.StatusBadRequest)
		return
	}

	delay, err := time.ParseDuration(req.Delay)
	if err != nil {
		http.Error(w, "Invalid delay format", http.StatusBadRequest)
		return
	}

	hs.consumer.SetProcessingDelay(delay)

	response := ControlResponse{
		Status:          "success",
		Paused:          hs.consumer.IsPaused(),
		ProcessingDelay: delay,
		Message:         "Processing delay updated to " + delay.String(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// rebalanceHandler triggers a consumer group rebalance
func (hs *HealthServer) rebalanceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := hs.consumer.TriggerRebalance(); err != nil {
		response := ControlResponse{
			Status:          "error",
			Paused:          hs.consumer.IsPaused(),
			ProcessingDelay: hs.consumer.GetProcessingDelay(),
			Message:         "Failed to trigger rebalance: " + err.Error(),
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(response)
		return
	}

	response := ControlResponse{
		Status:          "success",
		Paused:          hs.consumer.IsPaused(),
		ProcessingDelay: hs.consumer.GetProcessingDelay(),
		Message:         "Consumer group rebalance triggered successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// statusHandler returns current consumer status
func (hs *HealthServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	response := ControlResponse{
		Status:          "success",
		Paused:          hs.consumer.IsPaused(),
		ProcessingDelay: hs.consumer.GetProcessingDelay(),
		Message:         "Current consumer status",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
