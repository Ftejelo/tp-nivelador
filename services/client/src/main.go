package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/client"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

func loadConfig() (client.Config, error) {
	agencyId := os.Getenv("AGENCY_ID")
	if agencyId == "" {
		return client.Config{}, errors.New("AGENCY_ID environment variable is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.Config{}, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.Config{}, errors.New("SERVER_PORT environment variable is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.Config{}, errors.New("INPUT_FILE environment variable is required")
	}

	outputFile := os.Getenv("OUTPUT_FILE")
	if outputFile == "" {
		return client.Config{}, errors.New("OUTPUT_FILE environment variable is required")
	}
	batchSizeRaw := os.Getenv("BATCH_SIZE")
	if batchSizeRaw == "" {
		return client.Config{}, errors.New("BATCH_SIZE environment variable is required")
	}
	batchSize, err := strconv.Atoi(batchSizeRaw)
	if err != nil || batchSize <= 0 {
		return client.Config{}, errors.New("BATCH_SIZE environment variable must be a positive integer")
	}

	return client.Config{
		ServerHost: serverHost,
		ServerPort: serverPort,
		AgencyId:   agencyId,
		InputFile:  inputFile,
		OutputFile: outputFile,
		BatchSize:  batchSize,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		logger.Error("load-config", logger.Fail, "err", err)
		return 1
	}

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT)

	client, err := client.NewClient(config)
	if err != nil {
		logger.Error("client-new", logger.Fail, "err", err)
		return 1
	}

	// Run client in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- client.RunWithContext(ctx)
	}()

	// Wait for either client completion or signal
	select {
	case err := <-errChan:
		if err != nil {
			logger.Error("client-run", logger.Fail, "err", err)
			return 1
		}
		return 0
	case sig := <-sigChan:
		logger.Info("shutdown", logger.InProgress, "signal", sig)
		cancel() // Signal client to shutdown gracefully
		// Wait for client to finish with timeout
		select {
		case err := <-errChan:
			// Context cancellation is expected during graceful shutdown
			if err != nil && err != context.Canceled {
				logger.Error("client-run", logger.Fail, "err", err)
				return 1
			}
			logger.Info("shutdown", logger.Success)
			return 0
		case <-time.After(10 * time.Second):
			logger.Warn("shutdown", logger.Fail, "reason", "timeout")
			return 1
		}
	}
}

func main() {
	os.Exit(run())
}
