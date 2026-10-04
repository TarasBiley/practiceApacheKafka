package main

import (
	"audit-service/internal/config"
	"audit-service/internal/consumer"
	"audit-service/internal/handler"
	"audit-service/internal/kafkaclient"
	"audit-service/internal/outbox"
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "audit-service/docs"

	"github.com/jackc/pgx/v5/pgxpool"
	httpSwagger "github.com/swaggo/http-swagger"
)

// @title Audit Service API
// @version 1.0
// @description API для аудита действий пользователей.
// @host localhost:8080
// @BasePath /
func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	cfg := config.Load()

	kafkaWriter := kafkaclient.NewWriter(
		cfg.KafkaBroker,
		cfg.KafkaTopic,
	)
	defer kafkaWriter.Close()

	kafkaReader := kafkaclient.NewReader(
		cfg.KafkaBroker,
		cfg.KafkaTopic,
		cfg.KafkaGroup,
		cfg.KafkaCommitInterval,
		cfg.KafkaReadMaxBytes,
	)
	defer kafkaReader.Close()

	db, err := pgxpool.New(
		context.Background(),
		cfg.DatabaseURL,
	)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	if err := db.Ping(context.Background()); err != nil {
		panic(err)
	}

	stopOutbox := outbox.StartWorker(
		db,
		kafkaWriter,
		2*time.Second,
	)
	defer stopOutbox()

	stopAnalytics := consumer.StartAnalyticsConsumer(
		db,
		kafkaReader,
		cfg.AnalyticsInterval,
		cfg.AnalyticsWindow,
	)
	defer stopAnalytics()

	http.HandleFunc(
		"POST /api/audit",
		handler.AuditHandler(db),
	)

	http.HandleFunc(
		"GET /api/audit",
		handler.GetAuditHandler(db),
	)

	http.HandleFunc(
		"GET /api/stats",
		handler.GetStatsHandler(db),
	)

	http.HandleFunc(
		"POST /api/admin/rebuild-stats",
		handler.RebuildStatsHandler(
			db,
			cfg.KafkaBroker,
			cfg.KafkaTopic,
		),
	)

	http.Handle(
		"/swagger/",
		httpSwagger.WrapHandler,
	)

	server := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: http.DefaultServeMux,
	}

	go func() {
		log.Println("server started on :" + cfg.HTTPPort)

		if err := server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Printf("http server error: %v", err)
		}
	}()

	<-ctx.Done()

	log.Println("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}

	log.Println("server stopped")
}
