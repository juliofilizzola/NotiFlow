package app

import (
	"context"
	"errors"
	"net/http"
)

func (a *App) Run(ctx context.Context) error {
	workCtx, cancelWork := context.WithCancel(context.Background())
	defer cancelWork()

	poolDone := make(chan struct{})
	go func() {
		a.pool.Run(workCtx)
		close(poolDone)
	}()
	go a.drainDLQ()

	serverErr := make(chan error, 1)
	go func() {
		a.log.Info("listening", "addr", a.server.Addr)
		if err := a.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	var err error
	select {
	case <-ctx.Done():
	case err = <-serverErr:
	}
	a.shutdown(cancelWork, poolDone)
	return err
}

func (a *App) shutdown(cancelWork context.CancelFunc, poolDone <-chan struct{}) {
	a.log.Info("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout)
	defer cancel()

	if err := a.server.Shutdown(ctx); err != nil {
		a.log.Error("http shutdown", "err", err)
	}
	a.broker.Close()

	select {
	case <-poolDone:
	case <-ctx.Done():
		a.log.Warn("drain timeout, cancelling in-flight work")
		cancelWork()
		<-poolDone
	}
	a.log.Info("stopped")
}

func (a *App) drainDLQ() {
	for dl := range a.broker.DeadLetters() {
		a.log.Warn("dlq", "event_id", dl.Event.EventID, "attempts", dl.Attempts, "permanent", dl.Permanent, "reason", dl.Reason)
	}
}
