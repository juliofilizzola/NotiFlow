package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

type Policy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func (p Policy) Backoff(attempt int) time.Duration {
	if p.BaseDelay <= 0 {
		return 0
	}
	d := p.MaxDelay
	if shift := attempt - 1; shift < 62 {
		if exp := p.BaseDelay << shift; exp > 0 && exp < p.MaxDelay {
			d = exp
		}
	}
	half := int64(d / 2)
	return time.Duration(half + rand.Int64N(half+1))
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

func IsPermanent(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe)
}

func Do(
	ctx context.Context,
	p Policy,
	fn func(ctx context.Context, attempt int) error,
	onRetry func(attempt int, err error, wait time.Duration),
) (int, error) {
	maxAttempts := max(p.MaxAttempts, 1)
	for attempt := 1; ; attempt++ {
		err := fn(ctx, attempt)
		if err == nil {
			return attempt, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return attempt, ctxErr
		}
		if IsPermanent(err) || attempt >= maxAttempts {
			return attempt, err
		}

		wait := p.Backoff(attempt)
		if onRetry != nil {
			onRetry(attempt, err, wait)
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return attempt, ctx.Err()
		case <-timer.C:
		}
	}
}
