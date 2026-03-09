package concurrent

import (
	"context"
	"iter"
	"sync"
)

// Pipeline provides concurrent processing of iterator sequences with a transform function.
//
// This function takes an input iterator sequence and applies a transformation function
// to each element concurrently, respecting the context's concurrency limits. It maintains
// ordering and provides proper error handling and cancellation support.
//
// Type Parameters:
//   - Out: The output type after transformation
//   - In: The input type before transformation
//
// Parameters:
//   - ctx: Context that controls concurrency limits and cancellation
//   - seq: Input iterator sequence of In and error pairs
//   - transform: Function that transforms input items to output items
//
// Returns:
//
//	An iterator sequence that yields Out and error pairs, maintaining the
//	order of the input sequence while processing items concurrently.
//
// The function:
//   - Respects context concurrency limits from concurrent.Limit(ctx)
//   - Maintains proper ordering of results
//   - Handles context cancellation gracefully
//   - Uses goroutines for concurrent processing
//   - Propagates errors from both input sequence and transform function
func Pipeline[Out, In any](
	ctx context.Context,
	seq iter.Seq2[In, error],
	transform func(context.Context, In) (Out, error),
) iter.Seq2[Out, error] {
	return func(yield func(Out, error) bool) {
		type result struct {
			out   Out
			err   error
			panic any
		}

		type promise chan result
		limit := Limit(ctx)
		promises := make(chan promise, limit)
		semaphore := make(chan struct{}, limit)

		ctx, cancel := context.WithCancel(ctx)
		go func() {
			waitGroup := new(sync.WaitGroup)
			defer close(promises)
			defer waitGroup.Wait()

			for in, err := range seq {
				resolve := make(promise, 1)
				promises <- resolve

				if err != nil {
					resolve <- result{err: err}
					return
				}

				select {
				case semaphore <- struct{}{}:
				case <-ctx.Done():
					resolve <- result{err: context.Cause(ctx)}
					return
				}

				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					defer func() { <-semaphore }()
					defer recoverAndRethrow(func(v any) {
						resolve <- result{panic: v}
					})

					out, err := transform(ctx, in)
					resolve <- result{out: out, err: err}
				}()
			}
		}()

		defer func() {
			cancel()
			for range promises {
			}
		}()

		for p := range promises {
			r := <-p
			if r.panic != nil {
				panic(r.panic)
			}
			if !yield(r.out, r.err) {
				return
			}
		}
	}
}

// Exec executes multiple tasks concurrently and returns an iterator of errors.
//
// This is a special case of Pipeline where each task is a simple function that
// returns only an error. All tasks are executed concurrently up to the context's
// concurrency limit.
//
// Parameters:
//   - ctx: Context that controls concurrency limits and cancellation
//   - tasks: Variable number of functions to execute concurrently
//
// Returns:
//
//	An iterator sequence that yields error values, one for each task.
//	The order of errors corresponds to the order of tasks provided.
//
// Example:
//
//	for err := range Exec(ctx, task1, task2, task3) {
//	    if err != nil {
//	        // handle error
//	    }
//	}
func Exec(ctx context.Context, tasks ...func(context.Context) error) iter.Seq[error] {
	return func(yield func(error) bool) {
		for _, err := range Pipeline(ctx,
			func(yield func(func(context.Context) error, error) bool) {
				for _, task := range tasks {
					if !yield(task, nil) {
						return
					}
				}
			},
			func(ctx context.Context, task func(context.Context) error) (struct{}, error) {
				return struct{}{}, task(ctx)
			},
		) {
			if !yield(err) {
				return
			}
		}
	}
}

// Query executes multiple query tasks concurrently and returns an iterator of results and errors.
//
// This is a special case of Pipeline where each task is a function that returns
// a result and an error. All tasks are executed concurrently up to the context's
// concurrency limit.
//
// Type Parameters:
//   - R: The result type returned by each task
//
// Parameters:
//   - ctx: Context that controls concurrency limits and cancellation
//   - tasks: Variable number of query functions to execute concurrently
//
// Returns:
//
//	An iterator sequence that yields result and error pairs, one for each task.
//	The order of results corresponds to the order of tasks provided.
//
// Example:
//
//	for result, err := range Query(ctx, query1, query2, query3) {
//	    if err != nil {
//	        // handle error
//	    } else {
//	        // use result
//	    }
//	}
func Query[R any](ctx context.Context, tasks ...func(context.Context) (R, error)) iter.Seq2[R, error] {
	return Pipeline(ctx,
		func(yield func(func(context.Context) (R, error), error) bool) {
			for _, task := range tasks {
				if !yield(task, nil) {
					return
				}
			}
		},
		func(ctx context.Context, task func(context.Context) (R, error)) (R, error) {
			return task(ctx)
		},
	)
}

// items converts a slice to an iterator sequence.
// Each element is yielded with a nil error.
func items[T any](s []T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, item := range s {
			if !yield(item, nil) {
				return
			}
		}
	}
}

// pair holds a key-value pair from a map iteration.
type pair[K, V any] struct {
	key K
	val V
}

// pairs converts a map to an iterator sequence of key-value pairs.
// Each pair is yielded with a nil error.
func pairs[K comparable, V any](m map[K]V) iter.Seq2[pair[K, V], error] {
	return func(yield func(pair[K, V], error) bool) {
		for key, value := range m {
			if !yield(pair[K, V]{key, value}, nil) {
				return
			}
		}
	}
}

// Run is a convenience function that processes each item in the jobs slice
// concurrently using the provided process function.
//
// Results are returned in the same order as the input jobs, regardless of
// which jobs complete first. This is achieved by using Pipeline internally.
//
// The concurrency level is controlled by the context's concurrency limit
// (see WithLimit).
func Run[R, T any](ctx context.Context, jobs []T, process func(context.Context, T) (R, error)) iter.Seq2[R, error] {
	return Pipeline(ctx, items(jobs), process)
}

// Run2 is like Run but it takes its input jobs as a map.
//
// For each key-value pair in the jobs map, the process function is called
// concurrently. Note that map iteration order in Go is not deterministic,
// so while results maintain the iteration order, that order itself varies
// between runs.
//
// The concurrency level is controlled by the context's concurrency limit
// (see WithLimit).
func Run2[R any, K comparable, V any](ctx context.Context, jobs map[K]V, process func(context.Context, K, V) (R, error)) iter.Seq2[R, error] {
	return Pipeline(ctx, pairs(jobs), func(ctx context.Context, p pair[K, V]) (R, error) {
		return process(ctx, p.key, p.val)
	})
}

// RunTasks processes each item in the tasks slice concurrently using the
// provided process function, returning the first error encountered in input
// order.
//
// Tasks are executed concurrently, but errors are checked in the order of
// the input slice. This means if tasks[0] and tasks[2] both fail, the error
// from tasks[0] will be returned even if tasks[2] completed first.
//
// The concurrency level is controlled by the context's concurrency limit
// (see WithLimit).
//
// Type parameter T represents the type of input items to process.
func RunTasks[T any](ctx context.Context, tasks []T, process func(context.Context, T) error) error {
	for _, err := range Pipeline(ctx, items(tasks), func(ctx context.Context, task T) (struct{}, error) {
		return struct{}{}, process(ctx, task)
	}) {
		if err != nil {
			return err
		}
	}
	return nil
}

// RunTasks2 is like RunTasks but it takes its input tasks as a map.
//
// Note that map iteration order in Go is not deterministic, so while errors
// are returned in iteration order, that order itself varies between runs.
func RunTasks2[K comparable, V any](ctx context.Context, tasks map[K]V, process func(context.Context, K, V) error) error {
	for _, err := range Pipeline(ctx, pairs(tasks), func(ctx context.Context, p pair[K, V]) (struct{}, error) {
		return struct{}{}, process(ctx, p.key, p.val)
	}) {
		if err != nil {
			return err
		}
	}
	return nil
}
