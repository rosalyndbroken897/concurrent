# concurrent

Go package providing structured concurrency primitives built on iterators (`iter.Seq` / `iter.Seq2`). All concurrent operations respect a context-based concurrency limit and propagate panics safely across goroutine boundaries.

## Build & Test

Requires Go 1.25+.

```bash
go test ./...
```

## Key Files

- `pipeline.go` - Core concurrent pipeline, plus Exec, Query, Run, RunTasks helpers
- `queue.go` - Thread-safe bounded job queue with producer-consumer pattern (Queue, TaskQueue, Process)
- `limit.go` - Context-based concurrency limit (WithLimit / Limit)
- `panic.go` - Panic recovery helper for cross-goroutine panic propagation

## Public API

### Concurrency Control (`limit.go`)
- `WithLimit(ctx, n)` - Set max concurrency on context (can only decrease, never increase)
- `Limit(ctx)` - Read current limit (default: 10, clamped to [1, 1000])

### Pipeline (`pipeline.go`)
- `Pipeline[Out, In](ctx, seq, transform)` - Concurrent ordered transform of `iter.Seq2[In, error]`
- `Exec(ctx, tasks...)` - Run `func(context.Context) error` tasks concurrently, return `iter.Seq[error]`
- `Query[R](ctx, tasks...)` - Run `func(context.Context) (R, error)` tasks concurrently
- `Run[R, T](ctx, jobs, process)` - Process a slice concurrently, ordered results
- `Run2[R, K, V](ctx, jobs, process)` - Process a map concurrently
- `RunTasks[T](ctx, tasks, process)` - Process a slice, return first error
- `RunTasks2[K, V](ctx, tasks, process)` - Process a map, return first error

### Queue (`queue.go`)
- `Queue[T]` - Bounded concurrent job queue
- `NewQueue[T]()` / `NewQueueWithCapacity[T](n)` - Create queue
- `Push(job)` / `Pull()` / `Wait()` / `Flush()` / `Done()` - Queue operations
- `Process[T](ctx, queue)` - Consume queue with worker pool, return `iter.Seq2[T, error]`
- `TaskQueue` / `NewTaskQueue()` / `ProcessTasks(ctx, queue)` - Simplified error-only queue

## Code Conventions

- Iterator-based design: all concurrent operations return `iter.Seq` or `iter.Seq2`
- Context-based concurrency limits via `WithLimit` / `Limit`; limits can only decrease through the call tree
- Panic safety: panics in worker goroutines are captured and re-panicked on the caller's goroutine
- Pipeline preserves input ordering regardless of completion order
- Queue capacity defaults to 1,000,000; excess jobs emit `ErrFull`
