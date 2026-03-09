# concurrent [![CI](https://github.com/firetiger-oss/concurrent/actions/workflows/ci.yml/badge.svg)](https://github.com/firetiger-oss/concurrent/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/firetiger-oss/concurrent.svg)](https://pkg.go.dev/github.com/firetiger-oss/concurrent)

A modern take on structured concurrency in Go.

## Motivation

Go provides excellent low-level concurrency primitives (goroutines,
channels, sync package), but composing them into higher-level patterns such as
fan-out pipelines, bounded worker pools, ordered result collection—requires
repetitive boilerplate and careful coordination. Furthermore, applying limits to
avoid unbounded resource utilization due to spawning too many goroutines is a
tedious task that often gets overlooked and only breaks when the code is shipped
to production. Spawning goroutines also introduces reliability challenges, such
as dealing with panics unwinding to the start of a goroutines and terminating the
entire program, which is rarely the desirable behavior in server applications that
Go is often used for.

The `concurrent` package provides a small set of composable building blocks
that handle goroutine lifecycle, ordering, error handling, and panic propagation.
All concurrent operations respect a context-based concurrency limit, making it
easy to control resource usage across an entire call tree.

Results are returned as `iter.Seq` / `iter.Seq2` iterators, so they compose
naturally with the standard library and range loops.

## Usage

### [concurrent.WithLimit](https://pkg.go.dev/github.com/firetiger-oss/concurrent#WithLimit)

Control parallelism via context. The limit propagates through the call tree and
can only be decreased, never increased.

```go
ctx := concurrent.WithLimit(ctx, 4) // at most 4 concurrent operations
```

### [concurrent.Pipeline](https://pkg.go.dev/github.com/firetiger-oss/concurrent#Pipeline)

The core primitive — use when you have an `iter.Seq2` stream and a transform
function. All other APIs are convenience wrappers built on top of Pipeline.

```go
results := concurrent.Pipeline(ctx, inputSeq, func(ctx context.Context, in T) (Out, error) {
    ...
})

for out, err := range results {
    // results arrive in input order
    ...
}
```

### [concurrent.Run](https://pkg.go.dev/github.com/firetiger-oss/concurrent#Run) / [concurrent.RunTasks](https://pkg.go.dev/github.com/firetiger-oss/concurrent#RunTasks)

Convenience wrappers for when your input is a `[]T` slice. `Run` collects
results as an iterator, `RunTasks` for functions that have side-effects but
don't return any errors.

```go
for result, err := range concurrent.Run(ctx, urls,
    func(ctx context.Context, url string) (Result, error) {
        ...
    },
) {
    ...
}
```
```go
err := concurrent.RunTasks(ctx, items, func(ctx context.Context, item Item) error {
    ...
})
```

### [concurrent.Exec](https://pkg.go.dev/github.com/firetiger-oss/concurrent#Exec) / [concurrent.Query](https://pkg.go.dev/github.com/firetiger-oss/concurrent#Query)

Use when you have a small, fixed set of independent functions rather than a
homogeneous slice. `Exec` for error-only tasks, `Query` when each task returns
a value.

```go
for err := range concurrent.Exec(ctx, task1, task2, task3) {
    if err != nil {
        ...
    }
}
```
```go
for result, err := range concurrent.Query(ctx, query1, query2) {
    ...
}
```

### [concurrent.Queue](https://pkg.go.dev/github.com/firetiger-oss/concurrent#Queue) / [concurrent.Process](https://pkg.go.dev/github.com/firetiger-oss/concurrent#Process)

Use for producer-consumer patterns where jobs arrive dynamically over time
rather than being known upfront.

```go
q := concurrent.NewQueue[Result]()

// Producer goroutine
go func() {
    for job := range jobs {
        q.Push(func(ctx context.Context, yield func(Result, error) bool) {
            yield(process(ctx, job))
        })
    }
}()

// Consumer — blocks until queue.Done() is called and all jobs are processed
for result, err := range concurrent.Process(ctx, q) {
    // ...
}
```

## Contributing

Contributions are welcome! To get started:

1. Ensure you have Go 1.25+ installed
2. Run `go test ./...` to verify tests pass

Please report bugs and feature requests via [GitHub Issues](https://github.com/firetiger-oss/concurrent/issues).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
