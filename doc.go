// Package concurrent provides structured concurrency primitives for Go, built
// on iterators (iter.Seq / iter.Seq2).
//
// All concurrent operations respect a context-based concurrency limit set via
// [WithLimit] and propagate panics safely across goroutine boundaries. Results
// are returned in input order regardless of completion order.
//
// The core primitive is [Pipeline], which applies a transform function to an
// iterator stream using a bounded worker pool. Higher-level helpers—[Exec],
// [Query], [Run], [RunTasks]—cover common patterns like processing slices,
// maps, or fixed sets of independent tasks.
//
// For producer-consumer patterns where jobs arrive dynamically, use [Queue]
// with [Process].
package concurrent
