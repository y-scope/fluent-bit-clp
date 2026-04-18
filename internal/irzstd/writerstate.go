package irzstd

//go:generate stringer -type=WriterState

// WriterState is the state of a [Writer].
type WriterState int

const (
	// Ready to accept writes.
	Open WriterState = iota
	// Streams are terminated and [Writer] must be [Reset] before writing again.
	StreamsClosed
	// There was an unrecoverable error and writer is unusable.
	Corrupted
	// Writer is closed and ready for garbage collection.
	Closed
)
