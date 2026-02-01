package irzstd

// WriterState is the state of a [Writer].
type WriterState int

const (
	// Ready to accept writes.
	Open WriterState = iota
	// Streams are terminated and [Writer] must be [Reset] before writing again.
	StreamsClosed
	// There was an unrecoverable error and writer is unusable.
	Corrupted
)

var writerStateNames = map[WriterState]string{
	Open:          "Open",
	StreamsClosed: "StreamsClosed",
	Corrupted:     "Corrupted",
}

// Getter for string representation of [WriterState].
//
// Returns:
//   - name: String representation of the state
func (s WriterState) String() string {
	return writerStateNames[s]
}
