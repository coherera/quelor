package trace

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

// Writer accepts decision-trace records.
type Writer interface {
	Write(Record) error
	Close() error
}

// JSONLWriter appends one JSON object per line.
// Now, when non-nil, is the clock used to fill an empty TS. Tests inject it.
// Concurrent Write calls are serialized.
type JSONLWriter struct {
	mu     sync.Mutex
	w      io.Writer
	Now    func() time.Time
	closer io.Closer
}

// NewJSONLWriter returns a mutex-protected append-only JSONL writer.
// When SchemaVersion or TS is empty, Write fills schema version "trace.v1"
// and the time from Now (time.Now if Now is nil), formatted in UTC as
// RFC3339Nano. The caller's Record value is not modified.
// Close is a no-op unless the writer was created by OpenFile.
func NewJSONLWriter(w io.Writer) *JSONLWriter {
	return &JSONLWriter{w: w}
}

// OpenFile opens path in append mode with permissions 0600, creating it if
// needed, and returns a JSONLWriter that closes the file on Close.
// The 0600 mode applies on Unix. Windows has no Unix permission bits, so
// there the file inherits the access control list of its directory.
func OpenFile(path string) (*JSONLWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	w := NewJSONLWriter(f)
	w.closer = f
	return w, nil
}

// Write appends rec as one JSON line. An empty schema version or timestamp is filled.
func (w *JSONLWriter) Write(rec Record) error {
	if w == nil || w.w == nil {
		return errors.New("trace: nil writer")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if rec.SchemaVersion == "" {
		rec.SchemaVersion = SchemaVersion
	}
	if rec.TS == "" {
		nowFn := time.Now
		if w.Now != nil {
			nowFn = w.Now
		}
		rec.TS = nowFn().UTC().Format(time.RFC3339Nano)
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	_, err = w.w.Write(b)
	return err
}

// Close closes the file opened by OpenFile. Further and other writers no-op.
func (w *JSONLWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closer == nil {
		return nil
	}
	err := w.closer.Close()
	w.closer = nil
	return err
}

// MemoryWriter collects records in memory. The zero value is ready for use.
// Concurrent Write and Records calls are safe.
type MemoryWriter struct {
	mu      sync.Mutex
	records []Record
}

// Write stores a deep copy of rec. schema_version and ts are not filled here.
func (m *MemoryWriter) Write(rec Record) error {
	if m == nil {
		return errors.New("trace: nil writer")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, cloneRecord(rec))
	return nil
}

// Records returns a deep copy of the records stored so far.
func (m *MemoryWriter) Records() []Record {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, len(m.records))
	for i := range m.records {
		out[i] = cloneRecord(m.records[i])
	}
	return out
}

// Close is a no-op.
func (m *MemoryWriter) Close() error { return nil }

type multiWriter struct {
	ws []Writer
}

// Multi returns a Writer that fans a record out to every writer.
// Every sink is attempted. The returned error is errors.Join of the failures.
func Multi(writers ...Writer) Writer {
	ws := make([]Writer, len(writers))
	copy(ws, writers)
	return &multiWriter{ws: ws}
}

func (m *multiWriter) Write(rec Record) error {
	var errs []error
	for _, w := range m.ws {
		if err := w.Write(rec); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (m *multiWriter) Close() error {
	var errs []error
	for _, w := range m.ws {
		if err := w.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type nopWriter struct{}

// Nop discards every record.
var Nop Writer = nopWriter{}

func (nopWriter) Write(Record) error { return nil }
func (nopWriter) Close() error       { return nil }

var (
	_ Writer = (*JSONLWriter)(nil)
	_ Writer = (*MemoryWriter)(nil)
	_ Writer = (*multiWriter)(nil)
	_ Writer = nopWriter{}
	_ Writer = Nop
)
