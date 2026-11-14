package trace

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/quelor"
)

func TestWriteReadRoundTrip(t *testing.T) {
	fixed := time.Date(2026, 10, 31, 1, 2, 3, 4, time.UTC)
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf)
	w.Now = func() time.Time { return fixed }

	full := fullRecord()
	if err := w.Write(full); err != nil {
		t.Fatal(err)
	}
	second := Record{Op: OpExpire, DecisionRef: "e2", TraceID: "t-expire"}
	if err := w.Write(second); err != nil {
		t.Fatal(err)
	}
	if second.SchemaVersion != "" || second.TS != "" {
		t.Fatal("Write mutated the caller record")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := ReadAll(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d", len(got))
	}
	wantFull, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	gotFull, err := json.Marshal(got[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wantFull, gotFull) {
		t.Fatalf("round trip\nwant %s\ngot  %s", wantFull, gotFull)
	}
	if got[1].SchemaVersion != SchemaVersion {
		t.Fatalf("schema_version = %q", got[1].SchemaVersion)
	}
	if got[1].TS != "2026-10-31T01:02:03.000000004Z" {
		t.Fatalf("ts = %q", got[1].TS)
	}
	if got[1].Op != OpExpire || got[1].DecisionRef != "e2" || got[1].TraceID != "t-expire" {
		t.Fatalf("second record %+v", got[1])
	}
	if err := Validate(got[0]); err != nil {
		t.Fatal(err)
	}
	if err := Validate(got[1]); err != nil {
		t.Fatal(err)
	}
}

func TestJSONLWriterDefaultsAndPreserve(t *testing.T) {
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf)
	before := time.Now().Add(-time.Second)
	rec := Record{Op: OpAcquire, DecisionRef: "d"}
	if err := w.Write(rec); err != nil {
		t.Fatal(err)
	}
	after := time.Now().Add(time.Second)
	if rec.SchemaVersion != "" || rec.TS != "" {
		t.Fatal("Write mutated the caller record")
	}
	got, err := ReadAll(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d", len(got))
	}
	if got[0].SchemaVersion != SchemaVersion {
		t.Fatalf("schema_version = %q", got[0].SchemaVersion)
	}
	parsed, err := time.Parse(time.RFC3339Nano, got[0].TS)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Before(before) || parsed.After(after) {
		t.Fatalf("ts %s outside [%s, %s]", got[0].TS, before, after)
	}

	buf.Reset()
	kept := Record{
		SchemaVersion: "trace.v9",
		TS:            "2020-01-02T03:04:05Z",
		Op:            OpRenew,
		DecisionRef:   "kept",
	}
	if err := w.Write(kept); err != nil {
		t.Fatal(err)
	}
	got, err = ReadAll(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].SchemaVersion != "trace.v9" || got[0].TS != "2020-01-02T03:04:05Z" {
		t.Fatalf("pre-set fields overwritten: %+v", got[0])
	}

	buf.Reset()
	env := quelor.Envelope{UnitID: "u", Demands: map[string]int64{"api": 1}}
	snap := Record{Op: OpObserve, DecisionRef: "snap", Envelope: &env}
	if err := w.Write(snap); err != nil {
		t.Fatal(err)
	}
	env.Demands["api"] = 5
	got, err = ReadAll(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Envelope.Demands["api"] != 1 {
		t.Fatalf("demands = %d", got[0].Envelope.Demands["api"])
	}
}

func TestJSONLConcurrentWrites(t *testing.T) {
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)
	wantTS := "2026-01-02T03:04:05.000000006Z"
	var buf bytes.Buffer
	w := NewJSONLWriter(&buf)
	w.Now = func() time.Time { return fixed }

	const n = 64
	var wg sync.WaitGroup
	errs := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			errs <- w.Write(Record{
				Op:          OpObserve,
				DecisionRef: fmt.Sprintf("d-%02d", i),
				TraceID:     fmt.Sprintf("t-%d", i),
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if bytes.Count(buf.Bytes(), []byte{'\n'}) != n {
		t.Fatalf("newlines = %d", bytes.Count(buf.Bytes(), []byte{'\n'}))
	}
	recs, err := ReadAll(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != n {
		t.Fatalf("len = %d", len(recs))
	}
	seen := make(map[string]bool, n)
	for _, rec := range recs {
		if seen[rec.DecisionRef] {
			t.Fatalf("duplicate %s", rec.DecisionRef)
		}
		seen[rec.DecisionRef] = true
		if rec.SchemaVersion != SchemaVersion || rec.TS != wantTS || rec.Op != OpObserve {
			t.Fatalf("record %+v", rec)
		}
		if err := Validate(rec); err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != n {
		t.Fatalf("distinct = %d", len(seen))
	}
}

func TestOpenFileAppendMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	fixed := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	w, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Now = func() time.Time { return fixed }
	if err := w.Write(Record{Op: OpAcquire, DecisionRef: "a"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	// Windows has no Unix permission bits: Go reports 0666 for any writable file.
	if got := fi.Mode().Perm(); runtime.GOOS != "windows" && got != 0o600 {
		t.Fatalf("permissions = %#o", got)
	}

	w2, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	w2.Now = func() time.Time { return fixed }
	if err := w2.Write(Record{Op: OpRenew, DecisionRef: "b"}); err != nil {
		t.Fatal(err)
	}
	if err := w2.Close(); err != nil {
		t.Fatal(err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	recs, err := ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].DecisionRef != "a" || recs[1].DecisionRef != "b" {
		t.Fatalf("records %+v", recs)
	}
}

func TestMemoryWriterConcurrentCopy(t *testing.T) {
	m := &MemoryWriter{}
	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			errs <- m.Write(Record{
				SchemaVersion: SchemaVersion,
				TS:            "2026-10-31T00:00:00Z",
				Op:            OpAcquire,
				DecisionRef:   fmt.Sprintf("m-%d", i),
				Declared:      map[string]int64{"slots": int64(i)},
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	recs := m.Records()
	if len(recs) != n {
		t.Fatalf("len = %d", len(recs))
	}
	seen := map[string]bool{}
	for _, rec := range recs {
		seen[rec.DecisionRef] = true
	}
	if len(seen) != n {
		t.Fatalf("distinct = %d", len(seen))
	}
}

func TestMemoryWriterRecordsCopy(t *testing.T) {
	m := &MemoryWriter{}
	env := quelor.Envelope{UnitID: "u", Demands: map[string]int64{"api": 1}}
	rec := Record{
		DecisionRef: "d",
		Envelope:    &env,
		Verdict:     &quelor.Verdict{Kind: quelor.Defer, Stamps: []string{"s"}, WakeToken: "w"},
		Readings:    []quelor.Reading{{Resource: "api", Available: 1}},
		RuleIDs:     []string{"r"},
		Stamps:      []string{"stamp"},
		Declared:    map[string]int64{"api": 3},
		Consumed:    map[string]int64{"api": 2},
		Outcome:     &Outcome{Class: quelor.Unknown, LogExcerpt: "log", Failed: true},
	}
	if err := m.Write(rec); err != nil {
		t.Fatal(err)
	}
	env.Demands["api"] = 9
	got := m.Records()
	if got[0].Envelope.Demands["api"] != 1 {
		t.Fatalf("stored demands = %d", got[0].Envelope.Demands["api"])
	}
	got[0].Envelope.Demands["api"] = 8
	got[0].Verdict.Stamps[0] = "mut"
	got[0].RuleIDs[0] = "mut"
	got[0].Stamps[0] = "mut"
	got[0].Declared["api"] = 0
	got[0].Consumed["api"] = 0
	got[0].Outcome.LogExcerpt = "mut"
	got[0].Readings[0].Resource = "mut"
	got[0].DecisionRef = "mut"
	again := m.Records()
	if again[0].DecisionRef != "d" ||
		again[0].Envelope.Demands["api"] != 1 ||
		again[0].Verdict.Stamps[0] != "s" ||
		again[0].RuleIDs[0] != "r" ||
		again[0].Stamps[0] != "stamp" ||
		again[0].Declared["api"] != 3 ||
		again[0].Consumed["api"] != 2 ||
		again[0].Outcome.LogExcerpt != "log" ||
		again[0].Readings[0].Resource != "api" {
		t.Fatalf("Records returned an alias: %+v", again[0])
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
}

type failWriter struct{ err error }

func (f failWriter) Write(Record) error { return f.err }
func (f failWriter) Close() error       { return f.err }

type closeCount struct{ n int }

func (c *closeCount) Write(Record) error { return nil }
func (c *closeCount) Close() error       { c.n++; return nil }

func TestMultiAndNop(t *testing.T) {
	fixed := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	var buf bytes.Buffer
	jw := NewJSONLWriter(&buf)
	jw.Now = func() time.Time { return fixed }
	mem := &MemoryWriter{}
	rec := Record{Op: OpRenew, DecisionRef: "r"}
	if err := Multi(jw, mem, Nop).Write(rec); err != nil {
		t.Fatal(err)
	}
	if rec.SchemaVersion != "" {
		t.Fatal("Multi mutated the caller")
	}
	stored := mem.Records()
	if len(stored) != 1 || stored[0].DecisionRef != "r" || stored[0].SchemaVersion != "" {
		t.Fatalf("memory %+v", stored)
	}
	got, err := ReadAll(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SchemaVersion != SchemaVersion || got[0].DecisionRef != "r" {
		t.Fatalf("jsonl %+v", got)
	}
	if got[0].TS != "2026-07-01T00:00:00Z" {
		t.Fatalf("ts = %q", got[0].TS)
	}

	boom := errors.New("boom")
	mem2 := &MemoryWriter{}
	err = Multi(mem2, failWriter{err: boom}).Write(Record{DecisionRef: "x"})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if len(mem2.Records()) != 1 {
		t.Fatal("Multi stopped after the failure")
	}

	c := &closeCount{}
	if err := Multi(c, Nop).Close(); err != nil {
		t.Fatal(err)
	}
	if c.n != 1 {
		t.Fatalf("closes = %d", c.n)
	}
	if err := Nop.Write(Record{}); err != nil {
		t.Fatal(err)
	}
	if err := Nop.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Multi().Write(Record{}); err != nil {
		t.Fatal(err)
	}
	if err := Multi().Close(); err != nil {
		t.Fatal(err)
	}
}

func TestValidate(t *testing.T) {
	ok := minimalRecord()
	tests := []struct {
		name    string
		mutate  func(Record) Record
		wantSub string
	}{
		{name: "minimal", mutate: func(r Record) Record { return r }},
		{name: "missing schema_version", mutate: func(r Record) Record { r.SchemaVersion = ""; return r }, wantSub: "schema_version is required"},
		{name: "bad schema_version", mutate: func(r Record) Record { r.SchemaVersion = "trace.v0"; return r }, wantSub: "schema_version must be"},
		{name: "missing ts", mutate: func(r Record) Record { r.TS = ""; return r }, wantSub: "ts is required"},
		{name: "bad ts", mutate: func(r Record) Record { r.TS = "yesterday"; return r }, wantSub: "ts must be RFC3339Nano"},
		{name: "ts without zone", mutate: func(r Record) Record { r.TS = "2026-10-31T00:00:00"; return r }, wantSub: "ts must be RFC3339Nano"},
		{name: "ts nano", mutate: func(r Record) Record { r.TS = "2026-10-31T00:00:00.123456789Z"; return r }},
		{name: "ts offset", mutate: func(r Record) Record { r.TS = "2026-10-31T00:00:00.1+02:00"; return r }},
		{name: "missing op", mutate: func(r Record) Record { r.Op = ""; return r }, wantSub: "op is required"},
		{name: "bad op", mutate: func(r Record) Record { r.Op = "ACQUIRE"; return r }, wantSub: "op is invalid"},
		{name: "missing decision_ref", mutate: func(r Record) Record { r.DecisionRef = ""; return r }, wantSub: "decision_ref is required"},
		{name: "bad decider_status", mutate: func(r Record) Record { r.DeciderStatus = "maybe"; return r }, wantSub: "decider_status is invalid"},
		{name: "decider none", mutate: func(r Record) Record { r.DeciderStatus = DeciderStatusNone; return r }},
		{name: "bad mode", mutate: func(r Record) Record { r.Mode = "yolo"; return r }, wantSub: "mode is invalid"},
		{name: "mode advise", mutate: func(r Record) Record { r.Mode = ModeAdvise; return r }},
		{name: "bad verdict kind", mutate: func(r Record) Record {
			r.Verdict = &quelor.Verdict{Kind: "admit"}
			return r
		}, wantSub: "verdict.kind is invalid"},
		{name: "verdict admit", mutate: func(r Record) Record {
			r.Verdict = &quelor.Verdict{Kind: quelor.Admit}
			return r
		}},
		{name: "empty verdict kind", mutate: func(r Record) Record {
			r.Verdict = &quelor.Verdict{Reason: "later"}
			return r
		}},
		{name: "bad outcome class", mutate: func(r Record) Record {
			r.Outcome = &Outcome{Class: "flake", Failed: true}
			return r
		}, wantSub: "outcome.class is invalid"},
		{name: "outcome flaky", mutate: func(r Record) Record {
			r.Outcome = &Outcome{Class: quelor.Flaky, ExitCode: 1, Failed: true}
			return r
		}},
		{name: "outcome without class", mutate: func(r Record) Record {
			r.Outcome = &Outcome{ExitCode: 2, Failed: true}
			return r
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.mutate(ok))
			if tt.wantSub == "" {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Fatalf("error type %T", err)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Fatalf("error %q does not contain %q", err, tt.wantSub)
			}
		})
	}
	if err := Validate(fullRecord()); err != nil {
		t.Fatal(err)
	}
}

func TestValidateEnums(t *testing.T) {
	base := minimalRecord()
	for _, op := range []Op{OpAcquire, OpRenew, OpSettle, OpExpire, OpObserve} {
		rec := base
		rec.Op = op
		if err := Validate(rec); err != nil {
			t.Fatalf("%s: %v", op, err)
		}
	}
	for _, st := range []DeciderStatus{
		DeciderStatusAnswered, DeciderStatusAbstained, DeciderStatusUnavailable, DeciderStatusKilled, DeciderStatusNone,
	} {
		rec := base
		rec.DeciderStatus = st
		if err := Validate(rec); err != nil {
			t.Fatalf("%s: %v", st, err)
		}
	}
	for _, mode := range []Mode{ModeLogOnly, ModeAdvise, ModeEnforce} {
		rec := base
		rec.Mode = mode
		if err := Validate(rec); err != nil {
			t.Fatalf("%s: %v", mode, err)
		}
	}
	for _, kind := range []quelor.VerdictKind{quelor.Admit, quelor.Defer, quelor.Shed, quelor.Deny} {
		rec := base
		rec.Verdict = &quelor.Verdict{Kind: kind}
		if err := Validate(rec); err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}
	for _, class := range []quelor.OutcomeClass{
		quelor.ResourceExhausted, quelor.DependencyUnavailable, quelor.Flaky, quelor.RealDefect, quelor.Unknown,
	} {
		rec := base
		rec.Outcome = &Outcome{Class: class, Failed: true}
		if err := Validate(rec); err != nil {
			t.Fatalf("%s: %v", class, err)
		}
	}
}

func TestValidateRequiredSet(t *testing.T) {
	err := Validate(Record{})
	if err == nil {
		t.Fatal("expected error")
	}
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("error type %T", err)
	}
	if len(ve.Problems) != len(RequiredFields()) {
		t.Fatalf("problems %v", ve.Problems)
	}
	for _, f := range RequiredFields() {
		if !strings.Contains(err.Error(), f) {
			t.Fatalf("error %q missing %s", err, f)
		}
		if err := Validate(clearField(t, minimalRecord(), f)); err == nil {
			t.Fatalf("clearing %s accepted", f)
		}
	}
	if err := Validate(minimalRecord()); err != nil {
		t.Fatal(err)
	}
}

func TestHashEnvelope(t *testing.T) {
	e1 := quelor.Envelope{
		UnitID:   "u",
		TenantID: "t",
		HolderID: "h",
		Demands:  map[string]int64{"zeta": 1, "alpha": 2, "mid": 3},
	}
	e2 := quelor.Envelope{
		UnitID:   "u",
		TenantID: "t",
		HolderID: "h",
		Demands:  map[string]int64{"alpha": 2, "mid": 3, "zeta": 1},
	}
	wantJSON := `{"unit_id":"u","tenant_id":"t","holder_id":"h","demands":{"alpha":2,"mid":3,"zeta":1}}`
	raw1, err := json.Marshal(e1)
	if err != nil {
		t.Fatal(err)
	}
	raw2, err := json.Marshal(e2)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw1) != wantJSON || string(raw2) != wantJSON {
		t.Fatalf("canonical json\n%s\n%s", raw1, raw2)
	}
	h1 := HashEnvelope(e1)
	h2 := HashEnvelope(e2)
	if h1 != h2 {
		t.Fatalf("insertion order changed hash %s vs %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("hex len %d", len(h1))
	}
	sum := sha256.Sum256(raw1)
	if hex.EncodeToString(sum[:]) != h1 {
		t.Fatalf("hash %s is not sha256 of canonical json", h1)
	}
	e3 := e1
	e3.Demands = map[string]int64{"alpha": 2, "mid": 4, "zeta": 1}
	if HashEnvelope(e3) == h1 {
		t.Fatal("changed demands hashed equal")
	}
}

func TestReadAllBlankLinesAndErrors(t *testing.T) {
	line, err := json.Marshal(minimalRecord())
	if err != nil {
		t.Fatal(err)
	}
	in := "\n\n  " + string(line) + "  \n\n   \n" + string(line)
	got, err := ReadAll(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d", len(got))
	}
	if got[0].DecisionRef != "d1" || got[1].DecisionRef != "d1" {
		t.Fatalf("%+v", got)
	}

	_, err = ReadAll(strings.NewReader("\n" + string(line) + "\n{bad\n"))
	if err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("err = %v", err)
	}
	_, err = ReadAll(strings.NewReader("\n\n{\n"))
	if err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("err = %v", err)
	}
	got, err = ReadAll(strings.NewReader(""))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("len = %d", len(got))
	}
}

func TestReadAllLineLimit(t *testing.T) {
	big := strings.Repeat("a", 128*1024)
	rec := minimalRecord()
	rec.Op = OpSettle
	rec.Outcome = &Outcome{LogExcerpt: big, Failed: true}
	b, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) <= 64*1024 {
		t.Fatalf("line length %d does not exceed the default scanner buffer", len(b))
	}
	got, err := ReadAll(bytes.NewReader(append(b, '\n')))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Outcome == nil || got[0].Outcome.LogExcerpt != big {
		t.Fatal("large line mismatch")
	}

	tooBig := strings.Repeat("x", maxLineSize+8) + "\n"
	_, err = ReadAll(strings.NewReader(tooBig))
	if err == nil {
		t.Fatal("expected too-long line error")
	}
	if !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("error %v missing line number", err)
	}
	if !errors.Is(err, bufio.ErrTooLong) {
		t.Fatalf("err = %v", err)
	}
}

func TestSchemaAgreesWithRecord(t *testing.T) {
	data, err := os.ReadFile("../../schema/trace.v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Schema               string                     `json:"$schema"`
		ID                   string                     `json:"$id"`
		Type                 string                     `json:"type"`
		AdditionalProperties *bool                      `json:"additionalProperties"`
		Required             []string                   `json:"required"`
		Properties           map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(doc.Schema, "2020-12") {
		t.Fatalf("$schema = %s", doc.Schema)
	}
	if doc.ID != "https://quelor.dev/schema/trace.v1.json" {
		t.Fatalf("$id = %s", doc.ID)
	}
	if doc.Type != "object" {
		t.Fatalf("type = %s", doc.Type)
	}
	if doc.AdditionalProperties == nil || *doc.AdditionalProperties {
		t.Fatalf("additionalProperties = %v", doc.AdditionalProperties)
	}
	if !sameStringSet(doc.Required, RequiredFields()) {
		t.Fatalf("schema required %v, Validate required %v", doc.Required, RequiredFields())
	}

	tags := recordPropertyNames(t)
	if len(tags) != len(doc.Properties) {
		t.Fatalf("struct has %d json names, schema has %d properties", len(tags), len(doc.Properties))
	}
	for name := range tags {
		if _, ok := doc.Properties[name]; !ok {
			t.Errorf("struct json name %q missing from schema properties", name)
		}
	}
	for name := range doc.Properties {
		if _, ok := tags[name]; !ok {
			t.Errorf("schema property %q missing from Record", name)
		}
	}

	rec := fullRecord()
	if err := Validate(rec); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		t.Fatal(err)
	}
	for name := range obj {
		if _, ok := doc.Properties[name]; !ok {
			t.Errorf("marshalled record has undeclared key %q", name)
		}
	}
	for name := range doc.Properties {
		if _, ok := obj[name]; !ok {
			t.Errorf("full record omitted schema property %q", name)
		}
	}
}

func recordPropertyNames(t *testing.T) map[string]struct{} {
	t.Helper()
	rt := reflect.TypeOf(Record{})
	names := make(map[string]struct{}, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		tag := f.Tag.Get("json")
		if tag == "" {
			t.Fatalf("field %s missing json tag", f.Name)
		}
		name, _, _ := strings.Cut(tag, ",")
		if name == "" || name == "-" {
			t.Fatalf("field %s has unusable json tag %q", f.Name, tag)
		}
		if _, dup := names[name]; dup {
			t.Fatalf("duplicate json name %s", name)
		}
		names[name] = struct{}{}
	}
	return names
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	n := make(map[string]int, len(a))
	for _, s := range a {
		n[s]++
	}
	for _, s := range b {
		n[s]--
	}
	for _, c := range n {
		if c != 0 {
			return false
		}
	}
	return true
}

func clearField(t *testing.T, rec Record, field string) Record {
	t.Helper()
	switch field {
	case "schema_version":
		rec.SchemaVersion = ""
	case "ts":
		rec.TS = ""
	case "op":
		rec.Op = ""
	case "decision_ref":
		rec.DecisionRef = ""
	default:
		t.Fatalf("unknown required field %s", field)
	}
	return rec
}

func minimalRecord() Record {
	return Record{
		SchemaVersion: SchemaVersion,
		TS:            "2026-10-31T00:00:00Z",
		Op:            OpAcquire,
		DecisionRef:   "d1",
	}
}

func fullRecord() Record {
	deadline := time.Date(2026, 10, 31, 18, 0, 0, 0, time.UTC)
	env := sampleEnvelope(deadline)
	wake := deadline.Add(time.Minute)
	return Record{
		SchemaVersion: SchemaVersion,
		TS:            "2026-10-31T17:00:00.123456789Z",
		Op:            OpSettle,
		DecisionRef:   "dec-1",
		TraceID:       "trace-1",
		EnvelopeHash:  HashEnvelope(env),
		Envelope:      &env,
		HolderID:      "holder-1",
		TenantID:      "tenant-1",
		LeaseID:       "lease-1",
		Cell:          "cell-a",
		Verdict: &quelor.Verdict{
			Kind:      quelor.Admit,
			Reason:    "ok",
			WakeAt:    &wake,
			WakeToken: "wt",
			Hibernate: true,
			Stamps:    []string{"ruleset-moved"},
		},
		Readings: []quelor.Reading{{
			Resource:   "api",
			Available:  9,
			Capacity:   10,
			Known:      true,
			Stale:      false,
			ObservedAt: deadline,
			AgeMS:      12,
		}},
		RulesetID:   "rs",
		RulesetHash: "abc",
		DeciderID:   "decider-local",
		RuleIDs:     []string{"r1", "r2"},
		Provenance: Provenance{
			Step:          "admit",
			Tier:          "t1",
			EvidenceClass: "hard",
		},
		DeciderStatus:   DeciderStatusAnswered,
		Mode:            ModeEnforce,
		Stamps:          []string{"bound-unreachable"},
		DirectiveSigner: "signer-1",
		Declared:        map[string]int64{"api": 2},
		Consumed:        map[string]int64{"api": 1},
		Outcome: &Outcome{
			Class:      quelor.Flaky,
			ExitCode:   1,
			HTTPStatus: 503,
			LogExcerpt: "boom <html>",
			Failed:     true,
		},
		LatencyMS: 12.5,
	}
}

func sampleEnvelope(deadline time.Time) quelor.Envelope {
	return quelor.Envelope{
		UnitID:         "unit-1",
		TenantID:       "tenant-1",
		HolderID:       "holder-1",
		Demands:        map[string]int64{"gpu": 1, "api": 2},
		AffinityKey:    "aff",
		CoalesceKey:    "co-1",
		CoalescePolicy: quelor.CoalesceSupersede,
		Generation:     7,
		Priority:       quelor.PriorityReleaseCritical,
		Deadline:       &deadline,
	}
}
