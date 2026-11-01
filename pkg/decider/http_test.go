package decider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/quelor"
)

func TestEffectiveClientTimeout(t *testing.T) {
	if defaultClientTimeout != 250*time.Millisecond {
		t.Fatalf("default %s", defaultClientTimeout)
	}
	cases := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{name: "zero", in: 0, want: defaultClientTimeout},
		{name: "negative", in: -time.Second, want: defaultClientTimeout},
		{name: "set", in: 12 * time.Millisecond, want: 12 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &HTTPClient{Timeout: tc.in}
			if c.effectiveTimeout() != tc.want {
				t.Fatalf("got %s", c.effectiveTimeout())
			}
		})
	}
	bare := &HTTPClient{}
	if bare.httpClient().Timeout != defaultClientTimeout {
		t.Fatalf("nil client timeout %s", bare.httpClient().Timeout)
	}
	bare.Timeout = 12 * time.Millisecond
	if bare.httpClient().Timeout != 12*time.Millisecond {
		t.Fatal(bare.httpClient().Timeout)
	}
	custom := &http.Client{Timeout: time.Second}
	bare.HTTP = custom
	if bare.httpClient() != custom {
		t.Fatal("provided client not used")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHTTPClientContextDeadline(t *testing.T) {
	cases := []struct {
		name    string
		timeout time.Duration
		min     time.Duration
		max     time.Duration
	}{
		{name: "default", timeout: 0, min: 200 * time.Millisecond, max: defaultClientTimeout},
		{name: "custom", timeout: 40 * time.Millisecond, min: 20 * time.Millisecond, max: 40 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var fail string
			c := &HTTPClient{
				URL:       "http://example.test/decide",
				Timeout:   tc.timeout,
				DeciderID: "client-decider",
				HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.Method != http.MethodPost {
						fail = "method " + r.Method
					}
					if r.Header.Get("Content-Type") != "application/json" {
						fail = "content-type " + r.Header.Get("Content-Type")
					}
					if r.Header.Get("X-Quelor-Decider-API") != "1" {
						fail = "api header " + r.Header.Get("X-Quelor-Decider-API")
					}
					dl, ok := r.Context().Deadline()
					if !ok {
						fail = "no deadline"
					} else {
						remain := time.Until(dl)
						if remain < tc.min || remain > tc.max+time.Millisecond {
							fail = remain.String()
						}
					}
					body := `{"decider_api_version":"1","status":"answered","verdict":{"kind":"ADMIT"},"decider_id":"rt","latency_ms":1}`
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     make(http.Header),
						Body:       io.NopCloser(strings.NewReader(body)),
					}, nil
				})},
			}
			resp, err := c.Decide(context.Background(), Request{Op: OpAcquire, Envelope: quelor.Envelope{UnitID: "u"}})
			if fail != "" {
				t.Fatal(fail)
			}
			if err != nil {
				t.Fatal(err)
			}
			if resp.Status != StatusAnswered || resp.DeciderID != "rt" || resp.LatencyMS != 1 {
				t.Fatalf("%#v", resp)
			}
		})
	}
}

func TestHTTPClientTransportCases(t *testing.T) {
	boom := errors.New("dial failed")
	base := `{"decider_api_version":"1","status":"answered","verdict":{"kind":"ADMIT"},"decider_id":"pad","latency_ms":1,"decision_ref":"abc"}`
	exact := base + strings.Repeat(" ", maxBodyBytes-len(base))
	if len(exact) != maxBodyBytes {
		t.Fatalf("pad len %d", len(exact))
	}
	over := base + strings.Repeat(" ", maxBodyBytes)

	type transportCase struct {
		name       string
		status     int
		body       string
		mode       string
		wantErr    bool
		wantStatus Status
		wantKind   quelor.VerdictKind
		wantID     string
		wantLat    float64
		fillLat    bool
		errHas     string
		notErrHas  string
		sentinel   error
	}
	cases := []transportCase{
		{name: "admit", status: 200, body: `{"decider_api_version":"1","status":"answered","mode":"enforce","verdict":{"kind":"ADMIT","reason":"ok"},"decision_ref":"abc","decider_id":"side","latency_ms":2.5}`, wantStatus: StatusAnswered, wantKind: quelor.Admit, wantID: "side", wantLat: 2.5},
		{name: "empty-version", status: 200, body: `{"status":"abstained","decider_id":"abs","latency_ms":1}`, wantStatus: StatusAbstained, wantID: "abs", wantLat: 1},
		{name: "zero-latency", status: 200, body: `{"decider_api_version":"1","status":"answered","verdict":{"kind":"SHED"},"decider_id":"z"}`, wantStatus: StatusAnswered, wantKind: quelor.Shed, wantID: "z", fillLat: true},
		{name: "wrong-version", status: 200, body: `{"decider_api_version":"2","status":"answered","verdict":{"kind":"ADMIT"},"decider_id":"old","latency_ms":4}`, wantErr: true, wantStatus: StatusUnavailable, wantID: "client-decider", errHas: `"2"`},
		{name: "version-with-space", status: 200, body: `{"decider_api_version":"1 ","status":"answered","verdict":{"kind":"ADMIT"}}`, wantErr: true, wantStatus: StatusUnavailable, wantID: "client-decider", errHas: "decider_api_version"},
		{name: "garbage", status: 200, body: `{not json`, wantErr: true, wantStatus: StatusUnavailable, wantID: "client-decider", errHas: "decode response"},
		{name: "http-500", status: 500, body: `nope`, wantErr: true, wantStatus: StatusUnavailable, wantID: "client-decider", errHas: "HTTP 500"},
		{name: "http-503", status: 503, body: `{"error":"backend down"}`, wantErr: true, wantStatus: StatusUnavailable, wantID: "client-decider", errHas: "backend down"},
		{name: "exact-cap", status: 200, body: exact, wantStatus: StatusAnswered, wantKind: quelor.Admit, wantID: "pad", wantLat: 1},
		{name: "over-cap", status: 200, body: over, wantErr: true, wantStatus: StatusUnavailable, wantID: "client-decider", errHas: "1MiB"},
		{name: "under-cap-garbage", status: 200, body: strings.Repeat("{", maxBodyBytes), wantErr: true, wantStatus: StatusUnavailable, errHas: "decode", notErrHas: "1MiB"},
		{name: "transport", mode: "error", wantErr: true, wantStatus: StatusUnavailable, wantID: "client-decider", errHas: "dial failed", sentinel: boom},
		{name: "block", mode: "block", wantErr: true, wantStatus: StatusUnavailable, wantID: "client-decider", sentinel: context.DeadlineExceeded},
		{name: "bad-url", mode: "bad-url", wantErr: true, wantStatus: StatusUnavailable, wantID: "client-decider"},
		{name: "nil-client", mode: "nil-receiver", wantErr: true, wantStatus: StatusUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &HTTPClient{
				URL:       "http://example.test/decide",
				Timeout:   30 * time.Millisecond,
				DeciderID: "client-decider",
				HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					raw, _ := io.ReadAll(r.Body)
					var got Request
					if err := json.Unmarshal(raw, &got); err != nil {
						return nil, err
					}
					if got.Envelope.UnitID != "u-transport" || got.Op != OpAcquire {
						return nil, fmt.Errorf("request %#v", got)
					}
					switch tc.mode {
					case "error":
						return nil, boom
					case "block":
						<-r.Context().Done()
						return nil, r.Context().Err()
					default:
						return &http.Response{
							StatusCode: tc.status,
							Header:     make(http.Header),
							Body:       io.NopCloser(strings.NewReader(tc.body)),
						}, nil
					}
				})},
			}
			if tc.mode == "bad-url" {
				c.URL = "://bad"
			}
			call := c
			if tc.mode == "nil-receiver" {
				call = nil
			}
			resp, err := call.Decide(context.Background(), Request{Op: OpAcquire, Envelope: quelor.Envelope{UnitID: "u-transport"}})
			if tc.wantErr {
				if !errors.Is(err, ErrUnavailable) {
					t.Fatalf("err %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if resp.Status != tc.wantStatus {
				t.Fatalf("status %q err %v", resp.Status, err)
			}
			if tc.wantKind != "" && resp.Verdict.Kind != tc.wantKind {
				t.Fatalf("kind %q", resp.Verdict.Kind)
			}
			if tc.wantErr && resp.Verdict.Kind == quelor.Admit {
				t.Fatal("admit leaked into unavailable response")
			}
			if tc.wantID != "" && resp.DeciderID != tc.wantID {
				t.Fatalf("id %q", resp.DeciderID)
			}
			if tc.mode == "nil-receiver" && resp.DeciderID != "" {
				t.Fatalf("id %q", resp.DeciderID)
			}
			if tc.wantLat != 0 && resp.LatencyMS != tc.wantLat {
				t.Fatalf("latency %v", resp.LatencyMS)
			}
			if tc.fillLat && resp.LatencyMS <= 0 {
				t.Fatalf("latency not filled %v", resp.LatencyMS)
			}
			if tc.errHas != "" && (err == nil || !strings.Contains(err.Error(), tc.errHas)) {
				t.Fatalf("err %v", err)
			}
			if tc.notErrHas != "" && err != nil && strings.Contains(err.Error(), tc.notErrHas) {
				t.Fatalf("err %v", err)
			}
			if tc.sentinel != nil && !errors.Is(err, tc.sentinel) {
				t.Fatalf("sentinel %v", err)
			}
			if tc.wantErr && resp.DeciderAPIVersion != APIVersion && tc.mode != "nil-receiver" {
				t.Fatalf("version %q", resp.DeciderAPIVersion)
			}
		})
	}
}

type headerSnap struct {
	mu   sync.Mutex
	ct   string
	api  string
	meth string
}

func (s *headerSnap) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ct = r.Header.Get("Content-Type")
	s.api = r.Header.Get("X-Quelor-Decider-API")
	s.meth = r.Method
}

func (s *headerSnap) assert(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.meth != http.MethodPost {
		t.Fatalf("method %s", s.meth)
	}
	if s.ct != "application/json" {
		t.Fatalf("content-type %q", s.ct)
	}
	if s.api != "1" {
		t.Fatalf("api header %q", s.api)
	}
}

func serve(t *testing.T, h http.Handler) (*httptest.Server, *headerSnap) {
	t.Helper()
	snap := &headerSnap{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snap.record(r)
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, snap
}

func TestHTTPRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		fn   func(*testing.T)
	}{
		{name: "static-defer", fn: roundTripStaticDefer},
		{name: "abstained", fn: roundTripAbstained},
		{name: "killed-log-only", fn: roundTripKilled},
		{name: "renew", fn: roundTripRenew},
		{name: "server-500", fn: roundTripServer500},
		{name: "timeout", fn: roundTripTimeout},
		{name: "garbage", fn: roundTripGarbage},
		{name: "wrong-api-version", fn: roundTripWrongVersion},
		{name: "decider-error-503", fn: roundTripDeciderError},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.fn)
	}
}

func roundTripStaticDefer(t *testing.T) {
	req, check := acquireFixture()
	wake := time.Date(2026, 12, 2, 3, 4, 5, 0, time.UTC)
	verdict := quelor.Verdict{
		Kind:      quelor.Defer,
		Reason:    "budget",
		WakeAt:    &wake,
		WakeToken: "wtok",
		Hibernate: true,
		Stamps:    []string{"bound-unreachable"},
	}
	d := validating(Static(verdict, "side-1"), check)
	srv, snap := serve(t, Handler(d))
	c := &HTTPClient{URL: srv.URL, Timeout: time.Second, DeciderID: "client-decider"}
	resp, err := c.Decide(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	snap.assert(t)
	if resp.Status != StatusAnswered || resp.Mode != ModeEnforce || !resp.Enforced() {
		t.Fatalf("%#v", resp)
	}
	if resp.DeciderID != "side-1" || resp.TraceID != req.Meta.TraceID || resp.DeciderAPIVersion != APIVersion {
		t.Fatalf("%#v", resp)
	}
	if resp.DecisionRef != decisionRef(OpAcquire, "unit-9", quelor.Defer) {
		t.Fatalf("ref %q", resp.DecisionRef)
	}
	if !verdictEqual(resp.Verdict, verdict) {
		t.Fatalf("verdict %#v", resp.Verdict)
	}
	if resp.LatencyMS <= 0 {
		t.Fatalf("latency %v", resp.LatencyMS)
	}
}

func roundTripAbstained(t *testing.T) {
	req, check := acquireFixture()
	obs := time.Date(2026, 10, 31, 4, 5, 6, 0, time.UTC)
	var want Response
	want.DeciderAPIVersion = APIVersion
	want.Status = StatusAbstained
	want.Mode = ModeAdvise
	want.DecisionRef = "abs-ref"
	want.RulesetID = "rs"
	want.RulesetHash = "hash"
	want.RuleIDs = []string{"r1", "r2"}
	want.Abstains = []string{"no-rule", "stale-reading"}
	want.Provenance = Provenance{Step: "gate", Tier: "t1", EvidenceClass: "live"}
	want.TraceID = "tr-1"
	want.LatencyMS = 6.5
	want.DeciderID = "abs-1"
	want.Verdict.Reason = "no matching rule"
	want.Readings = []quelor.Reading{{
		Resource:   "gpu",
		Available:  3,
		Capacity:   4,
		Known:      true,
		ObservedAt: obs,
		AgeMS:      12,
	}}
	d := validating(Func(func(_ context.Context, got Request) (Response, error) {
		if err := check(got); err != nil {
			return Response{}, err
		}
		return want, nil
	}), nil)
	srv, snap := serve(t, Handler(d))
	c := &HTTPClient{URL: srv.URL, Timeout: time.Second, DeciderID: "client-decider"}
	resp, err := c.Decide(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	snap.assert(t)
	if err := responsesEqual(resp, want); err != nil {
		t.Fatal(err)
	}
	if resp.Enforced() {
		t.Fatal("abstain enforced")
	}
}

func roundTripKilled(t *testing.T) {
	req, check := acquireFixture()
	var want Response
	want.DeciderAPIVersion = APIVersion
	want.Status = StatusKilled
	want.Mode = ModeLogOnly
	want.DeciderID = "killed-1"
	want.DecisionRef = "k1"
	want.LatencyMS = 1
	want.TraceID = req.Meta.TraceID
	d := validating(Func(func(_ context.Context, got Request) (Response, error) {
		if err := check(got); err != nil {
			return Response{}, err
		}
		return want, nil
	}), nil)
	srv, snap := serve(t, Handler(d))
	resp, err := (&HTTPClient{URL: srv.URL, Timeout: time.Second, DeciderID: "client-decider"}).Decide(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	snap.assert(t)
	if err := responsesEqual(resp, want); err != nil {
		t.Fatal(err)
	}
	if resp.Enforced() {
		t.Fatal("log-only enforced")
	}
}

func roundTripRenew(t *testing.T) {
	req, check := renewFixture()
	d := validating(Static(quelor.Verdict{Kind: quelor.Admit, Reason: "extend"}, "renew-1"), check)
	srv, snap := serve(t, Handler(d))
	resp, err := (&HTTPClient{URL: srv.URL, Timeout: time.Second, DeciderID: "client-decider"}).Decide(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	snap.assert(t)
	if resp.Verdict.Kind != quelor.Admit || resp.Verdict.Reason != "extend" || resp.DeciderID != "renew-1" {
		t.Fatalf("%#v", resp)
	}
	if resp.DecisionRef != decisionRef(OpRenew, "unit-9", quelor.Admit) {
		t.Fatalf("ref %q", resp.DecisionRef)
	}
	if resp.LatencyMS <= 0 {
		t.Fatalf("latency %v", resp.LatencyMS)
	}
}

func roundTripServer500(t *testing.T) {
	srv, snap := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	resp, err := (&HTTPClient{URL: srv.URL, Timeout: time.Second, DeciderID: "client-decider"}).Decide(context.Background(), Request{Op: OpAcquire})
	snap.assert(t)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if resp.Status != StatusUnavailable || resp.DeciderID != "client-decider" {
		t.Fatalf("%#v", resp)
	}
	if !strings.Contains(err.Error(), "HTTP 500") {
		t.Fatal(err)
	}
}

func roundTripTimeout(t *testing.T) {
	srv, _ := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(300 * time.Millisecond):
		}
	}))
	c := &HTTPClient{
		URL:       srv.URL,
		Timeout:   20 * time.Millisecond,
		DeciderID: "client-decider",
		HTTP:      &http.Client{},
	}
	start := time.Now()
	resp, err := c.Decide(context.Background(), Request{Op: OpAcquire, Envelope: quelor.Envelope{UnitID: "slow"}})
	elapsed := time.Since(start)
	if elapsed > 150*time.Millisecond {
		t.Fatalf("elapsed %s", elapsed)
	}
	if !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	if resp.Status != StatusUnavailable || resp.DeciderID != "client-decider" {
		t.Fatalf("%#v", resp)
	}
}

func roundTripGarbage(t *testing.T) {
	srv, _ := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{not json"))
	}))
	resp, err := (&HTTPClient{URL: srv.URL, Timeout: time.Second, DeciderID: "client-decider"}).Decide(context.Background(), Request{})
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "decode response") {
		t.Fatal(err)
	}
	if resp.Status != StatusUnavailable || resp.DeciderID != "client-decider" {
		t.Fatalf("%#v", resp)
	}
}

func roundTripWrongVersion(t *testing.T) {
	d := Func(func(context.Context, Request) (Response, error) {
		var r Response
		r.DeciderAPIVersion = "9"
		r.Status = StatusAnswered
		r.Verdict.Kind = quelor.Admit
		r.DeciderID = "old"
		r.LatencyMS = 3
		return r, nil
	})
	srv, snap := serve(t, Handler(d))
	resp, err := (&HTTPClient{URL: srv.URL, Timeout: time.Second, DeciderID: "client-decider"}).Decide(context.Background(), Request{Op: OpAcquire})
	snap.assert(t)
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), `"9"`) {
		t.Fatal(err)
	}
	if resp.Status != StatusUnavailable || resp.DeciderID != "client-decider" || resp.Verdict.Kind != "" {
		t.Fatalf("%#v", resp)
	}
}

func roundTripDeciderError(t *testing.T) {
	d := Func(func(context.Context, Request) (Response, error) {
		return Response{}, errors.New("backend down")
	})
	srv, snap := serve(t, Handler(d))
	resp, err := (&HTTPClient{URL: srv.URL, Timeout: time.Second, DeciderID: "client-decider"}).Decide(context.Background(), Request{Op: OpAcquire})
	snap.assert(t)
	if !errors.Is(err, ErrUnavailable) || !strings.Contains(err.Error(), "HTTP 503") || !strings.Contains(err.Error(), "backend down") {
		t.Fatal(err)
	}
	if resp.Status != StatusUnavailable || resp.DeciderID != "client-decider" {
		t.Fatalf("%#v", resp)
	}
}

func TestHTTPHandlerStatus(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		body       string
		oversize   bool
		deciderErr error
		nilDecider bool
		want       int
		wantBody   string
		wantAllow  bool
		wantID     string
	}{
		{name: "get", method: http.MethodGet, body: `{}`, want: http.StatusMethodNotAllowed, wantAllow: true},
		{name: "put", method: http.MethodPut, body: `{}`, want: http.StatusMethodNotAllowed, wantAllow: true},
		{name: "patch", method: http.MethodPatch, body: `{}`, want: http.StatusMethodNotAllowed, wantAllow: true},
		{name: "delete", method: http.MethodDelete, body: `{}`, want: http.StatusMethodNotAllowed, wantAllow: true},
		{name: "head", method: http.MethodHead, want: http.StatusMethodNotAllowed, wantAllow: true},
		{name: "empty", method: http.MethodPost, body: ``, want: http.StatusBadRequest, wantBody: "invalid json"},
		{name: "truncated", method: http.MethodPost, body: `{"op":`, want: http.StatusBadRequest, wantBody: "invalid json"},
		{name: "array", method: http.MethodPost, body: `[]`, want: http.StatusBadRequest, wantBody: "invalid json"},
		{name: "oversize", method: http.MethodPost, oversize: true, want: http.StatusBadRequest, wantBody: "1MiB"},
		{name: "decider-error", method: http.MethodPost, body: `{"op":"acquire"}`, deciderErr: errors.New("backend down"), want: http.StatusServiceUnavailable, wantBody: "backend down"},
		{name: "nil-decider", method: http.MethodPost, body: `{"op":"acquire"}`, nilDecider: true, want: http.StatusServiceUnavailable, wantBody: "nil decider"},
		{name: "null-body", method: http.MethodPost, body: `null`, want: http.StatusOK, wantID: "srv"},
		{name: "ok", method: http.MethodPost, body: `{"decider_api_version":"1","op":"acquire","envelope":{"unit_id":"u-h"},"call_meta":{"trace_id":"tr-h"}}`, want: http.StatusOK, wantID: "srv"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.body
			if tc.oversize {
				body = strings.Repeat("a", maxBodyBytes+1)
			}
			var d Decider
			if !tc.nilDecider {
				d = Func(func(_ context.Context, req Request) (Response, error) {
					if tc.deciderErr != nil {
						return Response{}, tc.deciderErr
					}
					var r Response
					r.DeciderAPIVersion = APIVersion
					r.Status = StatusAnswered
					r.Verdict.Kind = quelor.Admit
					r.DecisionRef = "d1"
					r.DeciderID = "srv"
					r.TraceID = req.Meta.TraceID
					if req.Envelope.UnitID != "" {
						r.Verdict.Reason = req.Envelope.UnitID
					}
					return r, nil
				})
			}
			rec := httptest.NewRecorder()
			httpReq := httptest.NewRequest(tc.method, "http://sidecar.test/decide", strings.NewReader(body))
			Handler(d).ServeHTTP(rec, httpReq)
			if rec.Code != tc.want {
				t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
			}
			if tc.wantAllow && rec.Header().Get("Allow") != http.MethodPost {
				t.Fatalf("allow %q", rec.Header().Get("Allow"))
			}
			if tc.wantBody != "" && !strings.Contains(rec.Body.String(), tc.wantBody) {
				t.Fatalf("body %s", rec.Body.String())
			}
			if tc.want == http.StatusServiceUnavailable {
				if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
					t.Fatalf("content-type %q", ct)
				}
				var payload struct {
					Error string `json:"error"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(payload.Error, tc.wantBody) {
					t.Fatalf("payload %#v", payload)
				}
			}
			if tc.want == http.StatusOK {
				if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
					t.Fatalf("content-type %q", ct)
				}
				var resp Response
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatal(err)
				}
				if resp.Status != StatusAnswered || resp.DeciderID != "srv" || resp.Verdict.Kind != quelor.Admit {
					t.Fatalf("%#v", resp)
				}
				if tc.name == "ok" && (resp.TraceID != "tr-h" || resp.Verdict.Reason != "u-h") {
					t.Fatalf("%#v", resp)
				}
			}
		})
	}
}

func validating(inner Decider, check func(Request) error) Decider {
	if check == nil {
		return inner
	}
	return Func(func(ctx context.Context, req Request) (Response, error) {
		if err := check(req); err != nil {
			return Response{}, err
		}
		return inner.Decide(ctx, req)
	})
}

func acquireFixture() (Request, func(Request) error) {
	deadline := time.Date(2026, 11, 1, 8, 30, 0, 0, time.UTC)
	obs := time.Date(2026, 10, 31, 1, 2, 3, 0, time.UTC)
	var req Request
	req.DeciderAPIVersion = APIVersion
	req.Op = OpAcquire
	req.Envelope.UnitID = "unit-9"
	req.Envelope.TenantID = "ten"
	req.Envelope.HolderID = "hold"
	req.Envelope.Demands = map[string]int64{"gpu": 2, "tokens": 9007199254740993}
	req.Envelope.AffinityKey = "aff"
	req.Envelope.CoalesceKey = "ck"
	req.Envelope.CoalescePolicy = quelor.CoalesceDropNew
	req.Envelope.Generation = 7
	req.Envelope.Priority = quelor.PriorityReleaseCritical
	req.Envelope.Deadline = &deadline
	req.SpendDelta = map[string]int64{"usd_cents": 5}
	req.Readings = []quelor.Reading{
		{Resource: "gpu", Available: 1, Capacity: 8, Known: true, ObservedAt: obs, AgeMS: 40},
		{Resource: "api", Available: 0, Capacity: 0, Known: false, Stale: true, ObservedAt: obs, AgeMS: 99},
	}
	req.PolicyProfile = "default"
	req.ExpectRulesetHash = "hsh"
	req.Meta.TraceID = "tr-1"
	req.Meta.Now = "2026-10-31T00:00:00Z"
	want := req
	return req, func(got Request) error { return requestsEqual(got, want) }
}

func renewFixture() (Request, func(Request) error) {
	req, _ := acquireFixture()
	req.Op = OpRenew
	granted := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	expires := time.Date(2026, 10, 31, 1, 0, 0, 0, time.UTC)
	req.Lease = &quelor.Lease{
		ID:          "lease-1",
		UnitID:      "unit-9",
		TenantID:    "ten",
		HolderID:    "hold",
		Declared:    map[string]int64{"gpu": 2},
		GrantedAt:   granted,
		ExpiresAt:   expires,
		DecisionRef: "prev",
		Renewals:    2,
	}
	want := req
	return req, func(got Request) error { return requestsEqual(got, want) }
}

func requestsEqual(got, want Request) error {
	if got.DeciderAPIVersion != want.DeciderAPIVersion || got.Op != want.Op || got.PolicyProfile != want.PolicyProfile || got.ExpectRulesetHash != want.ExpectRulesetHash || got.Meta != want.Meta {
		return fmt.Errorf("scalar got %#v want %#v", got, want)
	}
	if !envelopeEqual(got.Envelope, want.Envelope) {
		return fmt.Errorf("envelope got %#v want %#v", got.Envelope, want.Envelope)
	}
	if !reflect.DeepEqual(got.SpendDelta, want.SpendDelta) {
		return fmt.Errorf("spend got %v want %v", got.SpendDelta, want.SpendDelta)
	}
	if !readingsEqual(got.Readings, want.Readings) {
		return fmt.Errorf("readings got %#v want %#v", got.Readings, want.Readings)
	}
	if !leaseEqual(got.Lease, want.Lease) {
		return fmt.Errorf("lease got %#v want %#v", got.Lease, want.Lease)
	}
	return nil
}

func envelopeEqual(got, want quelor.Envelope) bool {
	if (got.Deadline == nil) != (want.Deadline == nil) {
		return false
	}
	if got.Deadline != nil && !got.Deadline.Equal(*want.Deadline) {
		return false
	}
	got.Deadline = nil
	want.Deadline = nil
	return reflect.DeepEqual(got, want)
}

func readingsEqual(got, want []quelor.Reading) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		g, w := got[i], want[i]
		gt, wt := g.ObservedAt, w.ObservedAt
		g.ObservedAt = time.Time{}
		w.ObservedAt = time.Time{}
		if g != w || !gt.Equal(wt) {
			return false
		}
	}
	return true
}

func leaseEqual(got, want *quelor.Lease) bool {
	if (got == nil) != (want == nil) {
		return false
	}
	if got == nil {
		return true
	}
	g, w := *got, *want
	if !g.GrantedAt.Equal(w.GrantedAt) || !g.ExpiresAt.Equal(w.ExpiresAt) {
		return false
	}
	g.GrantedAt, w.GrantedAt = time.Time{}, time.Time{}
	g.ExpiresAt, w.ExpiresAt = time.Time{}, time.Time{}
	return reflect.DeepEqual(g, w)
}

func responsesEqual(got, want Response) error {
	if got.DeciderAPIVersion != want.DeciderAPIVersion || got.Status != want.Status || got.Mode != want.Mode || got.DecisionRef != want.DecisionRef || got.RulesetID != want.RulesetID || got.RulesetHash != want.RulesetHash || got.TraceID != want.TraceID || got.DeciderID != want.DeciderID || got.LatencyMS != want.LatencyMS || got.Provenance != want.Provenance {
		return fmt.Errorf("scalar\n got %#v\nwant %#v", got, want)
	}
	if !verdictEqual(got.Verdict, want.Verdict) {
		return fmt.Errorf("verdict\n got %#v\nwant %#v", got.Verdict, want.Verdict)
	}
	if !reflect.DeepEqual(got.RuleIDs, want.RuleIDs) || !reflect.DeepEqual(got.Abstains, want.Abstains) {
		return fmt.Errorf("lists\n got %#v\nwant %#v", got, want)
	}
	if !readingsEqual(got.Readings, want.Readings) {
		return fmt.Errorf("readings\n got %#v\nwant %#v", got.Readings, want.Readings)
	}
	return nil
}

func verdictEqual(got, want quelor.Verdict) bool {
	if got.Kind != want.Kind || got.Reason != want.Reason || got.WakeToken != want.WakeToken || got.Hibernate != want.Hibernate {
		return false
	}
	if !reflect.DeepEqual(got.Stamps, want.Stamps) {
		return false
	}
	if (got.WakeAt == nil) != (want.WakeAt == nil) {
		return false
	}
	return got.WakeAt == nil || got.WakeAt.Equal(*want.WakeAt)
}

func TestHTTPResponseBodyNotTruncatedAsSuccess(t *testing.T) {
	// A body of max+1 that is valid JSON plus trailing space must not be
	// accepted by cutting the read at 1 MiB.
	base := `{"decider_api_version":"1","status":"answered","verdict":{"kind":"ADMIT"},"decider_id":"x","latency_ms":1}`
	raw := append([]byte(base), bytes.Repeat([]byte(" "), maxBodyBytes)...)
	c := &HTTPClient{
		URL:       "http://example.test/decide",
		DeciderID: "client-decider",
		Timeout:   time.Second,
		HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(raw)), Header: make(http.Header)}, nil
		})},
	}
	resp, err := c.Decide(context.Background(), Request{})
	if !errors.Is(err, ErrUnavailable) || !errors.Is(err, errBodyTooLarge) {
		t.Fatal(err)
	}
	if resp.Verdict.Kind == quelor.Admit || resp.Status != StatusUnavailable {
		t.Fatalf("%#v", resp)
	}
}
