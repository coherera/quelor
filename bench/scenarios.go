// Package bench measures Quelor admission latency and throughput and runs
// deterministic scarcity scenarios against a LeaseAPI.
package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coherera/quelor/pkg/classify"
	"github.com/coherera/quelor/pkg/engine"
	"github.com/coherera/quelor/pkg/quelor"
	"github.com/coherera/quelor/pkg/resource"
)

var _ quelor.LeaseAPI = (*engine.Engine)(nil)

const (
	defaultUnits          = 20000
	defaultSeed           = 1
	defaultLatencySamples = 20000

	tenantID       = "bench"
	resSlots       = "slots"
	resBucket      = "bucket"
	resUpstream    = "upstream"
	resDep         = "dep"
	bucketCapacity = 1000
	holdersN       = 3
	resumeBatch    = 100

	stormConfigured = 400
	stormTrueLimit  = 100
	stormWindows    = 6

	flapThreshold = 3
	realDefectN   = 5

	scenarioBucket = "shared-token-bucket"
	scenarioStorm  = "429-storm"
	scenarioFlap   = "dependency-flap"

	rateLimitLog  = "API rate limit exceeded"
	realDefectLog = "--- FAIL: TestX"
)

const (
	scenarioWindow = time.Hour
	flapOpenFor    = 30 * time.Second
	flapDownFor    = 120 * time.Second
	flapUpFor      = 2 * flapOpenFor
	throughputSpan = 200 * time.Millisecond
)

const (
	circClosed = iota
	circOpen
	circHalf
)

// FakeClock is a goroutine-safe instant that tests and scenarios move explicitly.
type FakeClock struct {
	mu sync.Mutex
	t  time.Time
}

// NewFakeClock starts at start. A zero start becomes 2026-01-01 UTC.
func NewFakeClock(start time.Time) *FakeClock {
	if start.IsZero() {
		start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &FakeClock{t: start}
}

// Now reports the current instant.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance adds d to the current instant.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// Options controls scenario scale, the shuffle seed, and latency sample count.
type Options struct {
	Units          int
	Seed           int64
	LatencySamples int
}

func normalizeOptions(opt Options) Options {
	if opt.Units <= 0 {
		opt.Units = defaultUnits
	}
	if opt.Seed == 0 {
		opt.Seed = defaultSeed
	}
	if opt.LatencySamples <= 0 {
		opt.LatencySamples = defaultLatencySamples
	}
	return opt
}

// ScenarioResult is one scenario's admission counts.
// CorrectDefers/TotalEmpty is that scenario's correctness ratio.
type ScenarioResult struct {
	Name              string
	Units             int
	Admitted          int
	Deferred          int
	Shed              int
	Denied            int
	AdmittedIntoEmpty int
	CorrectDefers     int
	TotalEmpty        int
	Notes             string
}

// Report is the headline measurement plus per-scenario rows.
type Report struct {
	AcquireP50  time.Duration
	AcquireP99  time.Duration
	Throughput  float64
	Correctness float64
	FalseRed    float64
	Scenarios   []ScenarioResult
}

// String prints four headline lines and a per-scenario table.
func (r Report) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "p50/p99 acquire: %s / %s\n", r.AcquireP50, r.AcquireP99)
	fmt.Fprintf(&b, "throughput: %.2f acquire+settle pairs/s\n", r.Throughput)
	fmt.Fprintf(&b, "correctness under scarcity: %.6f\n", r.Correctness)
	fmt.Fprintf(&b, "false-red rate: %.6f\n", r.FalseRed)
	fmt.Fprintf(&b, "%-22s %8s %8s %8s %8s %8s %12s %14s %11s\n",
		"scenario", "units", "admitted", "deferred", "shed", "denied", "into_empty", "correct_defers", "total_empty")
	for _, s := range r.Scenarios {
		fmt.Fprintf(&b, "%-22s %8d %8d %8d %8d %8d %12d %14d %11d\n",
			s.Name, s.Units, s.Admitted, s.Deferred, s.Shed, s.Denied, s.AdmittedIntoEmpty, s.CorrectDefers, s.TotalEmpty)
		if s.Notes != "" {
			fmt.Fprintf(&b, "  notes: %s\n", s.Notes)
		}
	}
	return b.String()
}

// JSON returns the report as indented JSON.
func (r Report) JSON() string {
	buf, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "{}\n"
	}
	return string(buf) + "\n"
}

// AdvanceFunc moves an engine's timers to now. It may be nil.
type AdvanceFunc func(now time.Time)

// Factory builds a LeaseAPI bound to clock.
type Factory func(clock *FakeClock) (quelor.LeaseAPI, AdvanceFunc, error)

// RunAll measures latency and throughput and runs every scenario.
func RunAll(opt Options) Report {
	opt = normalizeOptions(opt)
	rep := Report{Scenarios: make([]ScenarioResult, 0, 3)}
	rep.AcquireP50, rep.AcquireP99 = measureLatency(opt.LatencySamples)
	rep.Throughput = measureThroughput()

	bucket, err := RunSharedTokenBucket(opt, sharedBucketFactory)
	if err != nil {
		bucket = ScenarioResult{Name: scenarioBucket, Notes: "error: " + err.Error(), TotalEmpty: 1}
	}
	storm, fr, err := run429Storm(opt)
	if err != nil {
		storm = ScenarioResult{Name: scenarioStorm, Notes: "error: " + err.Error(), TotalEmpty: 1}
		fr = 1
	}
	flap, err := runDependencyFlap(opt)
	if err != nil {
		flap = ScenarioResult{Name: scenarioFlap, Notes: "error: " + err.Error(), TotalEmpty: 1}
	}
	rep.Scenarios = append(rep.Scenarios, bucket, storm, flap)
	rep.FalseRed = fr
	rep.Correctness = meanCorrectness(rep.Scenarios)
	return rep
}

func meanCorrectness(ss []ScenarioResult) float64 {
	if len(ss) == 0 {
		return 0
	}
	var sum float64
	for _, s := range ss {
		sum += ratio(s.CorrectDefers, s.TotalEmpty)
	}
	return sum / float64(len(ss))
}

func ratio(num, den int) float64 {
	if den <= 0 {
		return 0
	}
	return float64(num) / float64(den)
}

func measureLatency(n int) (time.Duration, time.Duration) {
	if n <= 0 {
		return 0, 0
	}
	api, err := openAPI(time.Now, time.Minute, leaseManifest(resSlots, 1<<62))
	if err != nil {
		return 0, 0
	}
	ctx := context.Background()
	samples := make([]time.Duration, 0, n)
	for i := 0; i < n; i++ {
		env := baseEnv(fmt.Sprintf("lat-%d", i), fmt.Sprintf("lat-holder-%d", i), resSlots, 1)
		t0 := time.Now()
		res, err := api.AcquireLease(ctx, quelor.AcquireRequest{Envelope: env})
		elapsed := time.Since(t0)
		if err != nil || res.Verdict.Kind != quelor.Admit || res.Lease == nil {
			continue
		}
		samples = append(samples, elapsed)
		_ = settleGranted(ctx, api, env.HolderID, res.Lease, env.Demands, nil, nil)
	}
	return percentile(samples, 50), percentile(samples, 99)
}

func measureThroughput() float64 {
	api, err := openAPI(time.Now, time.Minute, leaseManifest(resSlots, 1<<62))
	if err != nil {
		return 0
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = 1
	}
	ctx := context.Background()
	var seq atomic.Int64
	var pairs atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	deadline := start.Add(throughputSpan)
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				n := seq.Add(1)
				id := strconv.FormatInt(n, 10)
				env := baseEnv("tp-"+id, "tph-"+id, resSlots, 1)
				res, err := api.AcquireLease(ctx, quelor.AcquireRequest{Envelope: env})
				if err != nil || res.Verdict.Kind != quelor.Admit || res.Lease == nil {
					continue
				}
				if _, serr := api.SettleLease(ctx, quelor.SettleRequest{
					LeaseID:  res.Lease.ID,
					HolderID: env.HolderID,
					Consumed: consumedOf(res.Lease, env.Demands),
				}); serr != nil {
					continue
				}
				pairs.Add(1)
			}
		}()
	}
	wg.Wait()
	for time.Since(start) < throughputSpan {
		runtime.Gosched()
	}
	elapsed := time.Since(start)
	if elapsed <= 0 {
		return 0
	}
	return float64(pairs.Load()) / elapsed.Seconds()
}

// RunSharedTokenBucket admits 3 holders × Units demands of 1 against a
// 1000-per-hour window. After 1000 admits the bucket is empty for the rest
// of the window; Advance past the window must allow admissions again.
// factory injects the LeaseAPI so a broken stub can be measured.
func RunSharedTokenBucket(opt Options, factory Factory) (ScenarioResult, error) {
	opt = normalizeOptions(opt)
	if factory == nil {
		return ScenarioResult{}, fmt.Errorf("nil factory")
	}
	clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	api, adv, err := factory(clock)
	if err != nil {
		return ScenarioResult{}, err
	}
	if api == nil {
		return ScenarioResult{}, fmt.Errorf("nil lease api")
	}
	if adv != nil {
		adv(clock.Now())
	}

	ctx := context.Background()
	rng := rand.New(rand.NewSource(opt.Seed))
	subs := make([]holderSeq, 0, holdersN*opt.Units)
	for h := 0; h < holdersN; h++ {
		for i := 0; i < opt.Units; i++ {
			subs = append(subs, holderSeq{holder: h, seq: i})
		}
	}
	rng.Shuffle(len(subs), func(i, j int) {
		subs[i], subs[j] = subs[j], subs[i]
	})

	var s ScenarioResult
	s.Name = scenarioBucket
	admittedOK := 0
	empty := false
	for _, sub := range subs {
		env := baseEnv(
			fmt.Sprintf("tb-%d-%d", sub.holder, sub.seq),
			fmt.Sprintf("holder-%d", sub.holder),
			resBucket,
			1,
		)
		kind, lease, _ := admit(ctx, api, env)
		s.add(kind)
		if !empty {
			if kind == quelor.Admit && lease != nil {
				_ = settleGranted(ctx, api, env.HolderID, lease, env.Demands, nil, nil)
				admittedOK++
				if admittedOK >= bucketCapacity {
					empty = true
				}
			}
			continue
		}
		// Oracle: the shared bucket is empty after bucketCapacity admits.
		s.TotalEmpty++
		switch kind {
		case quelor.Defer:
			s.CorrectDefers++
		case quelor.Admit:
			s.AdmittedIntoEmpty++
			if lease != nil {
				_ = settleGranted(ctx, api, env.HolderID, lease, env.Demands, nil, nil)
			}
		}
	}

	advanceWindow(clock, adv)
	resumed := 0
	for i := 0; i < resumeBatch; i++ {
		env := baseEnv(fmt.Sprintf("tb-resume-%d", i), "holder-0", resBucket, 1)
		kind, lease, _ := admit(ctx, api, env)
		s.add(kind)
		if kind == quelor.Admit && lease != nil {
			resumed++
			_ = settleGranted(ctx, api, env.HolderID, lease, env.Demands, nil, nil)
		}
	}
	s.Notes = fmt.Sprintf("resumed_admits=%d seed=%d", resumed, opt.Seed)
	return s, nil
}

func sharedBucketFactory(clock *FakeClock) (quelor.LeaseAPI, AdvanceFunc, error) {
	return newTimedEngine(clock, resource.Manifest{
		ID:            resBucket,
		CapacityModel: resource.ModelWindowBucket,
		Capacity:      bucketCapacity,
		Window:        resource.Duration(scenarioWindow),
	})
}

func run429Storm(opt Options) (ScenarioResult, float64, error) {
	opt = normalizeOptions(opt)
	clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	api, adv, err := newTimedEngine(clock, resource.Manifest{
		ID:            resUpstream,
		CapacityModel: resource.ModelWindowBucket,
		Capacity:      stormConfigured,
		Window:        resource.Duration(scenarioWindow),
	})
	if err != nil {
		return ScenarioResult{}, 0, err
	}
	if adv != nil {
		adv(clock.Now())
	}

	ctx := context.Background()
	var s ScenarioResult
	s.Name = scenarioStorm
	counts := make([]int, 0, stormWindows)
	admitsN := make([]int, 0, stormWindows)
	failed := 0
	mis := 0

	for w := 0; w < stormWindows; w++ {
		type heldLease struct {
			lease   quelor.Lease
			holder  string
			demands map[string]int64
		}
		// Admit the whole configured window before settling, so 429 feedback
		// cannot shrink this window's burst. Successes are settled before 429s.
		held := make([]heldLease, 0, stormConfigured)
		for i := 0; i < stormConfigured; i++ {
			env := baseEnv(fmt.Sprintf("storm-%d-%d", w, i), "storm-holder", resUpstream, 1)
			kind, lease, _ := admit(ctx, api, env)
			s.add(kind)
			if kind == quelor.Admit && lease != nil {
				held = append(held, heldLease{lease: *lease, holder: env.HolderID, demands: env.Demands})
			}
		}
		left := stormTrueLimit
		n429 := 0
		for i := range held {
			if left > 0 {
				left--
				// A success reports no HTTP status: the engine's additive
				// increase is 5% of capacity per 2xx observation, so reporting
				// 200 for every success would restore the full burst within
				// one window and re-trigger the storm. This scenario measures
				// the multiplicative decrease only.
				out := quelor.ExecutionOutcome{}
				_ = settleGranted(ctx, api, held[i].holder, &held[i].lease, held[i].demands, &out, nil)
				continue
			}
			n429++
			out := quelor.ExecutionOutcome{HTTPStatus: 429, Log: rateLimitLog}
			obs := []quelor.Observation{{Resource: resUpstream, HTTPStatus: 429}}
			_ = settleGranted(ctx, api, held[i].holder, &held[i].lease, held[i].demands, &out, obs)
			failed++
			if classify.Default().Classify(out).Class != quelor.ResourceExhausted {
				mis++
			}
		}
		counts = append(counts, n429)
		admitsN = append(admitsN, len(held))
		advanceWindow(clock, adv)
	}

	realOK := true
	for i := 0; i < realDefectN; i++ {
		out := quelor.ExecutionOutcome{ExitCode: 1, Log: realDefectLog}
		got := classify.Default().Classify(out)
		failed++
		if !got.Failed || got.Class != quelor.RealDefect {
			realOK = false
		}
	}

	decreased := 0
	later := 0
	if len(counts) > 1 {
		later = len(counts) - 1
		for i := 1; i < len(counts); i++ {
			if counts[i] < counts[0] {
				decreased++
			}
		}
	}
	if !realOK {
		decreased = 0
	}
	s.CorrectDefers = decreased
	s.TotalEmpty = later
	if s.TotalEmpty == 0 {
		s.TotalEmpty = 1
	}
	fr := 0.0
	if failed > 0 {
		fr = float64(mis) / float64(failed)
	}
	s.Notes = fmt.Sprintf(
		"w429=%s wadmits=%s real_defect_ok=%t misclassified=%d failed=%d seed=%d",
		csvInts(counts), csvInts(admitsN), realOK, mis, failed, opt.Seed,
	)
	return s, fr, nil
}

func runDependencyFlap(opt Options) (ScenarioResult, error) {
	opt = normalizeOptions(opt)
	clock := NewFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	api, adv, err := newTimedEngine(clock, resource.Manifest{
		ID:               resDep,
		CapacityModel:    resource.ModelCircuit,
		Capacity:         100000,
		Window:           resource.Duration(flapOpenFor),
		FailureThreshold: flapThreshold,
		OpenFor:          resource.Duration(flapOpenFor),
	})
	if err != nil {
		return ScenarioResult{}, err
	}
	if adv != nil {
		adv(clock.Now())
	}

	ctx := context.Background()
	start := clock.Now()
	downUntil := start.Add(flapDownFor)
	ticks := int((flapDownFor + flapUpFor) / time.Second)

	var s ScenarioResult
	s.Name = scenarioFlap
	state := circClosed
	fails := 0
	var openedAt time.Time
	admittedDown := 0
	openArrivals := 0
	openDefers := 0
	admitsUp := 0
	var firstUp time.Time

	for tick := 0; tick < ticks; tick++ {
		now := clock.Now()
		up := !now.Before(downUntil)
		if state == circOpen && !now.Before(openedAt.Add(flapOpenFor)) {
			state = circHalf
		}
		if state == circOpen {
			openArrivals++
		}
		env := baseEnv(fmt.Sprintf("flap-%d", tick), "holder-flap", resDep, 1)
		kind, lease, _ := admit(ctx, api, env)
		s.add(kind)
		if state == circOpen && kind == quelor.Defer {
			openDefers++
		}
		if kind == quelor.Admit && lease != nil {
			status := 200
			if !up {
				status = 503
				admittedDown++
			} else {
				admitsUp++
				if firstUp.IsZero() {
					firstUp = now
				}
			}
			out := quelor.ExecutionOutcome{HTTPStatus: status}
			obs := []quelor.Observation{{Resource: resDep, HTTPStatus: status}}
			_ = settleGranted(ctx, api, env.HolderID, lease, env.Demands, &out, obs)
			if status >= 500 {
				fails++
				if state == circHalf || fails >= flapThreshold {
					state = circOpen
					openedAt = now
					fails = 0
				}
			} else {
				fails = 0
				state = circClosed
			}
		} else if state == circHalf {
			state = circOpen
			openedAt = now
		} else if state == circClosed && fails > 0 && kind != quelor.Admit {
			state = circOpen
			openedAt = now
			fails = 0
		}
		clock.Advance(time.Second)
		if adv != nil {
			adv(clock.Now())
		}
	}

	bound := flapThreshold + int(flapDownFor/flapOpenFor)
	resumeWithin := !firstUp.IsZero() && !firstUp.After(downUntil.Add(flapOpenFor))
	resumed := admitsUp >= 2 && resumeWithin
	s.CorrectDefers = openDefers
	s.TotalEmpty = openArrivals
	if s.TotalEmpty == 0 {
		s.TotalEmpty = 1
		s.CorrectDefers = 0
	}
	s.Notes = fmt.Sprintf(
		"admitted_while_down=%d bound=%d resumed=%t resume_within_open_for=%t open_arrivals=%d open_defers=%d seed=%d",
		admittedDown, bound, resumed, resumeWithin, openArrivals, openDefers, opt.Seed,
	)
	return s, nil
}

func openEngine(clock func() time.Time, ttl time.Duration, manifests ...resource.Manifest) (*engine.Engine, error) {
	reg := resource.NewRegistry(clock)
	for _, m := range manifests {
		m.WithDefaults()
		if err := reg.Register(m, nil); err != nil {
			return nil, err
		}
	}
	return engine.New(engine.Config{
		Registry: reg,
		Clock:    clock,
		LeaseTTL: ttl,
	})
}

func openAPI(clock func() time.Time, ttl time.Duration, manifests ...resource.Manifest) (quelor.LeaseAPI, error) {
	return openEngine(clock, ttl, manifests...)
}

func newTimedEngine(clock *FakeClock, manifests ...resource.Manifest) (quelor.LeaseAPI, AdvanceFunc, error) {
	eng, err := openEngine(clock.Now, scenarioWindow, manifests...)
	if err != nil {
		return nil, nil, err
	}
	return eng, func(now time.Time) { eng.Advance(now) }, nil
}

func leaseManifest(id string, capacity int64) resource.Manifest {
	return resource.Manifest{
		ID:            id,
		CapacityModel: resource.ModelLease,
		Capacity:      capacity,
	}
}

func advanceWindow(clock *FakeClock, adv AdvanceFunc) {
	clock.Advance(scenarioWindow + time.Second)
	if adv != nil {
		adv(clock.Now())
	}
}

type holderSeq struct {
	holder int
	seq    int
}

func (s *ScenarioResult) add(kind quelor.VerdictKind) {
	s.Units++
	switch kind {
	case quelor.Admit:
		s.Admitted++
	case quelor.Defer:
		s.Deferred++
	case quelor.Shed:
		s.Shed++
	default:
		s.Denied++
	}
}

func baseEnv(unit, holder, res string, qty int64) quelor.Envelope {
	return quelor.Envelope{
		UnitID:   unit,
		TenantID: tenantID,
		HolderID: holder,
		Demands:  map[string]int64{res: qty},
		Priority: quelor.PriorityChange,
	}
}

func admit(ctx context.Context, api quelor.LeaseAPI, env quelor.Envelope) (quelor.VerdictKind, *quelor.Lease, error) {
	res, err := api.AcquireLease(ctx, quelor.AcquireRequest{Envelope: env})
	if err != nil {
		return quelor.Deny, nil, err
	}
	return res.Verdict.Kind, res.Lease, nil
}

func settleGranted(ctx context.Context, api quelor.LeaseAPI, holder string, lease *quelor.Lease, demands map[string]int64, outcome *quelor.ExecutionOutcome, obs []quelor.Observation) error {
	if lease == nil {
		return fmt.Errorf("nil lease")
	}
	_, err := api.SettleLease(ctx, quelor.SettleRequest{
		LeaseID:      lease.ID,
		HolderID:     holder,
		Consumed:     consumedOf(lease, demands),
		Outcome:      outcome,
		Observations: obs,
	})
	return err
}

func consumedOf(lease *quelor.Lease, demands map[string]int64) map[string]int64 {
	if lease != nil && len(lease.Declared) > 0 {
		return copyIntMap(lease.Declared)
	}
	return copyIntMap(demands)
}

func copyIntMap(in map[string]int64) map[string]int64 {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func percentile(samples []time.Duration, p int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := len(sorted) * p / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func csvInts(vals []int) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = strconv.Itoa(v)
	}
	return strings.Join(parts, ",")
}
