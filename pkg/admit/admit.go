// Package admit gates a unit of work on a live GitHub rate-limit reading.
// There is no daemon and no local capacity cache: an unknown reading is never
// treated as available.
package admit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultBaseURL = "https://api.github.com"
const acceptHeader = "application/vnd.github+json"
const githubAPIVersion = "2022-11-28"
const userAgent = "quelor-admit"
const verdictAdmit = "admit"
const verdictDefer = "defer"
const verdictDegraded = "degraded"
const exitAdmit = 0
const exitDefer = 42
const exitDegraded = 43

// ResourceDockerHubPulls is the Docker Hub pull-budget resource.
const ResourceDockerHubPulls = "dockerhub_pulls"

// ErrMissingToken means no GitHub token was configured. The rate limit is
// unknown and must not be treated as available.
var ErrMissingToken = errors.New("github token is not set")

// nowFn is the clock used for "seconds until reset". Tests replace it.
var nowMu sync.Mutex
var nowFn = time.Now

func now() time.Time {
	nowMu.Lock()
	defer nowMu.Unlock()
	return nowFn()
}

// ResourceReader reads one named resource.
type ResourceReader interface {
	Read(context.Context, string) (Reading, error)
}

// Client reads GitHub rate-limit resources.
// A zero BaseURL resolves to $GITHUB_API_URL or https://api.github.com.
// A nil HTTP client uses a 10s timeout.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// Reading is one GitHub rate-limit window. Reset is UTC.
type Reading struct {
	Resource  string
	Limit     int64
	Remaining int64
	Reset     time.Time
}

// Result is the admission decision for one demand against one reading.
type Result struct {
	Verdict   string    `json:"verdict"`
	Resource  string    `json:"resource"`
	Need      int64     `json:"need"`
	Remaining int64     `json:"remaining"`
	Reset     time.Time `json:"reset,omitempty"`
	Reason    string    `json:"reason,omitempty"`
	Exit      int       `json:"exit"`

	// known is false when the reading is unknown. WriteGitHubOutput then
	// leaves remaining blank so a real zero is distinct from "no reading".
	known bool
}

type ghWindow struct {
	Limit     int64 `json:"limit"`
	Remaining int64 `json:"remaining"`
	Reset     int64 `json:"reset"`
	Used      int64 `json:"used"`
}

type ghPayload struct {
	Resources map[string]ghWindow `json:"resources"`
}

func (c *Client) resolveBaseURL() string {
	raw := ""
	if c != nil {
		raw = strings.TrimSpace(c.BaseURL)
	}
	if raw == "" {
		raw = strings.TrimSpace(os.Getenv("GITHUB_API_URL"))
	}
	if raw == "" {
		return defaultBaseURL
	}
	return strings.TrimRight(raw, "/")
}

func (c *Client) httpClient() *http.Client {
	if c != nil && c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 10 * time.Second}
}

func mapResource(resource string) (string, error) {
	switch resource {
	case "github_api":
		return "core", nil
	case "github_search":
		return "search", nil
	case "github_graphql":
		return "graphql", nil
	case "github_code_search":
		return "code_search", nil
	default:
		return "", fmt.Errorf("unknown resource %q", resource)
	}
}

func scrub(msg, token string) string {
	if msg == "" || token == "" {
		return msg
	}
	msg = strings.ReplaceAll(msg, token, "REDACTED")
	if esc := url.QueryEscape(token); esc != token {
		msg = strings.ReplaceAll(msg, esc, "REDACTED")
	}
	return msg
}

// Read GETs {BaseURL}/rate_limit and returns the window for resource.
// Unknown resources and a missing token fail before any request. The error
// text never contains the token.
func (c *Client) Read(ctx context.Context, resource string) (Reading, error) {
	if c == nil {
		c = &Client{}
	}
	out := Reading{Resource: resource}
	ghName, err := mapResource(resource)
	if err != nil {
		return out, err
	}
	token := strings.TrimSpace(c.Token)
	if token == "" {
		return out, ErrMissingToken
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.resolveBaseURL()+"/rate_limit", nil)
	if err != nil {
		return out, errors.New(scrub(err.Error(), token))
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", acceptHeader)
	req.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return out, errors.New(scrub(err.Error(), token))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		message := fmt.Sprintf("github rate_limit: http %d", resp.StatusCode)
		if retryAfter := strings.TrimSpace(resp.Header.Get("Retry-After")); retryAfter != "" {
			if seconds, err := strconv.ParseUint(retryAfter, 10, 64); err == nil {
				message += fmt.Sprintf(" (retry after %d seconds)", seconds)
			} else {
				message += fmt.Sprintf(" (retry after %s)", retryAfter)
			}
		}
		return out, errors.New(scrub(message, token))
	}
	var payload ghPayload
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&payload); err != nil {
		return out, errors.New(scrub("github rate_limit: "+err.Error(), token))
	}
	window, ok := payload.Resources[ghName]
	if !ok {
		return out, fmt.Errorf("github rate_limit: resource %q (%s) absent from response", resource, ghName)
	}
	out.Limit = window.Limit
	out.Remaining = window.Remaining
	out.Reset = time.Unix(window.Reset, 0).UTC()
	return out, nil
}

func secondsUntil(reset time.Time) int64 {
	d := reset.Sub(now())
	if d <= 0 {
		return 0
	}
	return int64(d / time.Second)
}

// Decide turns a rate-limit reading into an admission result.
// A non-nil err, including ErrMissingToken, is an unknown reading: Remaining
// on r is ignored and is never treated as available capacity.
func Decide(r Reading, err error, need int64, optional bool) Result {
	res := Result{Resource: r.Resource, Need: need}
	if err != nil {
		res.Verdict = verdictDegraded
		res.Exit = exitDegraded
		if errors.Is(err, ErrMissingToken) {
			res.Reason = "missing token: reading is unknown and is not treated as available"
		} else {
			res.Reason = fmt.Sprintf("rate-limit read failed: %s; reading is unknown and is not treated as available", err.Error())
		}
		return res
	}
	res.Remaining = r.Remaining
	res.Reset = r.Reset
	res.known = true
	if r.Remaining >= need {
		res.Verdict = verdictAdmit
		res.Exit = exitAdmit
		res.Reason = "remaining covers need"
		return res
	}
	if optional {
		if r.Reset.IsZero() {
			res.Verdict = verdictDefer
			res.Exit = exitDefer
			res.Reason = fmt.Sprintf("remaining %d < need %d; reset time unavailable", r.Remaining, need)
			return res
		}
		secs := secondsUntil(r.Reset)
		res.Verdict = verdictDefer
		res.Exit = exitDefer
		res.Reason = fmt.Sprintf("remaining %d < need %d; reset %s (%d seconds until reset)", r.Remaining, need, r.Reset.UTC().Format(time.RFC3339), secs)
		return res
	}
	res.Verdict = verdictDegraded
	res.Exit = exitDegraded
	res.Reason = fmt.Sprintf("remaining %d < need %d; demand is required", r.Remaining, need)
	return res
}

// Line is the one-line human verdict. A zero reset is left blank.
func (r Result) Line() string {
	reset := ""
	if !r.Reset.IsZero() {
		reset = r.Reset.UTC().Format(time.RFC3339)
	}
	return fmt.Sprintf("%s %s need=%d remaining=%d reset=%s", r.Verdict, r.Resource, r.Need, r.Remaining, reset)
}

// WriteGitHubOutput appends verdict and remaining lines to path, which is the
// GitHub Actions GITHUB_OUTPUT file. An empty path is a no-op. remaining is
// blank when the reading is unknown, and numeric (including 0) when it is known.
func (r Result) WriteGitHubOutput(path string) error {
	if path == "" {
		return nil
	}
	rem := ""
	if r.known {
		rem = strconv.FormatInt(r.Remaining, 10)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, werr := fmt.Fprintf(f, "verdict=%s\nremaining=%s\n", r.Verdict, rem)
	cerr := f.Close()
	if werr != nil {
		return werr
	}
	return cerr
}
