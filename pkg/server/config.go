package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/coherera/quelor/pkg/decider"
	"github.com/coherera/quelor/pkg/decider/reference"
	"github.com/coherera/quelor/pkg/engine"
	"github.com/coherera/quelor/pkg/resource"
	"github.com/coherera/quelor/pkg/trace"
	"gopkg.in/yaml.v3"
)

// Config is the quelord YAML document. Unknown fields are rejected.
type Config struct {
	Listen           string                  `yaml:"listen"`
	Resources        []resource.Manifest     `yaml:"resources"`
	ResourceFiles    []string                `yaml:"resource_files"`
	Decider          DeciderConfig           `yaml:"decider"`
	Shadows          []ShadowConfig          `yaml:"shadows"`
	TracePath        string                  `yaml:"trace_path"`
	SharedToken      string                  `yaml:"shared_token"`
	SharedTokenEnv   string                  `yaml:"shared_token_env"`
	HolderTokens     map[string]string       `yaml:"holder_tokens"`
	HolderTokensFile string                  `yaml:"holder_tokens_file"`
	HolderTokensEnv  string                  `yaml:"holder_tokens_env"`
	DefaultQuota     engine.Quota            `yaml:"default_quota"`
	HolderQuotas     map[string]engine.Quota `yaml:"holder_quotas"`
	LeaseTTL         string                  `yaml:"lease_ttl"`
	TicketTTL        string                  `yaml:"ticket_ttl"`
	AgingStep        string                  `yaml:"aging_step"`
	DeciderTimeout   string                  `yaml:"decider_timeout"`
	Cell             string                  `yaml:"cell"`
}

// DeciderConfig selects the primary decider.
// Mode is "reference" or "http". An empty mode means reference.
type DeciderConfig struct {
	Mode    string `yaml:"mode"`
	URL     string `yaml:"url"`
	Timeout string `yaml:"timeout"`
	Ruleset string `yaml:"ruleset"`
}

// ShadowConfig is an HTTP decider run beside the primary.
type ShadowConfig struct {
	URL     string `yaml:"url"`
	Timeout string `yaml:"timeout"`
}

// LoadConfig reads a YAML file with known fields enforced.
// An empty listen address becomes 127.0.0.1:8080.
func LoadConfig(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("load config: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	var c Config
	if err := dec.Decode(&c); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8080"
	}
	return c, nil
}

// CountResources is the inline manifest count plus manifests loaded from files.
func (c Config) CountResources() (int, error) {
	n := len(c.Resources)
	for _, p := range c.ResourceFiles {
		ms, err := resource.LoadManifestFile(p)
		if err != nil {
			return 0, fmt.Errorf("resource file %s: %w", p, err)
		}
		n += len(ms)
	}
	return n, nil
}

// Build wires a registry, decider, shadows, and trace file into an engine.
// No external resource readers are attached. Tokens are not logged.
func (c Config) Build() (*engine.Engine, error) {
	durs, err := c.durations()
	if err != nil {
		return nil, err
	}
	dec, err := c.buildDecider()
	if err != nil {
		return nil, err
	}
	shadows, err := c.buildShadows()
	if err != nil {
		return nil, err
	}
	reg, err := c.buildRegistry()
	if err != nil {
		return nil, err
	}
	tw, err := c.buildTrace()
	if err != nil {
		return nil, err
	}
	eng, err := engine.New(engine.Config{
		Registry:       reg,
		Decider:        dec,
		Shadows:        shadows,
		Trace:          tw,
		LeaseTTL:       durs.lease,
		TicketTTL:      durs.ticket,
		AgingStep:      durs.aging,
		DeciderTimeout: durs.decider,
		SharedToken:    c.sharedToken(),
		HolderTokens:   c.holderTokens(),
		DefaultQuota:   c.DefaultQuota,
		HolderQuotas:   c.HolderQuotas,
		Cell:           c.Cell,
	})
	if err != nil {
		closeIfCloser(tw)
		return nil, err
	}
	return eng, nil
}

type parsedDurations struct {
	lease   time.Duration
	ticket  time.Duration
	aging   time.Duration
	decider time.Duration
}

func (c Config) durations() (parsedDurations, error) {
	var d parsedDurations
	var err error
	if d.lease, err = parseDuration(c.LeaseTTL); err != nil {
		return d, fmt.Errorf("lease_ttl: %w", err)
	}
	if d.ticket, err = parseDuration(c.TicketTTL); err != nil {
		return d, fmt.Errorf("ticket_ttl: %w", err)
	}
	if d.aging, err = parseDuration(c.AgingStep); err != nil {
		return d, fmt.Errorf("aging_step: %w", err)
	}
	if d.decider, err = parseDuration(c.DeciderTimeout); err != nil {
		return d, fmt.Errorf("decider_timeout: %w", err)
	}
	return d, nil
}

func parseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, fmt.Errorf("negative duration %s", s)
	}
	return d, nil
}

// sharedToken prefers a non-empty SharedTokenEnv variable over SharedToken.
func (c Config) sharedToken() string {
	if c.SharedTokenEnv != "" {
		if v, ok := os.LookupEnv(c.SharedTokenEnv); ok && v != "" {
			return v
		}
	}
	return c.SharedToken
}

// the plaintext holder_tokens map, then holder_tokens_file, then holder_tokens_env.
// Tokens are never logged.
func (c Config) holderTokens() map[string]string {
	out := make(map[string]string)
	for k, v := range c.HolderTokens {
		if v != "" {
			out[k] = v
		}
	}
	if c.HolderTokensFile != "" {
		b, err := os.ReadFile(c.HolderTokensFile)
		if err == nil {
			var fromFile map[string]string
			if err := yaml.Unmarshal(b, &fromFile); err == nil {
				for k, v := range fromFile {
					if v != "" {
						out[k] = v
					}
				}
			}
		}
	}
	if c.HolderTokensEnv != "" {
		if v, ok := os.LookupEnv(c.HolderTokensEnv); ok && v != "" {
			var fromEnv map[string]string
			if err := json.Unmarshal([]byte(v), &fromEnv); err == nil {
				for k, tok := range fromEnv {
					if tok != "" {
						out[k] = tok
					}
				}
			} else {
				for _, part := range strings.Split(v, ",") {
					part = strings.TrimSpace(part)
					if part == "" {
						continue
					}
					if i := strings.IndexAny(part, "=:"); i > 0 {
						key := strings.TrimSpace(part[:i])
						val := strings.TrimSpace(part[i+1:])
						if key != "" && val != "" {
							out[key] = val
						}
					}
				}
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func (c Config) buildDecider() (decider.Decider, error) {
	mode := c.Decider.Mode
	if mode == "" {
		mode = "reference"
	}
	switch mode {
	case "reference":
		if c.Decider.Ruleset == "" {
			d := reference.Default()
			if d == nil {
				return nil, errors.New("reference default decider is nil")
			}
			return d, nil
		}
		b, err := os.ReadFile(c.Decider.Ruleset)
		if err != nil {
			return nil, fmt.Errorf("read ruleset: %w", err)
		}
		rs, err := reference.Parse(b)
		if err != nil {
			return nil, fmt.Errorf("parse ruleset: %w", err)
		}
		d := reference.New(rs)
		if d == nil {
			return nil, errors.New("reference decider is nil")
		}
		return d, nil
	case "http":
		if c.Decider.URL == "" {
			return nil, errors.New("decider url is required in http mode")
		}
		to, err := parseDuration(c.Decider.Timeout)
		if err != nil {
			return nil, fmt.Errorf("decider timeout: %w", err)
		}
		return &decider.HTTPClient{URL: c.Decider.URL, Timeout: to, DeciderID: "primary"}, nil
	default:
		return nil, fmt.Errorf("unknown decider mode %q", mode)
	}
}

func (c Config) buildShadows() ([]decider.Decider, error) {
	if len(c.Shadows) == 0 {
		return nil, nil
	}
	out := make([]decider.Decider, 0, len(c.Shadows))
	for i, s := range c.Shadows {
		if s.URL == "" {
			return nil, fmt.Errorf("shadow %d: url is required", i)
		}
		to, err := parseDuration(s.Timeout)
		if err != nil {
			return nil, fmt.Errorf("shadow %d timeout: %w", i, err)
		}
		out = append(out, &decider.HTTPClient{
			URL:       s.URL,
			Timeout:   to,
			DeciderID: fmt.Sprintf("shadow-%d", i),
		})
	}
	return out, nil
}

func (c Config) buildRegistry() (*resource.Registry, error) {
	reg := resource.NewRegistry(nil)
	for _, m := range c.Resources {
		if err := reg.Register(m, nil); err != nil {
			return nil, fmt.Errorf("register resource: %w", err)
		}
	}
	for _, p := range c.ResourceFiles {
		ms, err := resource.LoadManifestFile(p)
		if err != nil {
			return nil, fmt.Errorf("resource file %s: %w", p, err)
		}
		for _, m := range ms {
			if err := reg.Register(m, nil); err != nil {
				return nil, fmt.Errorf("register resource from %s: %w", p, err)
			}
		}
	}
	return reg, nil
}

func (c Config) buildTrace() (trace.Writer, error) {
	if c.TracePath == "" {
		return nil, nil
	}
	w, err := trace.OpenFile(c.TracePath)
	if err != nil {
		return nil, fmt.Errorf("trace: %w", err)
	}
	return w, nil
}

func closeIfCloser(w trace.Writer) {
	c, ok := w.(io.Closer)
	if ok && c != nil {
		_ = c.Close()
	}
}
