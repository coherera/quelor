package admit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	dockerHubAuthURL     = "https://auth.docker.io/token"
	dockerHubRegistryURL = "https://registry-1.docker.io/v2/ratelimitpreview/test/manifests/latest"
	dockerHubService     = "registry.docker.io"
	dockerHubScope       = "repository:ratelimitpreview/test:pull"
)

// DockerHubReader reads Docker Hub's anonymous or authenticated pull budget.
// Username and Token are optional, but must be supplied together. AuthURL and
// RegistryURL can be overridden for tests or compatible registry services.
type DockerHubReader struct {
	Username    string
	Token       string
	HTTP        *http.Client
	AuthURL     string
	RegistryURL string
}

func (c *DockerHubReader) httpClient() *http.Client {
	if c != nil && c.HTTP != nil {
		return c.HTTP
	}
	return (&Client{}).httpClient()
}

func (c *DockerHubReader) tokenURL() string {
	if c != nil && strings.TrimSpace(c.AuthURL) != "" {
		return strings.TrimSpace(c.AuthURL)
	}
	return dockerHubAuthURL
}

func (c *DockerHubReader) registryURL() string {
	if c != nil && strings.TrimSpace(c.RegistryURL) != "" {
		return strings.TrimSpace(c.RegistryURL)
	}
	return dockerHubRegistryURL
}

type dockerHubTokenResponse struct {
	Token       string `json:"token"`
	AccessToken string `json:"access_token"`
}

// Read fetches a short-lived registry token and makes a HEAD request to
// Docker's rate-limit preview image. It never returns an unknown reading as
// available capacity.
func (c *DockerHubReader) Read(ctx context.Context, resource string) (Reading, error) {
	out := Reading{Resource: resource}
	if resource != ResourceDockerHubPulls {
		return out, fmt.Errorf("unknown resource %q", resource)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil {
		c = &DockerHubReader{}
	}
	username := strings.TrimSpace(c.Username)
	password := strings.TrimSpace(c.Token)
	if (username == "") != (password == "") {
		return out, errors.New("dockerhub credentials require both username and token")
	}
	httpClient := c.httpClient()

	endpoint, err := url.Parse(c.tokenURL())
	if err != nil {
		return out, errors.New("dockerhub token URL is invalid")
	}
	query := endpoint.Query()
	query.Set("service", dockerHubService)
	query.Set("scope", dockerHubScope)
	endpoint.RawQuery = query.Encode()
	tokenReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return out, errors.New("dockerhub token request could not be created")
	}
	if username != "" {
		tokenReq.SetBasicAuth(username, password)
	}
	tokenResp, err := httpClient.Do(tokenReq)
	if err != nil {
		return out, errors.New("dockerhub token request failed")
	}
	defer tokenResp.Body.Close()
	if tokenResp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(tokenResp.Body, 1<<20))
		return out, fmt.Errorf("dockerhub token request: http %d", tokenResp.StatusCode)
	}
	var tokenPayload dockerHubTokenResponse
	if err := json.NewDecoder(io.LimitReader(tokenResp.Body, 1<<20)).Decode(&tokenPayload); err != nil {
		return out, errors.New("dockerhub token response is invalid")
	}
	registryToken := tokenPayload.Token
	if registryToken == "" {
		registryToken = tokenPayload.AccessToken
	}
	if registryToken == "" {
		return out, errors.New("dockerhub token response did not contain a token")
	}

	registryReq, err := http.NewRequestWithContext(ctx, http.MethodHead, c.registryURL(), nil)
	if err != nil {
		return out, errors.New("dockerhub registry request could not be created")
	}
	registryReq.Header.Set("Authorization", "Bearer "+registryToken)
	registryReq.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json")
	registryResp, err := httpClient.Do(registryReq)
	if err != nil {
		return out, errors.New("dockerhub registry request failed")
	}
	defer registryResp.Body.Close()
	if registryResp.StatusCode < http.StatusOK || registryResp.StatusCode >= http.StatusMultipleChoices {
		return out, fmt.Errorf("dockerhub rate-limit request: http %d", registryResp.StatusCode)
	}

	remainingHeader := strings.TrimSpace(registryResp.Header.Get("ratelimit-remaining"))
	if remainingHeader == "" {
		return out, errors.New("dockerhub rate-limit response is missing ratelimit-remaining; reading is unknown")
	}
	remaining, err := parseDockerHubLimit(remainingHeader)
	if err != nil {
		return out, errors.New("dockerhub rate-limit response has an invalid ratelimit-remaining; reading is unknown")
	}
	out.Remaining = remaining
	if limitHeader := strings.TrimSpace(registryResp.Header.Get("ratelimit-limit")); limitHeader != "" {
		if limit, err := parseDockerHubLimit(limitHeader); err == nil {
			out.Limit = limit
		}
	}
	return out, nil
}

func parseDockerHubLimit(value string) (int64, error) {
	value, _, _ = strings.Cut(value, ";")
	limit, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || limit < 0 {
		return 0, errors.New("invalid Docker Hub rate-limit value")
	}
	return limit, nil
}
