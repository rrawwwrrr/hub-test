// Package k8s provides a minimal Kubernetes API client for in-cluster use.
// It reads the service-account token and CA cert from the standard mount path
// and talks directly to the API server via HTTPS — no external dependencies.
package k8s

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const (
	inClusterTokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	inClusterCAFile    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	inClusterAPIServer = "https://kubernetes.default.svc"
)

// Client is a minimal Kubernetes REST client scoped to one namespace.
type Client struct {
	apiBase    string
	token      string
	httpClient *http.Client
	Namespace  string
}

// InClusterClient creates a Client using the pod's service-account credentials.
func InClusterClient(namespace string) (*Client, error) {
	tokenBytes, err := os.ReadFile(inClusterTokenFile)
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}
	caBytes, err := os.ReadFile(inClusterCAFile)
	if err != nil {
		return nil, fmt.Errorf("read CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, fmt.Errorf("failed to parse cluster CA cert")
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: pool},
		},
	}
	return &Client{
		apiBase:    inClusterAPIServer,
		token:      strings.TrimSpace(string(tokenBytes)),
		httpClient: httpClient,
		Namespace:  namespace,
	}, nil
}

// ── Minimal Kubernetes types ──────────────────────────────────────────────────

// Pod is a minimal representation of a Kubernetes Pod.
type Pod struct {
	APIVersion string      `json:"apiVersion,omitempty"`
	Kind       string      `json:"kind,omitempty"`
	Metadata   ObjectMeta  `json:"metadata"`
	Spec       PodSpec     `json:"spec"`
	Status     PodStatus   `json:"status,omitempty"`
}

// ObjectMeta contains identifying metadata.
type ObjectMeta struct {
	Name            string            `json:"name,omitempty"`
	Namespace       string            `json:"namespace,omitempty"`
	UID             string            `json:"uid,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
	ResourceVersion string            `json:"resourceVersion,omitempty"`
}

// PodSpec describes the desired state of a Pod.
type PodSpec struct {
	RestartPolicy                 string      `json:"restartPolicy,omitempty"`
	Containers                    []Container `json:"containers"`
	Volumes                       []Volume    `json:"volumes,omitempty"`
	TerminationGracePeriodSeconds *int64      `json:"terminationGracePeriodSeconds,omitempty"`
}

// Container describes one container in a Pod.
type Container struct {
	Name            string        `json:"name"`
	Image           string        `json:"image"`
	Command         []string      `json:"command,omitempty"`
	Args            []string      `json:"args,omitempty"`
	Env             []EnvVar      `json:"env,omitempty"`
	VolumeMounts    []VolumeMount `json:"volumeMounts,omitempty"`
	SecurityContext *SecurityContext `json:"securityContext,omitempty"`
}

// EnvVar is a name/value pair passed as an environment variable.
type EnvVar struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// VolumeMount describes a volume mount inside a container.
type VolumeMount struct {
	Name      string `json:"name"`
	MountPath string `json:"mountPath"`
}

// Volume describes a volume that can be mounted by containers in a Pod.
type Volume struct {
	Name     string    `json:"name"`
	EmptyDir *EmptyDir `json:"emptyDir,omitempty"`
}

// EmptyDir represents an empty directory volume.
type EmptyDir struct{}

// SecurityContext holds security configuration for a container.
type SecurityContext struct {
	Privileged *bool `json:"privileged,omitempty"`
}

// PodStatus describes the current status of a Pod.
type PodStatus struct {
	Phase             string            `json:"phase,omitempty"`
	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty"`
}

// ContainerStatus describes the status of a container.
type ContainerStatus struct {
	Name  string         `json:"name"`
	State ContainerState `json:"state"`
}

// ContainerState holds the possible states of a container.
type ContainerState struct {
	Running    *ContainerStateRunning    `json:"running,omitempty"`
	Terminated *ContainerStateTerminated `json:"terminated,omitempty"`
}

// ContainerStateRunning describes a running container.
type ContainerStateRunning struct{}

// ContainerStateTerminated describes a terminated container.
type ContainerStateTerminated struct {
	ExitCode int    `json:"exitCode"`
	Reason   string `json:"reason,omitempty"`
}

// PodList is a list of Pods.
type PodList struct {
	Items []Pod `json:"items"`
}

// ── API methods ────────────────────────────────────────────────────────────────

// ListPods returns all pods in the namespace matching the given label selector
// (e.g. "hub-test.io/managed=true"). Pass "" to list all.
func (c *Client) ListPods(ctx context.Context, labelSelector string) ([]Pod, error) {
	path := c.podPath("")
	if labelSelector != "" {
		path += "?labelSelector=" + url.QueryEscape(labelSelector)
	}
	var list PodList
	if err := c.get(ctx, path, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// GetPod retrieves a single pod by name.
func (c *Client) GetPod(ctx context.Context, name string) (*Pod, error) {
	var pod Pod
	if err := c.get(ctx, c.podPath(name), &pod); err != nil {
		return nil, err
	}
	return &pod, nil
}

// CreatePod creates a pod in the namespace.
func (c *Client) CreatePod(ctx context.Context, pod *Pod) error {
	pod.APIVersion = "v1"
	pod.Kind = "Pod"
	pod.Metadata.Namespace = c.Namespace
	return c.post(ctx, c.podPath(""), pod, nil)
}

// DeletePod deletes a pod by name (best-effort; ignores 404).
func (c *Client) DeletePod(ctx context.Context, name string) error {
	err := c.delete(ctx, c.podPath(name))
	if err != nil && strings.Contains(err.Error(), "404") {
		return nil
	}
	return err
}

// PodLogs returns the logs of a specific container in a pod.
// If tailLines > 0, only the last N lines are returned.
func (c *Client) PodLogs(ctx context.Context, name, container string, tailLines int) ([]byte, error) {
	path := fmt.Sprintf("%s/log?container=%s", c.podPath(name), url.QueryEscape(container))
	if tailLines > 0 {
		path += "&tailLines=" + strconv.Itoa(tailLines)
	}
	return c.getRaw(ctx, path)
}

// ── HTTP helpers ───────────────────────────────────────────────────────────────

func (c *Client) podPath(name string) string {
	base := fmt.Sprintf("/api/v1/namespaces/%s/pods", c.Namespace)
	if name == "" {
		return base
	}
	return base + "/" + name
}

func (c *Client) get(ctx context.Context, path string, out interface{}) error {
	body, err := c.getRaw(ctx, path)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

func (c *Client) getRaw(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiBase+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	return c.doRaw(req)
}

func (c *Client) post(ctx context.Context, path string, body, out interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiBase+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.doRaw(req)
	if err != nil {
		return err
	}
	if out != nil {
		return json.Unmarshal(resp, out)
	}
	return nil
}

func (c *Client) delete(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.apiBase+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	_, err = c.doRaw(req)
	return err
}

func (c *Client) doRaw(req *http.Request) ([]byte, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("k8s API %s %s: %d %s", req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}
