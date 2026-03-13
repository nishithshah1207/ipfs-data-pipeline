// Package ipfs provides a client for interacting with IPFS nodes via the Kubo HTTP RPC API.
package ipfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Client communicates with one or more IPFS Kubo nodes via their HTTP RPC API.
type Client struct {
	nodes      []string
	httpClient *http.Client
	mu         sync.RWMutex
}

// AddResponse is the response from IPFS add.
type AddResponse struct {
	Name string `json:"Name"`
	Hash string `json:"Hash"`
	Size string `json:"Size"`
}

// PinLsResponse is the response from IPFS pin ls.
type PinLsResponse struct {
	Keys map[string]PinInfo `json:"Keys"`
}

// PinInfo contains pin type information.
type PinInfo struct {
	Type string `json:"Type"`
}

// IDResponse is the response from IPFS id.
type IDResponse struct {
	ID              string   `json:"ID"`
	PublicKey        string   `json:"PublicKey"`
	Addresses       []string `json:"Addresses"`
	AgentVersion    string   `json:"AgentVersion"`
	ProtocolVersion string   `json:"ProtocolVersion"`
}

// NewClient creates a new IPFS client connected to the given node endpoints.
func NewClient(nodes []string) *Client {
	return &Client{
		nodes: nodes,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// Nodes returns the list of IPFS node endpoints.
func (c *Client) Nodes() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make([]string, len(c.nodes))
	copy(result, c.nodes)
	return result
}

// Add uploads data to an IPFS node and returns the CID.
func (c *Client) Add(nodeURL string, data []byte) (string, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	part, err := writer.CreateFormFile("file", "shard")
	if err != nil {
		return "", fmt.Errorf("creating form file: %w", err)
	}
	if _, err := part.Write(data); err != nil {
		return "", fmt.Errorf("writing data to form: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("closing multipart writer: %w", err)
	}

	url := fmt.Sprintf("%s/api/v0/add?pin=true&quieter=true", nodeURL)
	req, err := http.NewRequest("POST", url, &body)
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("adding to IPFS node %s: %w", nodeURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("IPFS add failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	var addResp AddResponse
	if err := json.NewDecoder(resp.Body).Decode(&addResp); err != nil {
		return "", fmt.Errorf("decoding add response: %w", err)
	}

	return addResp.Hash, nil
}

// Get retrieves data from an IPFS node by CID.
func (c *Client) Get(nodeURL string, cid string) ([]byte, error) {
	url := fmt.Sprintf("%s/api/v0/cat?arg=%s", nodeURL, cid)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("getting from IPFS node %s: %w", nodeURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("IPFS cat failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body: %w", err)
	}

	return data, nil
}

// Pin pins a CID on the specified IPFS node.
func (c *Client) Pin(nodeURL string, cid string) error {
	url := fmt.Sprintf("%s/api/v0/pin/add?arg=%s", nodeURL, cid)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("pinning on IPFS node %s: %w", nodeURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("IPFS pin failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// Unpin removes a pin for the given CID on the specified node.
func (c *Client) Unpin(nodeURL string, cid string) error {
	url := fmt.Sprintf("%s/api/v0/pin/rm?arg=%s", nodeURL, cid)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("unpinning on IPFS node %s: %w", nodeURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("IPFS unpin failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// IsPinned checks if a CID is pinned on the specified IPFS node.
func (c *Client) IsPinned(nodeURL string, cid string) (bool, error) {
	url := fmt.Sprintf("%s/api/v0/pin/ls?arg=%s&type=all", nodeURL, cid)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return false, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("checking pin on IPFS node %s: %w", nodeURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusInternalServerError {
		// CID not pinned
		return false, nil
	}
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("IPFS pin ls failed (status %d): %s", resp.StatusCode, string(respBody))
	}

	var pinResp PinLsResponse
	if err := json.NewDecoder(resp.Body).Decode(&pinResp); err != nil {
		return false, fmt.Errorf("decoding pin ls response: %w", err)
	}

	_, pinned := pinResp.Keys[cid]
	return pinned, nil
}

// NodeID returns the peer ID of the specified IPFS node.
func (c *Client) NodeID(nodeURL string) (string, error) {
	url := fmt.Sprintf("%s/api/v0/id", nodeURL)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return "", fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("getting ID from IPFS node %s: %w", nodeURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("IPFS id failed (status %d)", resp.StatusCode)
	}

	var idResp IDResponse
	if err := json.NewDecoder(resp.Body).Decode(&idResp); err != nil {
		return "", fmt.Errorf("decoding id response: %w", err)
	}

	return idResp.ID, nil
}

// IsReachable checks if an IPFS node is reachable.
func (c *Client) IsReachable(nodeURL string) bool {
	_, err := c.NodeID(nodeURL)
	if err != nil {
		log.Debug().Str("node", nodeURL).Err(err).Msg("IPFS node unreachable")
		return false
	}
	return true
}

// AddToAllNodes uploads data to all configured IPFS nodes and returns the CID.
// The CID should be the same across all nodes (content-addressed).
func (c *Client) AddToAllNodes(data []byte) (string, []string, error) {
	c.mu.RLock()
	nodes := make([]string, len(c.nodes))
	copy(nodes, c.nodes)
	c.mu.RUnlock()

	var cid string
	var pinnedNodes []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error

	for _, node := range nodes {
		wg.Add(1)
		go func(nodeURL string) {
			defer wg.Done()

			hash, err := c.Add(nodeURL, data)
			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				log.Error().Str("node", nodeURL).Err(err).Msg("failed to add to IPFS node")
				if firstErr == nil {
					firstErr = err
				}
				return
			}

			if cid == "" {
				cid = hash
			}
			pinnedNodes = append(pinnedNodes, nodeURL)
		}(node)
	}

	wg.Wait()

	if cid == "" {
		return "", nil, fmt.Errorf("failed to add data to any IPFS node: %v", firstErr)
	}

	return cid, pinnedNodes, nil
}

// GetFromAnyNode retrieves data from the first available IPFS node.
func (c *Client) GetFromAnyNode(cid string) ([]byte, error) {
	c.mu.RLock()
	nodes := make([]string, len(c.nodes))
	copy(nodes, c.nodes)
	c.mu.RUnlock()

	for _, node := range nodes {
		data, err := c.Get(node, cid)
		if err == nil {
			return data, nil
		}
		log.Debug().Str("node", node).Str("cid", cid).Err(err).Msg("failed to get from node, trying next")
	}

	return nil, fmt.Errorf("failed to retrieve CID %s from any IPFS node", cid)
}

// PinOnNodes ensures a CID is pinned on the specified nodes.
func (c *Client) PinOnNodes(cid string, nodeURLs []string) (int, error) {
	var pinned int
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, node := range nodeURLs {
		wg.Add(1)
		go func(nodeURL string) {
			defer wg.Done()
			if err := c.Pin(nodeURL, cid); err != nil {
				log.Error().Str("node", nodeURL).Str("cid", cid).Err(err).Msg("failed to pin")
				return
			}
			mu.Lock()
			pinned++
			mu.Unlock()
		}(node)
	}

	wg.Wait()
	return pinned, nil
}
