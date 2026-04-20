// Package hubws provides a WebSocket client for hub-server device events.
//
// Usage:
//
//	client := hubws.New("http://hub-server:8080", "peer")
//	client.Connect(ctx, myHandler)
package hubws

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// HubDevice represents a device tracked by hub-server.
type HubDevice struct {
	Serial          string                 `json:"serial"`
	Type            string                 `json:"type"`   // "android" | "ios"
	Status          string                 `json:"Status"` // "starting" | "running" | "error" | "stopped"
	StatusMsg       string                 `json:"StatusMsg"`
	ContainerStatus ContainerStatus        `json:"containerStatus"`
	Info            map[string]interface{} `json:"Info"`
	LastSeen        string                 `json:"LastSeen"`
}

// ContainerStatus holds K8s pod container state.
type ContainerStatus struct {
	State   string `json:"state"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// Platform returns the device platform ("android" or "ios").
func (d *HubDevice) Platform() string {
	if d.Type != "" {
		return d.Type
	}
	return "android"
}

// IsAndroid returns true if the device is Android.
func (d *HubDevice) IsAndroid() bool { return d.Type == "android" }

// IsIOS returns true if the device is iOS.
func (d *HubDevice) IsIOS() bool { return d.Type == "ios" }

// Model returns the device model from Info, or empty string.
func (d *HubDevice) Model() string {
	if d.Info == nil {
		return ""
	}
	if m, ok := d.Info["model"].(string); ok {
		return m
	}
	return ""
}

// HasInfo returns true if the peer pod has sent device info.
func (d *HubDevice) HasInfo() bool {
	return len(d.Info) > 0
}

// DeviceEventHandler receives device lifecycle events from hub-server WebSocket.
// All methods have empty default implementations — override only what you need.
type DeviceEventHandler interface {
	// OnWelcome is called once on connect with the full current device list.
	OnWelcome(devices []*HubDevice)

	// OnDeviceConnected is called when a device first appears (DEVICE ADD).
	// The peer pod has been created but is not yet ready for testing.
	OnDeviceConnected(device *HubDevice)

	// OnDeviceReady is called when the peer pod has sent device info (INFO ADD).
	// This is the signal to start tests.
	OnDeviceReady(device *HubDevice)

	// OnDeviceOffline is called when the device disconnects physically (INFO DELETE).
	// Fires immediately when the USB device is unplugged.
	OnDeviceOffline(serial, platform string)

	// OnDeviceDeleted is called when the device is fully removed from hub-server (DEVICE DELETE).
	// Fires ~1 minute after disconnect when the peer pod is torn down.
	OnDeviceDeleted(serial, platform string)

	// OnBatteryUpdated is called when hub-server receives a battery update from the peer pod.
	// level is percentage (0-100), temp is in tenths of a degree Celsius.
	OnBatteryUpdated(serial, platform string, level, temp int)
}

// BaseHandler provides empty default implementations of DeviceEventHandler.
// Embed this in your handler struct to avoid implementing unused methods.
type BaseHandler struct{}

func (BaseHandler) OnWelcome(_ []*HubDevice)               {}
func (BaseHandler) OnDeviceConnected(_ *HubDevice)         {}
func (BaseHandler) OnDeviceReady(_ *HubDevice)             {}
func (BaseHandler) OnDeviceOffline(_, _ string)            {}
func (BaseHandler) OnDeviceDeleted(_, _ string)            {}
func (BaseHandler) OnBatteryUpdated(_, _ string, _, _ int) {}

// wsMsg is the envelope for all hub-server WebSocket messages.
type wsMsg struct {
	Type     string          `json:"type"`
	Event    string          `json:"event"`
	Device   string          `json:"device"`
	Platform string          `json:"platform"`
	Data     json.RawMessage `json:"data"`
}

// Client is a reconnecting WebSocket client for hub-server.
type Client struct {
	hubURL    string
	namespace string
}

// New creates a new Client.
// hubURL is the hub-server base URL (e.g. "http://hub-server:8080").
// namespace is the K8s namespace used to build peer pod hostnames.
func New(hubURL, namespace string) *Client {
	return &Client{hubURL: hubURL, namespace: namespace}
}

// Connect blocks, dispatching events to handler until ctx is cancelled.
// Reconnects automatically on disconnect.
func (c *Client) Connect(ctx context.Context, handler DeviceEventHandler) {
	wsURL := strings.Replace(c.hubURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL += "/api/v1/devices/ws"

	for {
		if ctx.Err() != nil {
			return
		}
		err := c.runOnce(ctx, wsURL, handler)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("[hub-ws] disconnected: %v — reconnecting in 5s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// PeerHost returns the in-cluster DNS hostname of the peer pod for a given serial.
func (c *Client) PeerHost(serial string) string {
	return fmt.Sprintf("peer-%s.%s.svc.cluster.local", serial, c.namespace)
}

// runOnce runs a single WebSocket session until disconnect or ctx cancel.
func (c *Client) runOnce(ctx context.Context, wsURL string, handler DeviceEventHandler) error {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", wsURL, err)
	}
	defer conn.Close()
	log.Printf("[hub-ws] connected to %s", wsURL)

	var mu sync.Mutex
	devices := make(map[string]*HubDevice)

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	for {
		var msg wsMsg
		if err := conn.ReadJSON(&msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		switch msg.Event {
		case "welcome":
			var data struct {
				Devices []*HubDevice `json:"devices"`
			}
			if err := json.Unmarshal(msg.Data, &data); err != nil {
				log.Printf("[hub-ws] welcome parse: %v", err)
				continue
			}
			mu.Lock()
			devices = make(map[string]*HubDevice, len(data.Devices))
			for _, d := range data.Devices {
				if d.Serial != "" {
					devices[d.Serial] = d
				}
			}
			snapshot := make([]*HubDevice, 0, len(devices))
			for _, d := range devices {
				snapshot = append(snapshot, d)
			}
			mu.Unlock()
			log.Printf("[hub-ws] welcome: %d device(s)", len(snapshot))
			handler.OnWelcome(snapshot)
			for _, d := range snapshot {
				if d.HasInfo() {
					handler.OnDeviceReady(d)
				} else {
					handler.OnDeviceConnected(d)
				}
			}

		case "ADD":
			switch msg.Type {
			case "DEVICE":
				if msg.Device == "" {
					continue
				}
				d := &HubDevice{Serial: msg.Device, Type: msg.Platform}
				if len(msg.Data) > 2 {
					_ = json.Unmarshal(msg.Data, d)
				}
				if msg.Platform != "" {
					d.Type = msg.Platform
				}
				mu.Lock()
				devices[msg.Device] = d
				mu.Unlock()
				log.Printf("[hub-ws] DEVICE ADD: %s (platform=%s)", msg.Device, d.Platform())
				handler.OnDeviceConnected(d)

			case "INFO":
				if msg.Device == "" {
					continue
				}
				var info map[string]interface{}
				if err := json.Unmarshal(msg.Data, &info); err != nil {
					log.Printf("[hub-ws] INFO ADD parse: %v", err)
					continue
				}
				mu.Lock()
				d, ok := devices[msg.Device]
				if ok {
					d.Info = info
				}
				mu.Unlock()
				if !ok {
					log.Printf("[hub-ws] INFO ADD for unknown device %s", msg.Device)
					continue
				}
				log.Printf("[hub-ws] INFO ADD: %s (platform=%s) — device ready", msg.Device, d.Platform())
				handler.OnDeviceReady(d)
			}

		case "DELETE":
			if msg.Device == "" {
				continue
			}
			mu.Lock()
			d, ok := devices[msg.Device]
			platform := msg.Platform
			if ok {
				platform = d.Platform()
			}
			if msg.Type == "DEVICE" {
				delete(devices, msg.Device)
			}
			mu.Unlock()

			if msg.Type == "INFO" {
				log.Printf("[hub-ws] INFO DELETE: %s (platform=%s) — device offline", msg.Device, platform)
				handler.OnDeviceOffline(msg.Device, platform)
			} else if msg.Type == "DEVICE" {
				log.Printf("[hub-ws] DEVICE DELETE: %s (platform=%s)", msg.Device, platform)
				handler.OnDeviceDeleted(msg.Device, platform)
			}

		case "UPDATE":
			if msg.Device == "" {
				continue
			}
			switch msg.Type {
			case "STATUS":
				if len(msg.Data) > 2 {
					var upd HubDevice
					if err := json.Unmarshal(msg.Data, &upd); err == nil {
						mu.Lock()
						if d, ok := devices[msg.Device]; ok {
							d.Status = upd.Status
							d.ContainerStatus = upd.ContainerStatus
						}
						mu.Unlock()
						log.Printf("[hub-ws] STATUS UPDATE: %s → %s", msg.Device, upd.Status)
					}
				}
			case "BATTERY":
				if len(msg.Data) > 2 {
					var bat struct {
						Level       int `json:"level"`
						Temperature int `json:"temperature"`
					}
					if err := json.Unmarshal(msg.Data, &bat); err == nil {
						mu.Lock()
						platform := msg.Platform
						if d, ok := devices[msg.Device]; ok {
							platform = d.Platform()
						}
						mu.Unlock()
						log.Printf("[hub-ws] BATTERY UPDATE: %s level=%d%%", msg.Device, bat.Level)
						handler.OnBatteryUpdated(msg.Device, platform, bat.Level, bat.Temperature)
					}
				}
			}

		case "heartbeat":
			// ignore
		}
	}
}
