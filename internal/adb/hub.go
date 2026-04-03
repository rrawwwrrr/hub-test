package adb

import (
	"context"

	"xfarm.hub/hub-ws-client"
)

// DeviceEventHandler receives device lifecycle events from hub-server.
// Re-exported from xfarm.hub/hub-ws-client for use within hub-test.
type DeviceEventHandler = hubws.DeviceEventHandler

// BaseHandler provides empty default implementations of DeviceEventHandler.
type BaseHandler = hubws.BaseHandler

// TrackHubDevices connects to hub-server WebSocket and dispatches events to handler.
// Blocks until ctx is cancelled; reconnects automatically.
func TrackHubDevices(ctx context.Context, hubURL, namespace string, handler DeviceEventHandler, onReconnect func()) {
	client := hubws.New(hubURL, namespace)
	// onReconnect not supported in library — wrap if needed
	client.Connect(ctx, handler)
}

// PeerHost returns the in-cluster DNS hostname for a peer pod.
func PeerHost(serial, namespace string) string {
	return hubws.New("", namespace).PeerHost(serial)
}
