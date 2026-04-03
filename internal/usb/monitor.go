package usb

import (
	"fmt"
	"log"
	"sync"
	"time"

	"hub-test/internal/adb"
	"hub-test/internal/store"
)

// Monitor polls USB devices and records appear/disappear/mode_change events.
type Monitor struct {
	st           *store.Store
	prev         map[string]adb.USBAndroidDevice // keyed by USB path
	mu           sync.Mutex
	OnModeChange func(serial, path, vid, pid string) // optional callback on VID:PID change
}

// NewMonitor creates a new USB Monitor.
func NewMonitor(st *store.Store) *Monitor {
	return &Monitor{
		st:   st,
		prev: make(map[string]adb.USBAndroidDevice),
	}
}

// Poll runs one diff cycle. Call periodically (e.g. on each reconcile tick).
func (m *Monitor) Poll() {
	current := adb.USBAndroidDevices()

	// Cross-reference with ADB to set in_adb flag.
	adbDevs, _ := adb.ListDevices()
	adbSerials := make(map[string]bool, len(adbDevs))
	for _, d := range adbDevs {
		adbSerials[d.Serial] = true
	}
	for i := range current {
		if current[i].Serial != "" {
			current[i].InADB = adbSerials[current[i].Serial]
		}
	}

	curMap := make(map[string]adb.USBAndroidDevice, len(current))
	for _, d := range current {
		curMap[d.Path] = d
	}

	now := time.Now().UTC()

	m.mu.Lock()
	defer m.mu.Unlock()

	for path, d := range curMap {
		prev, existed := m.prev[path]
		if !existed {
			// New USB path — device plugged in.
			m.record(store.USBEvent{
				TS: now, Event: "appeared",
				Path: d.Path, VID: d.VID, PID: d.PID,
				Serial: d.Serial, Product: d.Product, Vendor: d.Vendor,
				InADB: d.InADB,
			})
		} else if prev.VID != d.VID || prev.PID != d.PID {
			// Same port, different VID:PID — Android switched USB mode.
			m.record(store.USBEvent{
				TS: now, Event: "mode_change",
				Path: d.Path, VID: d.VID, PID: d.PID,
				Serial: d.Serial, Product: d.Product, Vendor: d.Vendor,
				InADB:  d.InADB,
				Detail: fmt.Sprintf("%s:%s → %s:%s", prev.VID, prev.PID, d.VID, d.PID),
			})
			if m.OnModeChange != nil && d.Serial != "" {
				m.OnModeChange(d.Serial, d.Path, d.VID, d.PID)
			}
		} else if prev.InADB != d.InADB {
			// Same device, ADB visibility changed.
			m.record(store.USBEvent{
				TS: now, Event: "mode_change",
				Path: d.Path, VID: d.VID, PID: d.PID,
				Serial: d.Serial, Product: d.Product, Vendor: d.Vendor,
				InADB:  d.InADB,
				Detail: fmt.Sprintf("adb: %v → %v", prev.InADB, d.InADB),
			})
		}
	}

	// Detect disappeared devices.
	for path, d := range m.prev {
		if _, exists := curMap[path]; !exists {
			m.record(store.USBEvent{
				TS: now, Event: "disappeared",
				Path: d.Path, VID: d.VID, PID: d.PID,
				Serial: d.Serial, Product: d.Product, Vendor: d.Vendor,
				InADB: false,
			})
		}
	}

	m.prev = curMap
}

func (m *Monitor) record(ev store.USBEvent) {
	if err := m.st.InsertUSBEvent(ev); err != nil {
		log.Printf("[usb] insert %s event: %v", ev.Event, err)
		return
	}
	switch ev.Event {
	case "appeared":
		log.Printf("[usb] appeared    %s  %s:%s  %s  adb=%v", ev.Path, ev.VID, ev.PID, ev.Product, ev.InADB)
	case "disappeared":
		log.Printf("[usb] disappeared %s  %s:%s  %s", ev.Path, ev.VID, ev.PID, ev.Product)
	case "mode_change":
		log.Printf("[usb] mode_change %s  %s  %s  adb=%v", ev.Path, ev.Detail, ev.Product, ev.InADB)
	}
}
