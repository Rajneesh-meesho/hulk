package walle

import (
	"hulk/proto"
	"log"
	"sync"
	"time"
)

// TopologyManager manages the topology processor and handles change-based persistence
type TopologyManager struct {
	processor     *TopologyProcessor
	filename      string
	checkInterval time.Duration // How often to check for changes
	mutex         sync.RWMutex
	stopChan      chan bool
	isRunning     bool
}

var (
	instance *TopologyManager
	once     sync.Once
)

// GetInstance returns the singleton instance of TopologyManager
func GetInstance() *TopologyManager {
	once.Do(func() {
		instance = &TopologyManager{
			processor:     NewTopologyProcessor(),
			filename:      "connection.json",
			checkInterval: 5 * time.Second, // Check for changes every 5 seconds
			stopChan:      make(chan bool),
			isRunning:     false,
		}
	})
	return instance
}

// Start starts the topology manager with change-based saving
func (tm *TopologyManager) Start() error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if tm.isRunning {
		return nil
	}

	// Load existing data
	if err := tm.processor.LoadFromFile(tm.filename); err != nil {
		log.Printf("Warning: Could not load existing topology data: %v", err)
	}

	tm.isRunning = true

	// Start change-checking goroutine
	go tm.changeBasedSave()

	log.Printf("Topology manager started with change-based saving, checking for changes every %v", tm.checkInterval)
	return nil
}

// Stop stops the topology manager
func (tm *TopologyManager) Stop() error {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()

	if !tm.isRunning {
		return nil
	}

	tm.stopChan <- true
	tm.isRunning = false

	// Save final state
	if err := tm.processor.SaveToFile(tm.filename); err != nil {
		log.Printf("Error saving final topology data: %v", err)
		return err
	}

	log.Println("Topology manager stopped")
	return nil
}

// ProcessMetric processes a metric through the topology processor
func (tm *TopologyManager) ProcessMetric(metric *proto.MetricData) {
	log.Printf("TopologyManager: Processing metric %s (type: %s)", metric.MetricId, metric.MetricType.String())

	// Get data before processing
	dataBefore := tm.processor.GetTopologyData()
	beforeCount := len(dataBefore.ServiceCollection) + len(dataBefore.EndpointCollection)

	tm.processor.ProcessMetric(metric)

	// Get data after processing
	dataAfter := tm.processor.GetTopologyData()
	afterCount := len(dataAfter.ServiceCollection) + len(dataAfter.EndpointCollection)

	if afterCount > beforeCount {
		log.Printf("TopologyManager: Data updated - Services: %d, Endpoints: %d",
			len(dataAfter.ServiceCollection), len(dataAfter.EndpointCollection))
	}

	// Check if we should save immediately due to significant changes
	if dataAfter.HasChanges() {
		log.Printf("TopologyManager: Changes detected, will save on next check cycle")
	}
}

// changeBasedSave handles checking for changes and saving only when needed
func (tm *TopologyManager) changeBasedSave() {
	ticker := time.NewTicker(tm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			data := tm.processor.GetTopologyData()

			// Only save if there are changes
			if data.HasChanges() {
				serviceCount := len(data.ServiceCollection)
				endpointCount := len(data.EndpointCollection)
				graphCount := len(data.ConnectionGraph)
				parentCount := len(data.ParentCollection)

				if err := tm.processor.SaveToFile(tm.filename); err != nil {
					log.Printf("Error saving topology data: %v", err)
				} else {
					data.MarkSaved() // Clear the change flag
					log.Printf("Topology data saved to %s - Services: %d, Endpoints: %d, Graphs: %d, Parents: %d",
						tm.filename, serviceCount, endpointCount, graphCount, parentCount)
				}
			}
		case <-tm.stopChan:
			return
		}
	}
}

// GetTopologyData returns the current topology data (read-only)
func (tm *TopologyManager) GetTopologyData() *TopologyData {
	return tm.processor.GetTopologyData()
}

// SetCheckInterval sets the interval for checking changes
func (tm *TopologyManager) SetCheckInterval(interval time.Duration) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.checkInterval = interval
}

// SetFilename sets the filename for saving topology data
func (tm *TopologyManager) SetFilename(filename string) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.filename = filename
}

// ForceSave forces an immediate save of topology data
func (tm *TopologyManager) ForceSave() error {
	return tm.processor.SaveToFile(tm.filename)
}
