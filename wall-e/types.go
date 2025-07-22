package walle

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
)

// TopologyData represents all the collected topology information
type TopologyData struct {
	// map<string, int> - bidirectional mapping for services
	ServiceCollection map[string]int `json:"service_collection"`
	ServiceReverse    map[int]string `json:"service_reverse"`

	// map<string, int> - bidirectional mapping for endpoints/links
	EndpointCollection map[string]int `json:"endpoint_collection"`
	EndpointReverse    map[int]string `json:"endpoint_reverse"`

	// map<int, set<int>> - service to upstream_links mapping
	EndpointMapping map[int][]int `json:"endpoint_mapping"`

	// map<int, map<int, set<int>>> - adjacency graph
	// g[upstream_service][downstream_service] = [downstream_links...]
	ConnectionGraph map[int]map[int][]int `json:"connection_graph"`

	// map<int, set<int>> - parent collection
	// parent_collection[downstream_link] = [upstream_links...]
	ParentCollection map[int][]int `json:"parent_collection"`

	// Counters for assigning new IDs
	ServiceCounter  int `json:"service_counter"`
	EndpointCounter int `json:"endpoint_counter"`

	// Change tracking (not serialized to JSON)
	hasChanges bool         `json:"-"`
	mutex      sync.RWMutex `json:"-"`
}

// NewTopologyData creates a new instance of TopologyData
func NewTopologyData() *TopologyData {
	return &TopologyData{
		ServiceCollection:  make(map[string]int),
		ServiceReverse:     make(map[int]string),
		EndpointCollection: make(map[string]int),
		EndpointReverse:    make(map[int]string),
		EndpointMapping:    make(map[int][]int),
		ConnectionGraph:    make(map[int]map[int][]int),
		ParentCollection:   make(map[int][]int),
		ServiceCounter:     1,
		EndpointCounter:    1,
		hasChanges:         false,
	}
}

// markChanged marks that the data has been modified
func (td *TopologyData) markChanged() {
	td.hasChanges = true
}

// HasChanges returns true if data has been modified since last save
func (td *TopologyData) HasChanges() bool {
	td.mutex.RLock()
	defer td.mutex.RUnlock()
	return td.hasChanges
}

// MarkSaved marks that the data has been saved (clears change flag)
func (td *TopologyData) MarkSaved() {
	td.mutex.Lock()
	defer td.mutex.Unlock()
	td.hasChanges = false
}

// GetServiceID gets or creates an ID for a service name
// Returns (id, wasNewlyCreated)
func (td *TopologyData) GetServiceID(serviceName string) (int, bool) {
	td.mutex.Lock()
	defer td.mutex.Unlock()

	if id, exists := td.ServiceCollection[serviceName]; exists {
		return id, false
	}

	id := td.ServiceCounter
	td.ServiceCollection[serviceName] = id
	td.ServiceReverse[id] = serviceName
	td.ServiceCounter++
	td.markChanged() // Mark that data has changed
	return id, true
}

// GetEndpointID gets or creates an ID for an endpoint/link name
// Returns (id, wasNewlyCreated)
func (td *TopologyData) GetEndpointID(endpointName string) (int, bool) {
	td.mutex.Lock()
	defer td.mutex.Unlock()

	if id, exists := td.EndpointCollection[endpointName]; exists {
		return id, false
	}

	id := td.EndpointCounter
	td.EndpointCollection[endpointName] = id
	td.EndpointReverse[id] = endpointName
	td.EndpointCounter++
	td.markChanged() // Mark that data has changed
	return id, true
}

// AddToEndpointMapping adds upstream links to service mapping
func (td *TopologyData) AddToEndpointMapping(serviceID int, upstreamLinkIDs []int) {
	td.mutex.Lock()
	defer td.mutex.Unlock()

	if td.EndpointMapping[serviceID] == nil {
		td.EndpointMapping[serviceID] = make([]int, 0)
	}

	// Add unique upstream link IDs
	existingMap := make(map[int]bool)
	for _, id := range td.EndpointMapping[serviceID] {
		existingMap[id] = true
	}

	changed := false
	for _, linkID := range upstreamLinkIDs {
		if !existingMap[linkID] {
			td.EndpointMapping[serviceID] = append(td.EndpointMapping[serviceID], linkID)
			existingMap[linkID] = true
			changed = true
		}
	}

	if changed {
		td.markChanged()
	}
}

// AddToConnectionGraph adds connections to the adjacency graph
func (td *TopologyData) AddToConnectionGraph(upstreamServiceIDs, downstreamServiceIDs, downstreamLinkIDs []int) {
	td.mutex.Lock()
	defer td.mutex.Unlock()

	changed := false
	for _, usID := range upstreamServiceIDs {
		if td.ConnectionGraph[usID] == nil {
			td.ConnectionGraph[usID] = make(map[int][]int)
		}

		for i, dsID := range downstreamServiceIDs {
			if i < len(downstreamLinkIDs) {
				dlID := downstreamLinkIDs[i]

				if td.ConnectionGraph[usID][dsID] == nil {
					td.ConnectionGraph[usID][dsID] = make([]int, 0)
				}

				// Check if downstream link ID already exists
				exists := false
				for _, existingID := range td.ConnectionGraph[usID][dsID] {
					if existingID == dlID {
						exists = true
						break
					}
				}

				if !exists {
					td.ConnectionGraph[usID][dsID] = append(td.ConnectionGraph[usID][dsID], dlID)
					changed = true
				}
			}
		}
	}

	if changed {
		td.markChanged()
	}
}

// AddToParentCollection adds parent-child relationships for links
func (td *TopologyData) AddToParentCollection(upstreamLinkIDs, downstreamLinkIDs []int) {
	td.mutex.Lock()
	defer td.mutex.Unlock()

	changed := false
	for _, dlID := range downstreamLinkIDs {
		if td.ParentCollection[dlID] == nil {
			td.ParentCollection[dlID] = make([]int, 0)
		}

		// Create a map for existing upstream links to avoid duplicates
		existingMap := make(map[int]bool)
		for _, id := range td.ParentCollection[dlID] {
			existingMap[id] = true
		}

		// Add all upstream link IDs
		for _, ulID := range upstreamLinkIDs {
			if !existingMap[ulID] {
				td.ParentCollection[dlID] = append(td.ParentCollection[dlID], ulID)
				existingMap[ulID] = true
				changed = true
			}
		}
	}

	if changed {
		td.markChanged()
	}
}

// ToJSON converts the topology data to JSON bytes
func (td *TopologyData) ToJSON() ([]byte, error) {
	td.mutex.RLock()
	defer td.mutex.RUnlock()
	return json.MarshalIndent(td, "", "  ")
}

// FromJSON loads topology data from JSON bytes
func (td *TopologyData) FromJSON(data []byte) error {
	td.mutex.Lock()
	defer td.mutex.Unlock()
	return json.Unmarshal(data, td)
}

// LoadFromFile loads topology data from a JSON file
func (td *TopologyData) LoadFromFile(filename string) error {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		// File doesn't exist, start with empty data
		return nil
	}

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	return td.FromJSON(data)
}

// SaveToFile saves topology data to a JSON file
func (td *TopologyData) SaveToFile(filename string) error {
	data, err := td.ToJSON()
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(filename, data, 0644)
	if err == nil {
		td.MarkSaved() // Clear change flag after successful save
	}
	return err
}
