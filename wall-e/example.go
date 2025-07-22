package walle

import (
	"hulk/proto"
	"log"
)

// ExampleUsage demonstrates how to use the topology data collection
func ExampleUsage() {
	// Create sample metric data
	sampleMetric := &proto.MetricData{
		MetricId:   "test-metric-1",
		Service:    "api-service",
		MetricType: proto.MetricType_METRIC_TYPE_CONTROLLER,
		Topology: &proto.ServiceTopology{
			UpstreamServices:   []string{"auth-service", "user-service"},
			DownstreamServices: []string{"db-service", "cache-service"},
			UpstreamLinks:      []string{"/api/auth", "/api/users"},
			DownstreamLinks:    []string{"/db/query", "/cache/get"},
		},
	}

	// Get the topology manager instance
	manager := GetInstance()

	// Process the metric
	manager.ProcessMetric(sampleMetric)

	// Get the topology data
	data := manager.GetTopologyData()

	// Log the results
	log.Printf("Service Collection: %+v", data.ServiceCollection)
	log.Printf("Endpoint Collection: %+v", data.EndpointCollection)
	log.Printf("Endpoint Mapping: %+v", data.EndpointMapping)
	log.Printf("Connection Graph: %+v", data.ConnectionGraph)
	log.Printf("Parent Collection: %+v", data.ParentCollection)

	// Force save to file
	if err := manager.ForceSave(); err != nil {
		log.Printf("Error saving: %v", err)
	} else {
		log.Println("Data saved to connection.json")
	}
}
