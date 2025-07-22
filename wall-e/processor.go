package walle

import (
	"hulk/proto"
	"log"
)

// TopologyProcessor handles processing of topology data from gRPC metrics
type TopologyProcessor struct {
	data *TopologyData
}

// NewTopologyProcessor creates a new topology processor
func NewTopologyProcessor() *TopologyProcessor {
	return &TopologyProcessor{
		data: NewTopologyData(),
	}
}

// ProcessMetric processes a single metric data for topology information
func (tp *TopologyProcessor) ProcessMetric(metric *proto.MetricData) {
	// Only process METRIC_TYPE_CONTROLLER
	if metric.MetricType != proto.MetricType_METRIC_TYPE_CONTROLLER {
		return
	}

	// Check if topology data exists
	if metric.Topology == nil {
		log.Printf("Skipping metric %s: no topology data", metric.MetricId)
		return
	}

	topology := metric.Topology

	log.Printf("Processing CONTROLLER metric %s for service: %s", metric.MetricId, metric.Service)
	log.Printf("Topology data - UpstreamServices: %v, DownstreamServices: %v, UpstreamLinks: %v, DownstreamLinks: %v",
		topology.UpstreamServices, topology.DownstreamServices, topology.UpstreamLinks, topology.DownstreamLinks)

	// Process even if not all fields are present - just log what we're missing
	hasUpstreamServices := len(topology.UpstreamServices) > 0
	hasDownstreamServices := len(topology.DownstreamServices) > 0
	hasUpstreamLinks := len(topology.UpstreamLinks) > 0
	hasDownstreamLinks := len(topology.DownstreamLinks) > 0

	log.Printf("Field availability - US: %t, DS: %t, UL: %t, DL: %t",
		hasUpstreamServices, hasDownstreamServices, hasUpstreamLinks, hasDownstreamLinks)

	// Process service mappings if we have any services
	if hasUpstreamServices || hasDownstreamServices {
		tp.processServiceMappings(topology.UpstreamServices, topology.DownstreamServices)
	}

	// Process endpoint mappings if we have any links
	if hasUpstreamLinks || hasDownstreamLinks {
		tp.processEndpointMappings(topology.UpstreamLinks, topology.DownstreamLinks)
	}

	// Process service to upstream_links mapping if we have both
	if metric.Service != "" && hasUpstreamLinks {
		tp.processServiceToLinksMapping(metric.Service, topology.UpstreamLinks)
	}

	// Process connection graph only if we have all required fields
	if hasUpstreamServices && hasDownstreamServices && hasDownstreamLinks {
		tp.processConnectionGraph(topology.UpstreamServices, topology.DownstreamServices, topology.DownstreamLinks)
	} else {
		log.Printf("Skipping connection graph: missing required fields")
	}

	// Process parent collection only if we have both upstream and downstream links
	if hasUpstreamLinks && hasDownstreamLinks {
		tp.processParentCollection(topology.UpstreamLinks, topology.DownstreamLinks)
	} else {
		log.Printf("Skipping parent collection: missing link fields")
	}

	log.Printf("Finished processing metric %s", metric.MetricId)
}

// processServiceMappings handles service name to ID mappings
func (tp *TopologyProcessor) processServiceMappings(upstreamServices, downstreamServices []string) {
	// Process upstream services
	for _, service := range upstreamServices {
		if service != "" {
			tp.data.GetServiceID(service)
		}
	}

	// Process downstream services
	for _, service := range downstreamServices {
		if service != "" {
			tp.data.GetServiceID(service)
		}
	}
}

// processEndpointMappings handles endpoint/link name to ID mappings
func (tp *TopologyProcessor) processEndpointMappings(upstreamLinks, downstreamLinks []string) {
	// Process upstream links
	for _, link := range upstreamLinks {
		if link != "" {
			tp.data.GetEndpointID(link)
		}
	}

	// Process downstream links
	for _, link := range downstreamLinks {
		if link != "" {
			tp.data.GetEndpointID(link)
		}
	}
}

// processServiceToLinksMapping handles service to upstream_links mapping
func (tp *TopologyProcessor) processServiceToLinksMapping(serviceName string, upstreamLinks []string) {
	if serviceName == "" || len(upstreamLinks) == 0 {
		return
	}

	serviceID := tp.data.GetServiceID(serviceName)

	var upstreamLinkIDs []int
	for _, link := range upstreamLinks {
		if link != "" {
			linkID := tp.data.GetEndpointID(link)
			upstreamLinkIDs = append(upstreamLinkIDs, linkID)
		}
	}

	if len(upstreamLinkIDs) > 0 {
		tp.data.AddToEndpointMapping(serviceID, upstreamLinkIDs)
	}
}

// processConnectionGraph handles the adjacency graph creation
func (tp *TopologyProcessor) processConnectionGraph(upstreamServices, downstreamServices, downstreamLinks []string) {
	if len(upstreamServices) == 0 || len(downstreamServices) == 0 || len(downstreamLinks) == 0 {
		return
	}

	// Convert service names to IDs
	var upstreamServiceIDs []int
	for _, service := range upstreamServices {
		if service != "" {
			serviceID := tp.data.GetServiceID(service)
			upstreamServiceIDs = append(upstreamServiceIDs, serviceID)
		}
	}

	var downstreamServiceIDs []int
	for _, service := range downstreamServices {
		if service != "" {
			serviceID := tp.data.GetServiceID(service)
			downstreamServiceIDs = append(downstreamServiceIDs, serviceID)
		}
	}

	// Convert downstream link names to IDs
	var downstreamLinkIDs []int
	for _, link := range downstreamLinks {
		if link != "" {
			linkID := tp.data.GetEndpointID(link)
			downstreamLinkIDs = append(downstreamLinkIDs, linkID)
		}
	}

	if len(upstreamServiceIDs) > 0 && len(downstreamServiceIDs) > 0 && len(downstreamLinkIDs) > 0 {
		tp.data.AddToConnectionGraph(upstreamServiceIDs, downstreamServiceIDs, downstreamLinkIDs)
	}
}

// processParentCollection handles parent-child relationships for links
func (tp *TopologyProcessor) processParentCollection(upstreamLinks, downstreamLinks []string) {
	if len(upstreamLinks) == 0 || len(downstreamLinks) == 0 {
		return
	}

	// Convert upstream link names to IDs
	var upstreamLinkIDs []int
	for _, link := range upstreamLinks {
		if link != "" {
			linkID := tp.data.GetEndpointID(link)
			upstreamLinkIDs = append(upstreamLinkIDs, linkID)
		}
	}

	// Convert downstream link names to IDs
	var downstreamLinkIDs []int
	for _, link := range downstreamLinks {
		if link != "" {
			linkID := tp.data.GetEndpointID(link)
			downstreamLinkIDs = append(downstreamLinkIDs, linkID)
		}
	}

	if len(upstreamLinkIDs) > 0 && len(downstreamLinkIDs) > 0 {
		tp.data.AddToParentCollection(upstreamLinkIDs, downstreamLinkIDs)
	}
}

// GetTopologyData returns the current topology data
func (tp *TopologyProcessor) GetTopologyData() *TopologyData {
	return tp.data
}

// LoadFromFile loads topology data from a file
func (tp *TopologyProcessor) LoadFromFile(filename string) error {
	return tp.data.LoadFromFile(filename)
}

// SaveToFile saves topology data to a file
func (tp *TopologyProcessor) SaveToFile(filename string) error {
	return tp.data.SaveToFile(filename)
}
