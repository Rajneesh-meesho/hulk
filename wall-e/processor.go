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
	// Process METRIC_TYPE_CONTROLLER and METRIC_TYPE_HTTP (both can have topology data)
	if metric.MetricType != proto.MetricType_METRIC_TYPE_CONTROLLER &&
		metric.MetricType != proto.MetricType_METRIC_TYPE_HTTP {
		return
	}

	// Check if topology data exists
	if metric.Topology == nil {
		log.Printf("Skipping metric %s: no topology data", metric.MetricId)
		return
	}

	topology := metric.Topology

	log.Printf("Processing %s metric %s for service: %s", metric.MetricType.String(), metric.MetricId, metric.Service)
	log.Printf("Topology data - UpstreamServices: %v, DownstreamServices: %v, UpstreamLinks: %v, DownstreamLinks: %v",
		topology.UpstreamServices, topology.DownstreamServices, topology.UpstreamLinks, topology.DownstreamLinks)

	// Process even if not all fields are present - just log what we're missing
	hasUpstreamServices := len(topology.UpstreamServices) > 0
	hasDownstreamServices := len(topology.DownstreamServices) > 0
	hasUpstreamLinks := len(topology.UpstreamLinks) > 0
	hasDownstreamLinks := len(topology.DownstreamLinks) > 0

	log.Printf("Field availability - US: %t, DS: %t, UL: %t, DL: %t",
		hasUpstreamServices, hasDownstreamServices, hasUpstreamLinks, hasDownstreamLinks)

	// For CONTROLLER metrics: Handle basic collections
	if metric.MetricType == proto.MetricType_METRIC_TYPE_CONTROLLER {
		log.Printf("CONTROLLER metric: Processing basic collections")

		// Process service mappings if we have any services
		if hasUpstreamServices || hasDownstreamServices {
			tp.processServiceMappings(topology.UpstreamServices, topology.DownstreamServices)
		}

		// Also add the current metric's service to service collection
		if metric.Service != "" {
			tp.data.GetServiceID(metric.Service)
			log.Printf("Added current service '%s' to collection", metric.Service)
		}

		// Process endpoint mappings if we have any links
		if hasUpstreamLinks || hasDownstreamLinks {
			tp.processEndpointMappings(topology.UpstreamLinks, topology.DownstreamLinks)
		}

		// Process service to upstream_links mapping if we have both
		if metric.Service != "" && hasUpstreamLinks {
			tp.processServiceToLinksMapping(metric.Service, topology.UpstreamLinks)
		}
	}

	// For HTTP metrics: Handle connection graph and parent collection
	if metric.MetricType == proto.MetricType_METRIC_TYPE_HTTP {
		log.Printf("HTTP metric: Processing connection graph and parent collection")

		// Also process basic collections for HTTP metrics
		if hasUpstreamServices || hasDownstreamServices {
			tp.processServiceMappings(topology.UpstreamServices, topology.DownstreamServices)
		}

		if metric.Service != "" {
			tp.data.GetServiceID(metric.Service)
			log.Printf("Added current service '%s' to collection", metric.Service)
		}

		if hasUpstreamLinks || hasDownstreamLinks {
			tp.processEndpointMappings(topology.UpstreamLinks, topology.DownstreamLinks)
		}

		// Process connection graph and parent collection for HTTP metrics
		tp.processConnectionGraphSmart(metric.Service, topology.UpstreamServices, topology.DownstreamServices, topology.UpstreamLinks, topology.DownstreamLinks)
		tp.processParentCollectionSmart(topology.UpstreamLinks, topology.DownstreamLinks)
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

// processConnectionGraphSmart handles connection graph with smart inference from partial data
func (tp *TopologyProcessor) processConnectionGraphSmart(currentService string, upstreamServices, downstreamServices, upstreamLinks, downstreamLinks []string) {
	log.Printf("Smart connection graph processing...")

	// Case 1: We have all the data - use the normal method
	if len(upstreamServices) > 0 && len(downstreamServices) > 0 && len(downstreamLinks) > 0 {
		log.Printf("Using complete data for connection graph")
		tp.processConnectionGraph(upstreamServices, downstreamServices, downstreamLinks)
		return
	}

	// Case 2: We have upstream services and links, but no downstream info
	// Infer that current service is downstream of upstream services
	if len(upstreamServices) > 0 && len(upstreamLinks) > 0 && currentService != "" {
		log.Printf("Inferring connection: upstream services -> current service")

		currentServiceID := tp.data.GetServiceID(currentService)

		// Convert upstream service names to IDs
		var upstreamServiceIDs []int
		for _, service := range upstreamServices {
			if service != "" {
				serviceID := tp.data.GetServiceID(service)
				upstreamServiceIDs = append(upstreamServiceIDs, serviceID)
			}
		}

		// Convert upstream link names to IDs
		var upstreamLinkIDs []int
		for _, link := range upstreamLinks {
			if link != "" {
				linkID := tp.data.GetEndpointID(link)
				upstreamLinkIDs = append(upstreamLinkIDs, linkID)
			}
		}

		// Add connections: upstream_services -> current_service via upstream_links
		for _, usID := range upstreamServiceIDs {
			if tp.data.ConnectionGraph[usID] == nil {
				tp.data.ConnectionGraph[usID] = make(map[int][]int)
			}

			// Use upstream links as the connection links (since that's what we have)
			for _, linkID := range upstreamLinkIDs {
				if tp.data.ConnectionGraph[usID][currentServiceID] == nil {
					tp.data.ConnectionGraph[usID][currentServiceID] = make([]int, 0)
				}

				// Check if link ID already exists
				exists := false
				for _, existingID := range tp.data.ConnectionGraph[usID][currentServiceID] {
					if existingID == linkID {
						exists = true
						break
					}
				}

				if !exists {
					tp.data.ConnectionGraph[usID][currentServiceID] = append(tp.data.ConnectionGraph[usID][currentServiceID], linkID)
					log.Printf("Added connection: service %d -> service %d via link %d", usID, currentServiceID, linkID)
				}
			}
		}
	}

	// Case 3: We have downstream services and links, but no upstream info
	// Infer that current service is upstream of downstream services
	if len(downstreamServices) > 0 && len(downstreamLinks) > 0 && currentService != "" {
		log.Printf("Inferring connection: current service -> downstream services")

		currentServiceID := tp.data.GetServiceID(currentService)

		// Convert downstream service names to IDs
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

		// Add connections: current_service -> downstream_services via downstream_links
		if tp.data.ConnectionGraph[currentServiceID] == nil {
			tp.data.ConnectionGraph[currentServiceID] = make(map[int][]int)
		}

		for i, dsID := range downstreamServiceIDs {
			if i < len(downstreamLinkIDs) {
				linkID := downstreamLinkIDs[i]

				if tp.data.ConnectionGraph[currentServiceID][dsID] == nil {
					tp.data.ConnectionGraph[currentServiceID][dsID] = make([]int, 0)
				}

				// Check if link ID already exists
				exists := false
				for _, existingID := range tp.data.ConnectionGraph[currentServiceID][dsID] {
					if existingID == linkID {
						exists = true
						break
					}
				}

				if !exists {
					tp.data.ConnectionGraph[currentServiceID][dsID] = append(tp.data.ConnectionGraph[currentServiceID][dsID], linkID)
					log.Printf("Added connection: service %d -> service %d via link %d", currentServiceID, dsID, linkID)
				}
			}
		}
	}
}

// processParentCollectionSmart handles parent collection with smart inference from partial data
func (tp *TopologyProcessor) processParentCollectionSmart(upstreamLinks, downstreamLinks []string) {
	log.Printf("Smart parent collection processing...")

	// Case 1: We have both upstream and downstream links - use normal method
	if len(upstreamLinks) > 0 && len(downstreamLinks) > 0 {
		log.Printf("Using complete data for parent collection")
		tp.processParentCollection(upstreamLinks, downstreamLinks)
		return
	}

	// Case 2: We only have upstream links - create self-referencing relationships
	// This indicates these links are "root" links with no parents
	if len(upstreamLinks) > 0 {
		log.Printf("Creating root entries for upstream links")

		var upstreamLinkIDs []int
		for _, link := range upstreamLinks {
			if link != "" {
				linkID := tp.data.GetEndpointID(link)
				upstreamLinkIDs = append(upstreamLinkIDs, linkID)
			}
		}

		// Mark these as root links (no parents)
		for _, linkID := range upstreamLinkIDs {
			if tp.data.ParentCollection[linkID] == nil {
				tp.data.ParentCollection[linkID] = make([]int, 0)
				log.Printf("Added root link %d to parent collection", linkID)
			}
		}
	}

	// Case 3: We only have downstream links - also create entries
	if len(downstreamLinks) > 0 {
		log.Printf("Creating entries for downstream links")

		var downstreamLinkIDs []int
		for _, link := range downstreamLinks {
			if link != "" {
				linkID := tp.data.GetEndpointID(link)
				downstreamLinkIDs = append(downstreamLinkIDs, linkID)
			}
		}

		// Create entries for downstream links (they might get parents later)
		for _, linkID := range downstreamLinkIDs {
			if tp.data.ParentCollection[linkID] == nil {
				tp.data.ParentCollection[linkID] = make([]int, 0)
				log.Printf("Added downstream link %d to parent collection", linkID)
			}
		}
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
