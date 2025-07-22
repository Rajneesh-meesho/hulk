package server

import (
	"fmt"
	"hulk/proto"
	"net/http"
	"strings"
	"time"
)

func PushToVictoriaMetrics(m *proto.MetricData) error {
	// Handle timestamp - use provided timestamp or current time
	var ts int64
	if m.Timestamp > 0 {
		ts = m.Timestamp
	} else {
		ts = time.Now().Unix() * 1000
	}

	// Extract topology information
	var upstreamServices, downstreamServices, upstreamLinks, downstreamLinks string
	if m.Topology != nil {
		upstreamServices = strings.Join(m.Topology.UpstreamServices, ",")
		downstreamServices = strings.Join(m.Topology.DownstreamServices, ",")
		upstreamLinks = strings.Join(m.Topology.UpstreamLinks, ",")
		downstreamLinks = strings.Join(m.Topology.DownstreamLinks, ",")
	}

	// Convert MetricType enum to string
	metricTypeStr := m.MetricType.String()

	// Build attributes string from map
	var attributesStr []string
	for key, value := range m.Attributes {
		attributesStr = append(attributesStr, fmt.Sprintf("%s=%s", key, value))
	}
	attributes := strings.Join(attributesStr, ",")

	// Build metadata string from map
	var metadataStr []string
	for key, value := range m.Metadata {
		metadataStr = append(metadataStr, fmt.Sprintf("%s=%s", key, value))
	}
	metadata := strings.Join(metadataStr, ",")

	// Create Prometheus line with comprehensive labels
	line := fmt.Sprintf(
		`digest_logger{metric_id="%s",metric_type="%s",service="%s",operation="%s",success="%t",upstream_services="%s",downstream_services="%s",upstream_links="%s",downstream_links="%s",application="%s",environment="%s",version="%s",endpoint="%s",attributes="%s",metadata="%s"} %d %d`,
		m.MetricId, metricTypeStr, m.Service, m.Operation, m.Success,
		upstreamServices, downstreamServices, upstreamLinks, downstreamLinks,
		m.Application, m.Environment, m.Version, m.Endpoint, attributes, metadata,
		m.LatencyMs, ts,
	)

	body := strings.NewReader(line)
	resp, err := http.Post("http://localhost:8428/api/v1/import/prometheus", "text/plain", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("VictoriaMetrics responded with status: %d", resp.StatusCode)
	}
	return nil
}
