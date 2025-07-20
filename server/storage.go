package server

import (
	"fmt"
	"hulk/proto/proto"
	"net/http"
	"strings"
	"time"
)

func PushToVictoriaMetrics(m *proto.MetricData) error {
	// Join repeated fields with commas
	upstreamServices := strings.Join(m.UpstreamService, ",")
	downstreamServices := strings.Join(m.DownstreamService, ",")
	upstreamServiceLinks := strings.Join(m.UpstreamServiceLink, ",")
	downstreamServiceLinks := strings.Join(m.DownstreamServiceLink, ",")

	line := fmt.Sprintf(
		`digest_logger{metricType="%s",method="%s",upstreamService="%s",downstreamService="%s",upstreamServiceLink="%s",downstreamServiceLink="%s",service="%s"} %f %d`,
		m.MetricType, m.MethodName, upstreamServices, downstreamServices,
		upstreamServiceLinks, downstreamServiceLinks, m.Service,
		m.Value, time.Now().Unix()*1000,
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
