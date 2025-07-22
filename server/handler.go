package server

import (
	"context"
	"fmt"
	"hulk/proto"
	"log"
)

type MetricCollectorServer struct {
	proto.UnimplementedMetricCollectorServiceServer
}

func (s *MetricCollectorServer) SendMetric(ctx context.Context, in *proto.MetricData) (*proto.MetricResponse, error) {
	log.Printf("Received metric: %+v", in)

	err := PushToVictoriaMetrics(in)
	if err != nil {
		return &proto.MetricResponse{
			Success:  false,
			Message:  err.Error(),
			MetricId: in.MetricId,
		}, nil
	}

	return &proto.MetricResponse{
		Success:  true,
		Message:  "OK",
		MetricId: in.MetricId,
	}, nil
}

func (s *MetricCollectorServer) SendMetricsBatch(ctx context.Context, in *proto.MetricBatch) (*proto.MetricBatchResponse, error) {
	log.Printf("Received batch with %d metrics from %s", len(in.Metrics), in.SourceService)

	var processedCount int32
	var failedCount int32
	var failedMetricIds []string

	for _, metric := range in.Metrics {
		err := PushToVictoriaMetrics(metric)
		if err != nil {
			failedCount++
			failedMetricIds = append(failedMetricIds, metric.MetricId)
			log.Printf("Failed to process metric %s: %v", metric.MetricId, err)
		} else {
			log.Printf("metric %s", metric)
			processedCount++
		}
	}

	return &proto.MetricBatchResponse{
		Success:         failedCount == 0,
		Message:         fmt.Sprintf("Processed %d metrics, failed %d", processedCount, failedCount),
		ProcessedCount:  processedCount,
		FailedCount:     failedCount,
		FailedMetricIds: failedMetricIds,
	}, nil
}

func (s *MetricCollectorServer) HealthCheck(ctx context.Context, in *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	log.Printf("Health check requested for service: %s", in.Service)

	// Simple health check - you can add more sophisticated checks here
	return &proto.HealthCheckResponse{
		Status:  proto.HealthCheckResponse_SERVING,
		Message: "Service is healthy",
	}, nil
}
