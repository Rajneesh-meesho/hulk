package server

import (
	"context"
	"hulk/proto/proto"
	"log"
)

type MetricService struct {
	proto.UnimplementedMetricLoggerServer
}

func (s *MetricService) PushMetric(ctx context.Context, in *proto.MetricData) (*proto.Response, error) {
	log.Printf("Received metric: %s from %s data %s", in.MethodName, in.UpstreamService, in)

	err := PushToVictoriaMetrics(in)
	if err != nil {
		return &proto.Response{Success: false, Message: err.Error()}, nil
	}

	return &proto.Response{Success: true, Message: "OK"}, nil
}
