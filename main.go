package main

import (
	pb "hulk/proto"
	"hulk/server"
	walle "hulk/wall-e"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Check if we're in test mode
	if os.Getenv("TEST_MODE") == "true" {
		log.Println("Running in test mode...")
		walle.ExampleUsage()
		return
	}

	// Start the topology manager
	topologyManager := walle.GetInstance()
	if err := topologyManager.Start(); err != nil {
		log.Fatalf("Failed to start topology manager: %v", err)
	}

	// Set up graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Shutting down gracefully...")
		if err := topologyManager.Stop(); err != nil {
			log.Printf("Error stopping topology manager: %v", err)
		}
		os.Exit(0)
	}()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMetricCollectorServiceServer(grpcServer, &server.MetricCollectorServer{})
	reflection.Register(grpcServer)

	log.Println("gRPC server running on port :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
