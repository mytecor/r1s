package client_test

import (
	"context"
	"log"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/client"
)

func ExampleClient_Run() {
	ctx := context.Background()
	controller, err := client.Open("0123456789ab", client.Config{})
	if err != nil {
		log.Fatal(err)
	}
	defer controller.Close()
	if err := controller.Start(ctx); err != nil {
		log.Fatal(err)
	}
	result, err := controller.Run(ctx, &r1sv1.ExecutionRequest{
		Workload:      &r1sv1.Workload{Image: "registry.example/image@sha256:..."},
		Policy:        &r1sv1.ExecutionPolicy{},
		ResourceClass: "default",
	}, client.RunOptions{})
	if err != nil {
		log.Fatal(err)
	}
	_, _ = result.ExitStatus()
}

func ExampleRunTunnel() {
	ctx := context.Background()
	controller, err := client.OpenCurrent(client.Config{})
	if err != nil {
		log.Fatal(err)
	}
	defer controller.Close()
	if err := controller.Start(ctx); err != nil {
		log.Fatal(err)
	}
	runTunnel, err := controller.NewRunTunnel([]client.TunnelTarget{{Port: 9000}})
	if err != nil {
		log.Fatal(err)
	}
	defer runTunnel.Close()

	_, _ = controller.Run(ctx, &r1sv1.ExecutionRequest{
		Workload:      &r1sv1.Workload{Image: "registry.example/image@sha256:..."},
		Policy:        &r1sv1.ExecutionPolicy{},
		ResourceClass: "default",
	}, client.RunOptions{OnEvent: func(event client.Event) {
		if event.Kind == client.EventAttemptAssigned {
			runTunnel.SetActive(event.Attempt.ExecutionID)
			go func() {
				stream, err := runTunnel.Dial(ctx, 9000)
				if err != nil {
					return
				}
				defer stream.Close()
				// Speak any application protocol over stream.
			}()
		}
	}})
}
