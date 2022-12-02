package main

import (
	"context"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	apiv1 "github.com/uber/cadence-idl/go/proto/api/v1"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/compatibility"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/grpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"crypto/tls"
	"crypto/x509"
	"io/ioutil"

	"go.uber.org/yarpc/peer"
	"go.uber.org/yarpc/peer/hostport"
	"google.golang.org/grpc/credentials"
)

var HostPort = "cadence.lancetest.com:7833"
var Domain = "sample-domain"
var TaskListName = "helloWorldGroup"
var ClientName = "sample_worker"
var CadenceService = "cadence-frontend"

func main() {

	startWorkflow(buildCadenceClient(), HelloWorldWorkflow, "Cadence World")

	worker := getWorker(buildLogger(), buildCadenceClient())
	worker.RegisterActivity(sayHello)
	worker.RegisterWorkflow(HelloWorldWorkflow)
	worker.Run()
}

func buildLogger() *zap.Logger {
	config := zap.NewDevelopmentConfig()
	config.Level.SetLevel(zapcore.InfoLevel)

	var err error
	logger, err := config.Build()
	if err != nil {
		panic("Failed to setup logger")
	}

	return logger
}

func buildCadenceClient() workflowserviceclient.Interface {

	grpcTransport := grpc.NewTransport()
	var dialOptions []grpc.DialOption

	// Load server CA
	caCert, err := ioutil.ReadFile("cluster-ca-certificate.pem")
	if err != nil {
		fmt.Printf("Failed to load server CA certificate: %v", zap.Error(err))
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		fmt.Errorf("Failed to add server CA's certificate")
	}

	tlsConfig := tls.Config{
		RootCAs: caCertPool,
	}
	creds := credentials.NewTLS(&tlsConfig)
	dialOptions = append(dialOptions, grpc.DialerCredentials(creds))

	dialer := grpcTransport.NewDialer(dialOptions...)
	outbound := grpcTransport.NewOutbound(peer.NewSingle(hostport.PeerIdentifier(HostPort), dialer))

	dispatcher := yarpc.NewDispatcher(yarpc.Config{
		Name: ClientName,
		Outbounds: yarpc.Outbounds{
			CadenceService: {Unary: outbound},
		},
	})
	if err := dispatcher.Start(); err != nil {
		panic("Failed to start dispatcher")
	}

	clientConfig := dispatcher.ClientConfig(CadenceService)

	return compatibility.NewThrift2ProtoAdapter(
		apiv1.NewDomainAPIYARPCClient(clientConfig),
		apiv1.NewWorkflowAPIYARPCClient(clientConfig),
		apiv1.NewWorkerAPIYARPCClient(clientConfig),
		apiv1.NewVisibilityAPIYARPCClient(clientConfig),
	)
}

func getWorker(logger *zap.Logger, service workflowserviceclient.Interface) worker.Worker {

	workerOptions := worker.Options{
		Logger:       logger,
		MetricsScope: tally.NewTestScope(TaskListName, map[string]string{}),
	}

	worker := worker.New(
		service,
		Domain,
		TaskListName,
		workerOptions)

	return worker
}

// activity
func sayHello(name string) (string, error) {
	return "Hello " + name + "!!!", nil
}

// workflow
func HelloWorldWorkflow(ctx workflow.Context, args interface{}) error {

	ao := workflow.ActivityOptions{
		ScheduleToCloseTimeout: time.Minute,
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       time.Second * 20,
		WaitForCancellation:    false,
	}

	ctx = workflow.WithActivityOptions(ctx, ao)

	var result string
	err := workflow.ExecuteActivity(ctx, sayHello, args).Get(ctx, &result)
	if err != nil {
		return err
	}

	workflow.GetLogger(ctx).Info("Done", zap.String("result", result))
	return nil
}

func startWorkflow(service workflowserviceclient.Interface, demoWorkflow interface{}, argus interface{}) {

	serviceClient := client.NewClient(service, Domain, &client.Options{})

	workflowOption := client.StartWorkflowOptions{
		ID:                              "helloWorld_" + uuid.New(),
		TaskList:                        TaskListName,
		ExecutionStartToCloseTimeout:    time.Minute * 60,
		DecisionTaskStartToCloseTimeout: time.Second * 10,
	}

	we, err := serviceClient.StartWorkflow(context.Background(), workflowOption, demoWorkflow, argus)

	if err != nil {
		fmt.Printf("Failed to start workflow")
		return
	}

	fmt.Printf("workflowID: %v, RunID: %v", we.ID, we.RunID)
}
