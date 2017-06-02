package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	"github.com/fsouza/go-dockerclient"
)

const (
	zone           = "rdss-archivematica.test"
	dockerEndpoint = "unix:///var/run/docker.sock"
	workerTimeout  = 180 * time.Second
	defaultTTL     = 0
	defaultWeight  = 1
)

type handler interface {
	Handle(*docker.APIEvents, *dockerRouter) error
}

type dockerRouter struct {
	handlers      map[string][]handler
	dockerClient  *docker.Client
	listener      chan *docker.APIEvents
	workers       chan *worker
	workerTimeout time.Duration
	config        *dockerRouterConfiguration
}

type dockerRouterConfiguration struct {
	sess        *session.Session
	ec2Metadata *ec2metadata.EC2InstanceIdentityDocument
	zoneName    string
	zoneID      string
}

func dockerEventsRouter(bufferSize int, workerPoolSize int, dockerClient *docker.Client, config *dockerRouterConfiguration, handlers map[string][]handler) (*dockerRouter, error) {
	workers := make(chan *worker, workerPoolSize)
	for i := 0; i < workerPoolSize; i++ {
		workers <- &worker{}
	}

	dockerRouter := &dockerRouter{
		handlers:      handlers,
		dockerClient:  dockerClient,
		listener:      make(chan *docker.APIEvents, bufferSize),
		workers:       workers,
		workerTimeout: workerTimeout,
		config:        config,
	}

	return dockerRouter, nil
}

func (e *dockerRouter) start() error {
	go e.manageEvents()
	return e.dockerClient.AddEventListener(e.listener)
}

func (e *dockerRouter) stop() error {
	if e.listener == nil {
		return nil
	}
	return e.dockerClient.RemoveEventListener(e.listener)
}

func (e *dockerRouter) manageEvents() {
	for {
		event := <-e.listener
		timer := time.NewTimer(e.workerTimeout)
		gotWorker := false
		// Wait until we get a free worker or a timeout
		// there is a limit in the number of concurrent events managed by workers to avoid resource exhaustion
		// so we wait until we have a free worker or a timeout occurs
		for !gotWorker {
			select {
			case w := <-e.workers:
				if !timer.Stop() {
					<-timer.C
				}
				go w.doWork(event, e)
				gotWorker = true
			case <-timer.C:
				log.Println("Timed out waiting.")
			}
		}
	}
}

type worker struct{}

func (w *worker) doWork(event *docker.APIEvents, router *dockerRouter) {
	defer func() { router.workers <- w }()
	if handlers, ok := router.handlers[event.Status]; ok {
		log.Printf("Processing event: %#v", event)
		for _, handler := range handlers {
			if err := handler.Handle(event, router); err != nil {
				log.Printf("Error processing event %#v. Error: %v", event, err)
			}
		}
	}
}

func main() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create AWS session
	sess := session.Must(session.NewSession())

	// Retrieve EC2 instance metadata
	ec2Metadata, err := getEC2Metadata(sess)
	if err != nil {
		log.Fatalf("EC2 metadata could not be retrieved: %v", err)
	}
	log.Printf("EC2 metadata - Region: %s", ec2Metadata.Region)
	log.Printf("EC2 metadata - Private IP: %s", ec2Metadata.PrivateIP)

	// Retreive DNS zone ID
	zoneID, err := getDNSHostedZoneID(sess, zone)
	if err != nil {
		log.Fatalf("DNS zone ID could not be retrieved: %v", err)
	}
	log.Printf("DNS zone %s has ID %s", zone, zoneID)

	// Start Docker client
	client, err := docker.NewClient(dockerEndpoint)
	if err != nil {
		log.Fatal(err)
	}

	// Start our internal Docker events router
	rcfg := &dockerRouterConfiguration{
		sess:        sess,
		ec2Metadata: ec2Metadata,
		zoneName:    zone,
		zoneID:      zoneID,
	}
	router, err := dockerEventsRouter(5, 5, client, rcfg, map[string][]handler{
		"start": []handler{
			&dockerHandler{handlerFunc: containterStarted},
		},
		"die": []handler{
			&dockerHandler{handlerFunc: containerDied},
		},
	})
	router.start()
	defer router.stop()

	log.Println("Waiting for events...")

	// Subscribe to SIGINT signals and wait
	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, os.Interrupt)
	<-stopChan // Wait for SIGINT
}

func getEC2Metadata(sess *session.Session) (*ec2metadata.EC2InstanceIdentityDocument, error) {
	c := ec2metadata.New(sess)
	if !c.Available() {
		return nil, errors.New("EC2 Metadata service not available")
	}
	doc, err := c.GetInstanceIdentityDocument()
	if err != nil {
		return nil, err
	}
	return &doc, nil
}

func getDNSHostedZoneID(sess *session.Session, name string) (string, error) {
	r53 := route53.New(sess)
	params := &route53.ListHostedZonesByNameInput{
		DNSName: aws.String(name),
	}
	zones, err := r53.ListHostedZonesByName(params)
	if err != nil {
		return "", err
	}

	if len(zones.HostedZones) == 0 {
		return "", fmt.Errorf("DNS zone %s not found", name)
	}

	return aws.StringValue(zones.HostedZones[0].Id), nil
}

func getServiceName(container *docker.Container) string {
	for _, env := range container.Config.Env {
		envParts := strings.Split(env, "=")
		if len(envParts) != 2 {
			continue
		}
		en, sn := envParts[0], envParts[1]
		if en != "SERVICE_NAME" {
			continue
		}
		return sn
	}
	return ""
}

type dockerHandler struct {
	dockerClient *docker.Client
	handlerFunc  func(event *docker.APIEvents, router *dockerRouter) error
}

func (th *dockerHandler) Handle(event *docker.APIEvents, router *dockerRouter) error {
	return th.handlerFunc(event, router)
}

func containterStarted(event *docker.APIEvents, router *dockerRouter) error {
	container, err := router.dockerClient.InspectContainer(event.ID)
	if err != nil {
		return err
	}
	log.Printf("Docker container %s started", event.ID)

	serviceName := getServiceName(container)
	if serviceName == "" {
		return errors.New("Service name could not be found")
	}
	err = createDNSRecord(router.config, serviceName, event.ID)
	if err != nil {
		return err
	}
	return nil
}

func containerDied(event *docker.APIEvents, router *dockerRouter) error {
	container, err := router.dockerClient.InspectContainer(event.ID)
	if err != nil {
		return err
	}
	log.Printf("Docker container %s stopped", event.ID)

	serviceName := getServiceName(container)
	if serviceName == "" {
		return errors.New("Service name could not be found")
	}
	err = deleteDNSRecord(router.config, serviceName, event.ID)
	if err != nil {
		return err
	}
	return nil
}

func createDNSRecord(config *dockerRouterConfiguration, serviceName string, dockerID string) error {
	r53 := route53.New(config.sess)
	recordName := serviceName + "." + config.zoneName
	params := &route53.ChangeResourceRecordSetsInput{
		ChangeBatch: &route53.ChangeBatch{
			Changes: []*route53.Change{
				{
					Action: aws.String(route53.ChangeActionCreate),
					ResourceRecordSet: &route53.ResourceRecordSet{
						Name: aws.String(recordName),
						Type: aws.String(route53.RRTypeA),
						ResourceRecords: []*route53.ResourceRecord{
							{
								Value: aws.String(config.ec2Metadata.PrivateIP),
							},
						},
						SetIdentifier: aws.String(dockerID),
						TTL:           aws.Int64(defaultTTL), // TTL=0 to avoid DNS caches
						Weight:        aws.Int64(defaultWeight),
					},
				},
			},
			Comment: aws.String("Service Discovery Created Record"),
		},
		HostedZoneId: aws.String(config.zoneID),
	}
	_, err := r53.ChangeResourceRecordSets(params)
	if err != nil {
		return err
	}
	log.Printf("Record %s created", recordName)
	return nil
}

func deleteDNSRecord(config *dockerRouterConfiguration, serviceName string, dockerID string) error {
	r53 := route53.New(config.sess)
	recordName := serviceName + "." + config.zoneName

	// This API Call looks for the Route53 DNS record for this service and
	// Docker ID to get the values to delete.
	paramsList := &route53.ListResourceRecordSetsInput{
		HostedZoneId:          aws.String(config.zoneID), // Required
		MaxItems:              aws.String("10"),
		StartRecordIdentifier: aws.String(dockerID),
		StartRecordName:       aws.String(recordName),
		StartRecordType:       aws.String(route53.RRTypeA),
	}
	resp, err := r53.ListResourceRecordSets(paramsList)
	if err != nil {
		return err
	}

	var aValue string
	for _, rrset := range resp.ResourceRecordSets {
		if nil == rrset.SetIdentifier {
			continue
		}
		if *rrset.SetIdentifier == dockerID && (*rrset.Name == recordName || *rrset.Name == recordName+".") {
			for _, rrecords := range rrset.ResourceRecords {
				aValue = *rrecords.Value
				break
			}
		}
	}
	if aValue == "" {
		log.Println("Route53 record doesn't exist")
		return nil
	}

	// This API call deletes the DNS record for the service for this docker ID.
	params := &route53.ChangeResourceRecordSetsInput{
		ChangeBatch: &route53.ChangeBatch{
			Changes: []*route53.Change{
				{
					Action: aws.String(route53.ChangeActionDelete),
					ResourceRecordSet: &route53.ResourceRecordSet{
						Name: aws.String(recordName),
						Type: aws.String(route53.RRTypeA),
						ResourceRecords: []*route53.ResourceRecord{
							{
								Value: aws.String(aValue),
							},
						},
						SetIdentifier: aws.String(dockerID),
						TTL:           aws.Int64(defaultTTL),
						Weight:        aws.Int64(defaultWeight),
					},
				},
			},
		},
		HostedZoneId: aws.String(config.zoneID),
	}
	_, err = r53.ChangeResourceRecordSets(params)
	if err != nil {
		return err
	}
	log.Printf("Record %s deleted", recordName)
	return nil
}
