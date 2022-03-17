package nomad

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	nomad "github.com/hashicorp/nomad/api"

	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
)

var (
	DefaultSDConfig = SDConfig{
		UpdateInterval:       model.Duration(1 * time.Second),
		EventStreamMaxErrors: 5,
		HTTPClientConfig:     config.DefaultHTTPClientConfig,
	}
)

type SDConfig struct {
	Server               string                  `yaml:"server,omitempty"`
	Region               string                  `yaml:"region,omitempty"`
	Namespace            string                  `yaml:"namespace,omitempty"`
	SecretID             config.Secret           `yaml:"secret_id,omitempty"`
	UpdateInterval       model.Duration          `yaml:"update_interval,omitempty"`
	EventStreamMaxErrors int                     `yaml:"event_stream_max_errors,omitempty"`
	HTTPClientConfig     config.HTTPClientConfig `yaml:",inline"`
}

func (*SDConfig) Name() string { return "nomad" }

func (c *SDConfig) NewDiscoverer(opts discovery.DiscovererOptions) (discovery.Discoverer, error) {
	return NewDiscovery(c, opts.Logger)
}

func (c *SDConfig) SetDirectory(dir string) {
	c.HTTPClientConfig.SetDirectory(dir)
}

func (c *SDConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	*c = DefaultSDConfig
	type plain SDConfig
	err := unmarshal((*plain)(c))
	if err != nil {
		return err
	}
	if strings.TrimSpace(c.Server) == "" {
		return errors.New("nomad SD configuration requires a server address")
	}
	return c.HTTPClientConfig.Validate()
}

type Discovery struct {
	client             *nomad.Client
	logger             log.Logger
	interval           time.Duration
	allocStore         map[string]*nomad.Allocation
	jobStore           map[string]map[string]bool
	eventStreamMaxErrs int
}

func NewDiscovery(conf *SDConfig, logger log.Logger) (*Discovery, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	httpClient, err := config.NewClientFromConfig(conf.HTTPClientConfig, "nomad_sd")
	if err != nil {
		return nil, err
	}

	clientConf := &nomad.Config{
		Address:    conf.Server,
		Region:     conf.Region,
		Namespace:  conf.Namespace,
		SecretID:   string(conf.SecretID),
		HttpClient: httpClient,
	}

	client, err := nomad.NewClient(clientConf)
	if err != nil {
		return nil, err
	}

	nd := &Discovery{
		client:             client,
		logger:             logger,
		interval:           time.Duration(conf.UpdateInterval),
		allocStore:         make(map[string]*nomad.Allocation),
		jobStore:           make(map[string]map[string]bool),
		eventStreamMaxErrs: conf.EventStreamMaxErrors,
	}

	return nd, nil
}

const (
	metaLabelPrefix = model.MetaLabelPrefix + "nomad_"

	regionLabel    = metaLabelPrefix + "region"
	namespaceLabel = metaLabelPrefix + "namespace"
	allocIDLabel   = metaLabelPrefix + "alloc_id"
	jobIDLabel     = metaLabelPrefix + "job_id"
	jobNameLabel   = metaLabelPrefix + "job_name"
	jobTypeLabel   = metaLabelPrefix + "job_type"
	nodeIDLabel    = metaLabelPrefix + "node_id"
	nodeNameLabel  = metaLabelPrefix + "node_name"
	taskGroupLabel = metaLabelPrefix + "task_group"
	taskLabel      = metaLabelPrefix + "task"
	taskStateLabel = metaLabelPrefix + "task_state"
)

func (d *Discovery) groups() []*targetgroup.Group {
	tgs := make([]*targetgroup.Group, 0)

	for allocID, alloc := range d.allocStore {
		tg := targetgroup.Group{
			Source: allocID,
			Labels: model.LabelSet{
				regionLabel:    model.LabelValue(*alloc.Job.Region),
				namespaceLabel: model.LabelValue(alloc.Namespace),
				allocIDLabel:   model.LabelValue(allocID),
				jobIDLabel:     model.LabelValue(alloc.JobID),
				jobNameLabel:   model.LabelValue(*alloc.Job.Name),
				jobTypeLabel:   model.LabelValue(*alloc.Job.Type),
				nodeIDLabel:    model.LabelValue(alloc.NodeID),
				nodeNameLabel:  model.LabelValue(alloc.NodeName),
				taskGroupLabel: model.LabelValue(alloc.TaskGroup),
			},
			Targets: make([]model.LabelSet, 0),
		}

		for tName, tState := range alloc.TaskStates {
			t := model.LabelSet{
				// HACK: until nomad networking is figured out
				model.AddressLabel: model.LabelValue(fmt.Sprintf("%s:%s", allocID, tName)),
				taskLabel:          model.LabelValue(tName),
				taskStateLabel:     model.LabelValue(tState.State),
			}

			tg.Targets = append(tg.Targets, t)
		}

		tgs = append(tgs, &tg)
	}

	return tgs
}

func (d *Discovery) clearAllocs(eval *nomad.Evaluation) {
	if eval.TriggeredBy != "job-deregister" && eval.Status == "complete" {
		return
	}

	allocs, ok := d.jobStore[eval.JobID]
	if !ok {
		return
	}

	for allocId := range allocs {
		delete(d.allocStore, allocId)
	}

	delete(d.jobStore, eval.JobID)
}

func (d *Discovery) updateAllocs(alloc *nomad.Allocation) error {
	jobs := d.client.Jobs()

	if alloc.Job == nil {
		j, _, err := jobs.Info(alloc.JobID, &nomad.QueryOptions{})
		if err != nil {
			return nil
		}
		alloc.Job = j
	}

	d.allocStore[alloc.ID] = alloc

	if _, ok := d.jobStore[alloc.JobID]; !ok {
		d.jobStore[alloc.JobID] = make(map[string]bool)
	}
	d.jobStore[alloc.JobID][alloc.ID] = true

	return nil
}

func (d *Discovery) initAllocs() (uint64, error) {
	var idx uint64

	allocations := d.client.Allocations()

	allocs, meta, err := allocations.List(
		&nomad.QueryOptions{
			Params: map[string]string{
				"task_states": "false",
			},
		},
	)
	if err != nil {
		return idx, err
	}
	idx = meta.LastIndex

	for _, a := range allocs {
		alloc, meta, err := allocations.Info(a.ID, &nomad.QueryOptions{})
		if err != nil {
			return idx, err
		}
		idx = meta.LastIndex
		err = d.updateAllocs(alloc)
		if err != nil {
			return idx, err
		}
	}

	return idx, nil
}

func (d *Discovery) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	idx, err := d.initAllocs()
	if err != nil {
		level.Error(d.logger).Log("msg", "Error initializing allocations", "err", err)
		return
	}
	ch <- d.groups()

	eventStream := d.client.EventStream()

	topics := map[nomad.Topic][]string{
		nomad.TopicAllocation: {"*"},
		nomad.TopicEvaluation: {"*"},
	}

	eventChan := make(<-chan *nomad.Events, 10)
	subscribe := make(chan bool, 1)
	cancel := func() {}

	// rate limit updates since events can stream very quickly
	ticker := time.NewTicker(d.interval)

	// initial subscription
	subscribe <- true

	streamErrs := 0
	for {
		if streamErrs > d.eventStreamMaxErrs {
			level.Error(d.logger).Log("msg", "Errors in event stream passed max errors limit", "limit", d.eventStreamMaxErrs)
			return
		}

		select {
		case <-subscribe:
			level.Info(d.logger).Log("msg", "Subscribing to event stream")
			streamCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			eventChan, err = eventStream.Stream(streamCtx, topics, idx, &nomad.QueryOptions{})
			if err != nil {
				level.Error(d.logger).Log("msg", "Error subscribing to event stream", "err", err)
				streamErrs++
				continue
			}
		case event := <-eventChan:
			if event.Err != nil && strings.Contains(event.Err.Error(), "invalid character 's' looking for beginning of value") {
				level.Warn(d.logger).Log("msg", "Server disconnected, resubscribing")
				cancel()
				clearEventChan(eventChan)
				subscribe <- true
				continue
			}
			if event.Err != nil {
				level.Error(d.logger).Log("msg", "Error in event stream", "err", event.Err)
				streamErrs++
				continue
			}

			for _, e := range event.Events {
				idx = e.Index

				switch fmt.Sprintf("%s.%s", e.Topic, e.Type) {
				case "Allocation.AllocationUpdated":
					alloc, err := e.Allocation()
					if err != nil {
						level.Error(d.logger).Log("msg", "Error getting allocation from event stream", "err", err)
						streamErrs++
						continue
					}
					if alloc == nil {
						level.Error(d.logger).Log("msg", "Allocation in event stream shouldn't be nil")
						streamErrs++
						continue
					}

					err = d.updateAllocs(alloc)
					if err != nil {
						level.Error(d.logger).Log("msg", "Error updating allocation store", "err", err)
						return
					}
				case "Evaluation.EvaluationUpdated":
					eval, err := e.Evaluation()
					if err != nil {
						level.Error(d.logger).Log("msg", "Error getting evaluation from event stream", "err", err)
						streamErrs++
						continue
					}
					if eval == nil {
						level.Error(d.logger).Log("msg", "Evaluation in event stream shouldn't be nil")
						streamErrs++
						continue
					}

					d.clearAllocs(eval)
				default:
					continue
				}
			}
		case <-ticker.C:
			ch <- d.groups()
		}
	}
}

func clearEventChan(ch <-chan *nomad.Events) {
	for len(ch) > 0 {
		<-ch
	}
}
