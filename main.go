package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"gopkg.in/yaml.v3"

	sd "github.com/hyperbadger/nomad-sd/discovery/nomad"
)

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i, v := range a {
		if v != b[i] {
			return false
		}
	}

	return true
}

func flattenTgs(tgs []*targetgroup.Group) (fTgs []*targetgroup.Group) {
	fTgs = make([]*targetgroup.Group, 0)

	for _, tg := range tgs {
		for _, oT := range tg.Targets {
			ts := make([]model.LabelSet, 0, 1)
			ts = append(ts, model.LabelSet{
				model.AddressLabel: oT[model.AddressLabel],
			})

			fTgs = append(fTgs, &targetgroup.Group{
				Targets: ts,
				Labels:  tg.Labels.Merge(oT),
			})
		}
	}

	return
}

func main() {
	w := log.NewSyncWriter(os.Stderr)

	logger := log.NewLogfmtLogger(w)
	if len(os.Getenv("NOMAD_SD_LOG_JSON")) > 0 {
		logger = log.NewJSONLogger(w)
	}

	logger = level.NewFilter(logger, level.AllowInfo())
	if len(os.Getenv("NOMAD_SD_DEBUG")) > 0 {
		logger = level.NewFilter(logger, level.AllowAll())
	}

	cPath := flag.String("config", "config.yaml", "path to sd config")
	tPath := flag.String("targets", "targets.yaml", "path to targets file")
	flag.Parse()

	cBytes, err := os.ReadFile(*cPath)
	if err != nil {
		level.Error(logger).Log("msg", "Error reading sd config", "path", cPath, "err", err)
		os.Exit(1)
	}

	sdCfg := sd.SDConfig{}
	err = yaml.Unmarshal(cBytes, &sdCfg)
	if err != nil {
		level.Error(logger).Log("msg", "Error unmarshaling sd config", "err", err)
		os.Exit(1)
	}

	dOpts := discovery.DiscovererOptions{
		Logger: logger,
	}

	d, err := sdCfg.NewDiscoverer(dOpts)
	if err != nil {
		level.Error(logger).Log("msg", "Error initializing discoverer", "err", err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	tgsChan := make(chan []*targetgroup.Group, 1)
	// initalize channel with empty tgs so that an empty targets file is created
	emptyTgs := make([]*targetgroup.Group, 0)
	tgsChan <- emptyTgs

	go func() {
		d.Run(ctx, tgsChan)
		cancel()
	}()

	level.Info(logger).Log("msg", "Service discovery started")

	oldTBytes := make([]byte, 0)
	for {
		select {
		case tgs := <-tgsChan:
			fTgs := flattenTgs(tgs)
			sort.Slice(fTgs, func(i, j int) bool {
				return fTgs[i].Targets[0][model.AddressLabel] < fTgs[j].Targets[0][model.AddressLabel]
			})

			tBytes, err := yaml.Marshal(fTgs)
			if err != nil {
				level.Error(logger).Log("msg", "Error marshalling target group", "err", err)
				os.Exit(1)
			}

			if equalBytes(oldTBytes, tBytes) {
				continue
			}

			oldTBytes = tBytes

			err = os.WriteFile(*tPath, tBytes, 0644)
			if err != nil {
				level.Error(logger).Log("msg", "Error writing to targets file", "err", err)
				os.Exit(1)
			}
		case sig := <-sigs:
			level.Info(logger).Log("msg", "Recieved signal, stopping", "sig", sig)
			os.Exit(0)
		case <-ctx.Done():
			os.Exit(0)
		}
	}
}
