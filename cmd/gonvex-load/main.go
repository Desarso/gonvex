package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type variableFlag map[string]string

func (v variableFlag) String() string {
	encoded, _ := json.Marshal(map[string]string(v))
	return string(encoded)
}

func (v variableFlag) Set(raw string) error {
	key, value, ok := strings.Cut(raw, "=")
	key = strings.TrimSpace(key)
	if !ok || key == "" {
		return fmt.Errorf("variable must use key=value")
	}
	v[key] = value
	return nil
}

type cliOptions struct {
	profilePath             string
	runtimeURL              string
	project                 string
	tenant                  string
	connections             int
	subscriptions           int
	ramp                    time.Duration
	hold                    time.Duration
	connectTimeout          time.Duration
	initialTimeout          time.Duration
	authMode                string
	tokenEnvironment        string
	compression             bool
	maximumDialConcurrency  int
	sampleInterval          time.Duration
	targetPID               int
	minimumHostAvailableMiB uint64
	maximumTargetRSSMiB     uint64
	maximumErrorRate        float64
	reportPath              string
	allowNonLoopback        bool
	dryRun                  bool
	variables               variableFlag
}

func main() {
	if err := runMain(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runMain(args []string, stdout, stderr io.Writer) error {
	options, err := parseCLI(args, stderr)
	if err != nil {
		return err
	}
	if !options.allowNonLoopback {
		if err := assertLoopbackTarget(options.runtimeURL); err != nil {
			return err
		}
	}
	profileFile, err := os.Open(options.profilePath)
	if err != nil {
		return fmt.Errorf("open profile: %w", err)
	}
	profile, err := loadProfileReader(profileFile)
	profileFile.Close()
	if err != nil {
		return err
	}
	if options.subscriptions < 0 {
		options.subscriptions = len(profile.Subscriptions)
	}
	mode := authMode(options.authMode)
	sharedToken := ""
	if mode == authModeShared {
		sharedToken = strings.TrimSpace(os.Getenv(options.tokenEnvironment))
		if sharedToken == "" {
			return fmt.Errorf("shared auth token environment variable %s is empty", options.tokenEnvironment)
		}
	}
	config := runConfig{
		URL:                        options.runtimeURL,
		Project:                    options.project,
		Tenant:                     options.tenant,
		Connections:                options.connections,
		SubscriptionsPerConnection: options.subscriptions,
		RampDuration:               options.ramp,
		HoldDuration:               options.hold,
		ConnectTimeout:             options.connectTimeout,
		InitialTimeout:             options.initialTimeout,
		AuthMode:                   mode,
		SharedToken:                sharedToken,
		Compression:                options.compression,
		MaximumDialConcurrency:     options.maximumDialConcurrency,
		Variables:                  map[string]string(options.variables),
		SampleInterval:             options.sampleInterval,
		TargetPID:                  options.targetPID,
		Safety: safetyLimits{
			MinimumHostAvailableBytes: options.minimumHostAvailableMiB << 20,
			MaximumTargetRSSBytes:     options.maximumTargetRSSMiB << 20,
			MaximumErrorRate:          options.maximumErrorRate,
			MinimumOperations:         100,
		},
	}
	if err := validateRunConfig(config, profile); err != nil {
		return err
	}
	plan := map[string]any{
		"profile":                    profile.Name,
		"target":                     options.runtimeURL,
		"connections":                config.Connections,
		"subscriptionsPerConnection": config.SubscriptionsPerConnection,
		"totalSubscriptions":         config.Connections * config.SubscriptionsPerConnection,
		"ramp":                       config.RampDuration.String(),
		"hold":                       config.HoldDuration.String(),
		"authMode":                   config.AuthMode,
		"compression":                config.Compression,
		"report":                     options.reportPath,
	}
	if options.dryRun {
		return writeJSON(stdout, plan)
	}
	if err := writeJSON(stderr, map[string]any{"event": "gonvex-load-start", "plan": plan}); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	report, err := runLoad(ctx, config, profile)
	if err != nil {
		return err
	}
	if err := writeReport(options.reportPath, report); err != nil {
		return err
	}
	summary := map[string]any{
		"report":        options.reportPath,
		"abortReason":   report.AbortReason,
		"connections":   report.Connections,
		"subscriptions": report.Subscriptions,
		"wire":          report.Wire,
		"latency":       report.Latency,
		"samples":       len(report.Samples),
		"errorSamples":  report.ErrorSamples,
	}
	if err := writeJSON(stdout, summary); err != nil {
		return err
	}
	if report.AbortReason != "" {
		return fmt.Errorf("load run aborted: %s", report.AbortReason)
	}
	if report.Subscriptions.ErrorRate > options.maximumErrorRate {
		return fmt.Errorf("subscription error rate %.4f exceeded %.4f", report.Subscriptions.ErrorRate, options.maximumErrorRate)
	}
	return nil
}

func parseCLI(args []string, stderr io.Writer) (cliOptions, error) {
	timestamp := time.Now().UTC().Format("20060102T150405Z")
	options := cliOptions{
		runtimeURL:              "http://127.0.0.1:18080",
		project:                 "whagons-5",
		tenant:                  "loadtest",
		connections:             1,
		subscriptions:           -1,
		ramp:                    10 * time.Second,
		hold:                    time.Minute,
		connectTimeout:          10 * time.Second,
		initialTimeout:          2 * time.Minute,
		authMode:                string(authModeSynthetic),
		tokenEnvironment:        "GONVEX_LOAD_TOKEN",
		compression:             true,
		maximumDialConcurrency:  64,
		sampleInterval:          time.Second,
		minimumHostAvailableMiB: 4096,
		maximumTargetRSSMiB:     20480,
		maximumErrorRate:        0.05,
		reportPath:              filepath.Join("tmp", "gonvex-load", "report-"+timestamp+".json"),
		variables:               variableFlag{},
	}
	flags := flag.NewFlagSet("gonvex-load", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.profilePath, "profile", "", "Whagons/Gonvex subscription profile JSON (required)")
	flags.StringVar(&options.runtimeURL, "url", options.runtimeURL, "Gonvex runtime URL")
	flags.StringVar(&options.project, "project", options.project, "Gonvex project id")
	flags.StringVar(&options.tenant, "tenant", options.tenant, "active tenant")
	flags.IntVar(&options.connections, "connections", options.connections, "persistent WebSocket connections")
	flags.IntVar(&options.subscriptions, "subscriptions-per-connection", options.subscriptions, "profile subscriptions per connection; -1 uses all")
	flags.DurationVar(&options.ramp, "ramp", options.ramp, "connection ramp duration")
	flags.DurationVar(&options.hold, "hold", options.hold, "steady-state hold after initial results")
	flags.DurationVar(&options.connectTimeout, "connect-timeout", options.connectTimeout, "WebSocket/session timeout")
	flags.DurationVar(&options.initialTimeout, "initial-timeout", options.initialTimeout, "maximum time for initial subscriptions")
	flags.StringVar(&options.authMode, "auth-mode", options.authMode, "none, shared, or synthetic")
	flags.StringVar(&options.tokenEnvironment, "token-env", options.tokenEnvironment, "environment variable containing shared token")
	flags.BoolVar(&options.compression, "compression", options.compression, "negotiate WebSocket compression")
	flags.IntVar(&options.maximumDialConcurrency, "max-dial-concurrency", options.maximumDialConcurrency, "maximum concurrent WebSocket handshakes")
	flags.DurationVar(&options.sampleInterval, "sample-interval", options.sampleInterval, "resource and throughput sample interval; 0 disables")
	flags.IntVar(&options.targetPID, "target-pid", 0, "runtime process id to sample")
	flags.Uint64Var(&options.minimumHostAvailableMiB, "min-host-available-mib", options.minimumHostAvailableMiB, "abort below this host memory reserve")
	flags.Uint64Var(&options.maximumTargetRSSMiB, "max-target-rss-mib", options.maximumTargetRSSMiB, "abort above this runtime RSS; requires target-pid")
	flags.Float64Var(&options.maximumErrorRate, "max-error-rate", options.maximumErrorRate, "abort after 100 operations above this error ratio")
	flags.StringVar(&options.reportPath, "report", options.reportPath, "JSON report path")
	flags.BoolVar(&options.allowNonLoopback, "allow-non-loopback", false, "explicitly allow a remote target")
	flags.BoolVar(&options.dryRun, "dry-run", false, "validate and print the plan without connecting")
	flags.Var(options.variables, "var", "profile variable override in key=value form; repeatable")
	if err := flags.Parse(args); err != nil {
		return cliOptions{}, err
	}
	if options.profilePath == "" {
		return cliOptions{}, errors.New("--profile is required")
	}
	if flags.NArg() != 0 {
		return cliOptions{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	if options.connections < 1 || options.maximumDialConcurrency < 1 {
		return cliOptions{}, fmt.Errorf("connections and max-dial-concurrency must be positive")
	}
	if options.subscriptions < -1 {
		return cliOptions{}, fmt.Errorf("subscriptions-per-connection must be -1 or greater")
	}
	if options.maximumErrorRate < 0 || options.maximumErrorRate > 1 {
		return cliOptions{}, fmt.Errorf("max-error-rate must be between 0 and 1")
	}
	if options.targetPID == 0 {
		options.maximumTargetRSSMiB = 0
	}
	return options, nil
}

func assertLoopbackTarget(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}
	host := strings.TrimSpace(parsed.Hostname())
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("refusing non-loopback load target %q; pass --allow-non-loopback explicitly", host)
}

func writeReport(path string, report RunReport) error {
	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(path, append(payload, '\n'), 0o644); err != nil {
		return err
	}
	return nil
}

func writeJSON(writer io.Writer, value any) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func parsePositiveInt(raw string) (int, error) {
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 {
		return 0, fmt.Errorf("value must be a positive integer")
	}
	return value, nil
}
