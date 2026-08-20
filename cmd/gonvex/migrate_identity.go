package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	identity "github.com/gonvex/gonvex/server/pkg/controlplane/identity"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const identityMigrationScope = "account resolution and Control Plane migration ledger only; tenant schemas and foreign keys are not rewritten"

type legacyIdentityInventory struct {
	Records []identity.LegacyIdentity `json:"records"`
}

type identityMigrationSummary struct {
	LegacyRows          int    `json:"legacyRows"`
	UniqueAccounts      int    `json:"uniqueAccounts"`
	ProviderMatches     int    `json:"providerMatches"`
	VerifiedEmailMatches int   `json:"verifiedEmailMatches"`
	NewAccounts         int    `json:"newAccounts"`
	AmbiguousCollisions int    `json:"ambiguousCollisions"`
	PlanChecksum        string `json:"planChecksum"`
}

type identityMigrationOutput struct {
	Operation    string                       `json:"operation"`
	RunID        string                       `json:"runId"`
	Source       string                       `json:"source"`
	PlanFile     string                       `json:"planFile"`
	Scope        string                       `json:"scope"`
	Summary      identityMigrationSummary     `json:"summary"`
	Verification *identity.VerificationResult `json:"verification,omitempty"`
}

func runMigrate(args []string) error {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" {
		return fmt.Errorf("usage: gonvex migrate identity-v2 (--plan | --apply | --verify) [options]")
	}
	if args[0] != "identity-v2" {
		return fmt.Errorf("unknown migration %q; expected identity-v2", args[0])
	}
	return runIdentityV2Migration(args[1:])
}

func runIdentityV2Migration(args []string) error {
	flags := flag.NewFlagSet("migrate identity-v2", flag.ContinueOnError)
	planMode := flags.Bool("plan", false, "create a read-only, checksummed identity-v2 migration plan")
	applyMode := flags.Bool("apply", false, "install identity-v2 schema and resumably apply a reviewed plan")
	verifyMode := flags.Bool("verify", false, "verify the Control Plane ledger against a checksummed plan")
	controlPlaneURL := flags.String("control-plane-url", controlPlaneDatabaseURL(), "Control Plane PostgreSQL URL (env: GONVEX_CONTROL_PLANE_DATABASE_URL)")
	source := flags.String("source", "", "stable legacy identity source name")
	runID := flags.String("run-id", "", "stable, resumable migration run ID")
	input := flags.String("input", "", "legacy identity inventory JSON file (plan only; use - for stdin)")
	planFile := flags.String("plan-file", "identity-v2-plan.json", "checksummed JSON plan path")
	allowUnresolved := flags.Bool("allow-unresolved-collisions", false, "apply all unambiguous rows while recording unresolved collisions for explicit review")
	jsonOutput := flags.Bool("json", false, "print a machine-readable result")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if len(flags.Args()) != 0 {
		return fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}
	*controlPlaneURL = strings.TrimSpace(*controlPlaneURL)
	*source = strings.TrimSpace(*source)
	*runID = strings.TrimSpace(*runID)
	*input = strings.TrimSpace(*input)
	modeCount := boolCount(*planMode, *applyMode, *verifyMode)
	if modeCount != 1 {
		return fmt.Errorf("exactly one of --plan, --apply, or --verify is required")
	}
	if strings.TrimSpace(*controlPlaneURL) == "" {
		return fmt.Errorf("--control-plane-url is required (or set GONVEX_CONTROL_PLANE_DATABASE_URL)")
	}
	if strings.TrimSpace(*planFile) == "" {
		return fmt.Errorf("--plan-file is required")
	}
	if *allowUnresolved && !*applyMode {
		return fmt.Errorf("--allow-unresolved-collisions is valid only with --apply")
	}

	ctx := context.Background()
	db, err := sql.Open("pgx", *controlPlaneURL)
	if err != nil {
		return fmt.Errorf("open Control Plane database: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("connect to Control Plane database: %w", err)
	}

	if *planMode {
		if strings.TrimSpace(*source) == "" || strings.TrimSpace(*runID) == "" || strings.TrimSpace(*input) == "" {
			return fmt.Errorf("--plan requires --source, --run-id, and --input")
		}
		records, err := loadLegacyIdentityInventory(*input, *source)
		if err != nil {
			return err
		}
		existing, err := identity.LoadExistingAccounts(ctx, db)
		if err != nil {
			return err
		}
		plan, err := identity.PlanIdentityMigration(*runID, *source, records, existing)
		if err != nil {
			return err
		}
		if err := writeIdentityMigrationPlan(*planFile, plan); err != nil {
			return err
		}
		return printIdentityMigrationResult(*jsonOutput, "plan", *planFile, plan, nil)
	}

	plan, err := readIdentityMigrationPlan(*planFile)
	if err != nil {
		return err
	}
	if err := matchIdentityPlanFlags(plan, *runID, *source); err != nil {
		return err
	}
	store := identity.PostgresMigrationStore{DB: db}
	if *applyMode {
		if err := identity.InstallSchema(ctx, db); err != nil {
			return fmt.Errorf("install identity-v2 Control Plane schema: %w", err)
		}
		if err := identity.ApplyIdentityMigration(ctx, store, plan, *allowUnresolved); err != nil {
			return err
		}
		return printIdentityMigrationResult(*jsonOutput, "apply", *planFile, plan, nil)
	}

	result, err := identity.VerifyIdentityMigration(ctx, store, plan)
	if err != nil {
		return err
	}
	if err := printIdentityMigrationResult(*jsonOutput, "verify", *planFile, plan, &result); err != nil {
		return err
	}
	if len(result.Findings) > 0 {
		return fmt.Errorf("identity-v2 verification found %d issue(s)", len(result.Findings))
	}
	return nil
}

func controlPlaneDatabaseURL() string {
	for _, name := range []string{
		"GONVEX_CONTROL_PLANE_DATABASE_URL",
		"GONVEX_CONTROL_PLANE_URL",
		"CONTROL_PLANE_DATABASE_URL",
		"CONTROL_PLANE_URL",
		"GONVEX_LANDLORD_DATABASE_URL",
		"LANDLORD_DATABASE_URL",
	} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

func loadLegacyIdentityInventory(path, source string) ([]identity.LegacyIdentity, error) {
	var raw []byte
	var err error
	if path == "-" {
		raw, err = io.ReadAll(os.Stdin)
	} else {
		raw, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, fmt.Errorf("read legacy identity inventory: %w", err)
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return nil, fmt.Errorf("legacy identity inventory is empty")
	}

	var records []identity.LegacyIdentity
	if raw[0] == '[' {
		if err := decodeStrictJSON(raw, &records); err != nil {
			return nil, fmt.Errorf("decode legacy identity inventory: %w", err)
		}
	} else {
		var inventory legacyIdentityInventory
		if err := decodeStrictJSON(raw, &inventory); err != nil {
			return nil, fmt.Errorf("decode legacy identity inventory: %w", err)
		}
		records = inventory.Records
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("legacy identity inventory contains no records")
	}

	seen := make(map[string]bool, len(records))
	for index := range records {
		record := &records[index]
		if record.Source != strings.TrimSpace(record.Source) {
			return nil, fmt.Errorf("legacy identity %q has whitespace around source", record.LegacyUserID)
		}
		if record.LegacyUserID != strings.TrimSpace(record.LegacyUserID) {
			return nil, fmt.Errorf("legacy identity at index %d has whitespace around legacyUserId", index)
		}
		if strings.TrimSpace(record.Source) == "" {
			record.Source = source
		}
		if record.Source != source {
			return nil, fmt.Errorf("legacy identity %q has source %q; expected %q", record.LegacyUserID, record.Source, source)
		}
		if strings.TrimSpace(record.LegacyUserID) == "" {
			return nil, fmt.Errorf("legacy identity at index %d has no legacyUserId", index)
		}
		key := record.Source + "\x00" + record.LegacyUserID
		if seen[key] {
			return nil, fmt.Errorf("duplicate legacy identity %q/%q", record.Source, record.LegacyUserID)
		}
		seen[key] = true
	}
	return records, nil
}

func writeIdentityMigrationPlan(path string, plan identity.MigrationPlan) error {
	if err := identity.ValidateIdentityMigrationPlan(plan); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')

	if existingRaw, err := os.ReadFile(path); err == nil {
		var existing identity.MigrationPlan
		if decodeErr := decodeStrictJSON(existingRaw, &existing); decodeErr != nil {
			return fmt.Errorf("plan file %s already exists but is invalid: %w", path, decodeErr)
		}
		if validateErr := identity.ValidateIdentityMigrationPlan(existing); validateErr != nil {
			return fmt.Errorf("plan file %s already exists but failed checksum validation: %w", path, validateErr)
		}
		if existing.Checksum != plan.Checksum {
			return fmt.Errorf("plan file %s already exists with checksum %s; refusing to overwrite it with %s", path, existing.Checksum, plan.Checksum)
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect plan file: %w", err)
	}

	directory := filepath.Dir(path)
	if directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return fmt.Errorf("create plan directory: %w", err)
		}
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create plan file: %w", err)
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return fmt.Errorf("write plan file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close plan file: %w", err)
	}
	return nil
}

func readIdentityMigrationPlan(path string) (identity.MigrationPlan, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return identity.MigrationPlan{}, fmt.Errorf("read identity migration plan: %w", err)
	}
	var plan identity.MigrationPlan
	if err := decodeStrictJSON(raw, &plan); err != nil {
		return identity.MigrationPlan{}, fmt.Errorf("decode identity migration plan: %w", err)
	}
	if err := identity.ValidateIdentityMigrationPlan(plan); err != nil {
		return identity.MigrationPlan{}, err
	}
	return plan, nil
}

func decodeStrictJSON(raw []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func matchIdentityPlanFlags(plan identity.MigrationPlan, runID, source string) error {
	if runID != "" && runID != plan.RunID {
		return fmt.Errorf("--run-id %q does not match plan runId %q", runID, plan.RunID)
	}
	if source != "" && source != plan.Source {
		return fmt.Errorf("--source %q does not match plan source %q", source, plan.Source)
	}
	return nil
}

func printIdentityMigrationResult(jsonOutput bool, operation, planFile string, plan identity.MigrationPlan, verification *identity.VerificationResult) error {
	output := identityMigrationOutput{
		Operation: operation,
		RunID: plan.RunID,
		Source: plan.Source,
		PlanFile: planFile,
		Scope: identityMigrationScope,
		Summary: identityMigrationSummary{
			LegacyRows: plan.LegacyRows,
			UniqueAccounts: plan.UniqueAccounts,
			ProviderMatches: plan.ProviderMatches,
			VerifiedEmailMatches: plan.EmailMatches,
			NewAccounts: plan.NewAccounts,
			AmbiguousCollisions: plan.AmbiguousCollisions,
			PlanChecksum: plan.Checksum,
		},
		Verification: verification,
	}
	if jsonOutput {
		encoder := json.NewEncoder(os.Stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(output)
	}
	fmt.Printf("identity-v2 %s: run=%s source=%s\n", operation, plan.RunID, plan.Source)
	fmt.Printf("legacy rows=%d unique accounts=%d provider matches=%d verified-email matches=%d new accounts=%d collisions=%d\n",
		plan.LegacyRows, plan.UniqueAccounts, plan.ProviderMatches, plan.EmailMatches, plan.NewAccounts, plan.AmbiguousCollisions)
	fmt.Printf("plan=%s checksum=%s\n", planFile, plan.Checksum)
	fmt.Printf("scope: %s\n", identityMigrationScope)
	if verification != nil {
		fmt.Printf("verification findings=%d\n", len(verification.Findings))
		for _, finding := range verification.Findings {
			fmt.Printf("- %s [%s]: %s\n", finding.Code, finding.Scope, finding.Detail)
		}
	}
	return nil
}

func boolCount(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}
