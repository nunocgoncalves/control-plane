package work

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type ValueModelInput struct {
	Ref              string          `json:"ref"`
	Version          string          `json:"version"`
	Currency         string          `json:"currency"`
	BaselineSeconds  int64           `json:"baselineSeconds"`
	LoadedHourlyCost string          `json:"loadedHourlyCost"`
	Assumptions      json.RawMessage `json:"assumptions,omitempty"`
	Explanation      json.RawMessage `json:"explanation,omitempty"`
}

type ValueModel struct {
	ID               string          `json:"id"`
	Ref              string          `json:"ref"`
	Version          string          `json:"version"`
	Formula          string          `json:"formula"`
	Currency         string          `json:"currency"`
	BaselineSeconds  int64           `json:"baselineSeconds"`
	LoadedHourlyCost string          `json:"loadedHourlyCost"`
	Assumptions      json.RawMessage `json:"assumptions"`
	Explanation      json.RawMessage `json:"explanation"`
	CreatedAt        time.Time       `json:"createdAt"`
}

var exactMoneyPattern = regexp.MustCompile(`^[0-9]+(?:\.[0-9]{1,6})?$`)
var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

func (s *Store) CreateValueModel(ctx context.Context, in ValueModelInput) (ValueModel, error) {
	if err := validateValueModelInput(in); err != nil {
		return ValueModel{}, err
	}
	var out ValueModel
	err := s.pool.QueryRow(ctx, `
		INSERT INTO work.value_models(ref,version,currency,baseline_seconds,loaded_hourly_cost,assumptions,explanation)
		VALUES($1,$2,$3,$4,$5::numeric,$6,$7)
		RETURNING id,ref,version,formula,currency,baseline_seconds,loaded_hourly_cost::text,assumptions,explanation,created_at`,
		in.Ref, in.Version, in.Currency, in.BaselineSeconds, in.LoadedHourlyCost, jsonOrObject(in.Assumptions), jsonOrObject(in.Explanation)).
		Scan(&out.ID, &out.Ref, &out.Version, &out.Formula, &out.Currency, &out.BaselineSeconds, &out.LoadedHourlyCost, &out.Assumptions, &out.Explanation, &out.CreatedAt)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return ValueModel{}, fmt.Errorf("%w: value model ref/version already exists", ErrConflict)
		}
		return ValueModel{}, err
	}
	return out, nil
}

func validateValueModelInput(in ValueModelInput) error {
	if in.Ref == "" || in.Version == "" || in.Currency == "" || in.BaselineSeconds <= 0 || in.LoadedHourlyCost == "" {
		return fmt.Errorf("%w: ref, version, currency, baselineSeconds, and loadedHourlyCost are required", ErrInvalidInput)
	}
	cost, ok := new(big.Rat).SetString(in.LoadedHourlyCost)
	if !currencyPattern.MatchString(in.Currency) || !exactMoneyPattern.MatchString(in.LoadedHourlyCost) || !ok || cost.Sign() <= 0 {
		return fmt.Errorf("%w: currency must be ISO-4217 and loadedHourlyCost must be a positive decimal with at most six fractional digits", ErrInvalidInput)
	}
	if (len(in.Assumptions) > 0 && !json.Valid(in.Assumptions)) || (len(in.Explanation) > 0 && !json.Valid(in.Explanation)) {
		return fmt.Errorf("%w: assumptions and explanation must be valid JSON", ErrInvalidInput)
	}
	return nil
}

type DashboardSummary struct {
	Counts map[string]int64  `json:"counts"`
	Value  ValueSummary      `json:"value"`
	Trend  []ValueTrendPoint `json:"trend"`
}
type ValueSummary struct {
	Configured bool                 `json:"configured"`
	Estimated  bool                 `json:"estimated"`
	Totals     []CurrencyAmount     `json:"totals"`
	Models     []ValueModelSnapshot `json:"models"`
}

// ValueModelSnapshot is the immutable, customer-safe explanation attached to
// the attempts relevant to a Dashboard selection.
type ValueModelSnapshot struct {
	Ref              string          `json:"ref"`
	Version          string          `json:"version"`
	Formula          string          `json:"formula"`
	Currency         string          `json:"currency"`
	BaselineSeconds  int64           `json:"baselineSeconds"`
	LoadedHourlyCost string          `json:"loadedHourlyCost"`
	Assumptions      json.RawMessage `json:"assumptions"`
	Explanation      json.RawMessage `json:"explanation"`
}
type CurrencyAmount struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}
type ValueTrendPoint struct {
	Date     string `json:"date"`
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

//nolint:gocyclo // Counts, relevant immutable model snapshots, totals, and trend form one read projection.
func (s *Store) Dashboard(ctx context.Context, from, to time.Time) (DashboardSummary, error) {
	out := DashboardSummary{
		Counts: map[string]int64{StateTodo: 0, StateInProgress: 0, StateBlocked: 0, StateDone: 0, StateFailed: 0},
		Trend:  make([]ValueTrendPoint, 0),
	}
	// Active work remains on the board regardless of age; terminal work is
	// selected by completion time. Keep these counts identical to ListWorkItems.
	rows, err := s.pool.Query(ctx, `
		SELECT customer_state,COUNT(*) FROM work.current_work_items
		WHERE customer_state NOT IN ('done','failed')
		   OR (COALESCE(finished_at,created_at) >= $1 AND COALESCE(finished_at,created_at) < $2)
		GROUP BY customer_state`, from, to)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			rows.Close()
			return out, err
		}
		out.Counts[state] = count
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return out, err
	}
	// Configuration comes only from immutable snapshots attached to work/value
	// relevant to this selection. A globally registered but unreferenced model
	// must never turn an honest "not configured" Dashboard into configured.
	var relevant, configured int64
	if err := s.pool.QueryRow(ctx, `
		WITH relevant_attempts AS (
			SELECT current_attempt_id AS id FROM work.current_work_items
			WHERE customer_state NOT IN ('done','failed')
			   OR (COALESCE(finished_at,created_at) >= $1 AND COALESCE(finished_at,created_at) < $2)
			UNION
			SELECT attempt_id AS id FROM work.value_ledger
			WHERE created_at >= $1 AND created_at < $2
		)
		SELECT COUNT(*),COUNT(*) FILTER (WHERE a.value_model_id IS NOT NULL AND a.value_model_snapshot IS NOT NULL)
		FROM relevant_attempts r JOIN work.attempts a ON a.id=r.id`, from, to).Scan(&relevant, &configured); err != nil {
		return out, err
	}
	out.Value = ValueSummary{Configured: relevant > 0 && configured == relevant, Estimated: relevant > 0 && configured == relevant, Totals: make([]CurrencyAmount, 0), Models: make([]ValueModelSnapshot, 0)}
	if !out.Value.Configured {
		return out, nil
	}
	modelRows, err := s.pool.Query(ctx, `
		WITH relevant_attempts AS (
			SELECT current_attempt_id AS id FROM work.current_work_items
			WHERE customer_state NOT IN ('done','failed')
			   OR (COALESCE(finished_at,created_at) >= $1 AND COALESCE(finished_at,created_at) < $2)
			UNION
			SELECT attempt_id AS id FROM work.value_ledger
			WHERE created_at >= $1 AND created_at < $2
		)
		SELECT a.value_model_snapshot
		FROM relevant_attempts r JOIN work.attempts a ON a.id=r.id
		GROUP BY a.value_model_snapshot ORDER BY a.value_model_snapshot::text`, from, to)
	if err != nil {
		return out, err
	}
	for modelRows.Next() {
		var raw []byte
		if err := modelRows.Scan(&raw); err != nil {
			modelRows.Close()
			return out, err
		}
		var model ValueModelSnapshot
		if err := json.Unmarshal(raw, &model); err != nil {
			modelRows.Close()
			return out, err
		}
		out.Value.Models = append(out.Value.Models, model)
	}
	modelRows.Close()
	if err := modelRows.Err(); err != nil {
		return out, err
	}

	totals := make([]CurrencyAmount, 0)
	valueRows, err := s.pool.Query(ctx, `
		SELECT SUM(amount)::text,currency FROM work.value_ledger
		WHERE created_at >= $1 AND created_at < $2 GROUP BY currency ORDER BY currency`, from, to)
	if err != nil {
		return out, err
	}
	for valueRows.Next() {
		var total CurrencyAmount
		if err := valueRows.Scan(&total.Amount, &total.Currency); err != nil {
			valueRows.Close()
			return out, err
		}
		totals = append(totals, total)
	}
	valueRows.Close()
	if err := valueRows.Err(); err != nil {
		return out, err
	}
	out.Value.Totals = totals
	trendRows, err := s.pool.Query(ctx, `
		SELECT created_at::date::text,SUM(amount)::text,currency FROM work.value_ledger
		WHERE created_at >= $1 AND created_at < $2
		GROUP BY created_at::date,currency ORDER BY created_at::date,currency`, from, to)
	if err != nil {
		return out, err
	}
	defer trendRows.Close()
	for trendRows.Next() {
		var p ValueTrendPoint
		if err := trendRows.Scan(&p.Date, &p.Amount, &p.Currency); err != nil {
			return out, err
		}
		out.Trend = append(out.Trend, p)
	}
	return out, trendRows.Err()
}
