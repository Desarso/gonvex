package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// cronSchedule computes the next fire time strictly after a given instant.
type cronSchedule interface {
	Next(after time.Time) time.Time
}

// intervalSchedule fires every interval, measured from the previous fire.
type intervalSchedule struct {
	interval time.Duration
}

func (s intervalSchedule) Next(after time.Time) time.Time {
	if s.interval <= 0 {
		return time.Time{}
	}
	return after.Add(s.interval)
}

// exprSchedule is a parsed standard 5-field cron expression
// (minute hour day-of-month month day-of-week, Sunday = 0).
type exprSchedule struct {
	minute  [60]bool
	hour    [24]bool
	dom     [32]bool
	month   [13]bool
	dow     [7]bool
	domStar bool
	dowStar bool
}

func parseCronExpr(expr string) (exprSchedule, error) {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != 5 {
		return exprSchedule{}, fmt.Errorf("cron expression %q must have 5 fields, got %d", expr, len(fields))
	}

	var s exprSchedule
	if err := fillField(fields[0], 0, 59, s.minute[:], nil); err != nil {
		return exprSchedule{}, fmt.Errorf("minute: %w", err)
	}
	if err := fillField(fields[1], 0, 23, s.hour[:], nil); err != nil {
		return exprSchedule{}, fmt.Errorf("hour: %w", err)
	}
	if err := fillField(fields[2], 1, 31, s.dom[:], &s.domStar); err != nil {
		return exprSchedule{}, fmt.Errorf("day-of-month: %w", err)
	}
	if err := fillField(fields[3], 1, 12, s.month[:], nil); err != nil {
		return exprSchedule{}, fmt.Errorf("month: %w", err)
	}
	if err := fillDOW(fields[4], s.dow[:], &s.dowStar); err != nil {
		return exprSchedule{}, fmt.Errorf("day-of-week: %w", err)
	}
	return s, nil
}

func (s exprSchedule) Next(after time.Time) time.Time {
	t := after.Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(1, 0, 1)
	for t.Before(limit) {
		if s.matches(t) {
			return t
		}
		t = t.Add(time.Minute)
	}
	return time.Time{}
}

func (s exprSchedule) matches(t time.Time) bool {
	if !s.minute[t.Minute()] || !s.hour[t.Hour()] || !s.month[int(t.Month())] {
		return false
	}
	domMatch := s.dom[t.Day()]
	dowMatch := s.dow[int(t.Weekday())]
	switch {
	case s.domStar && s.dowStar:
		return true
	case s.domStar:
		return dowMatch
	case s.dowStar:
		return domMatch
	default:
		// When both day fields are restricted, standard cron matches either.
		return domMatch || dowMatch
	}
}

// fillField parses a single cron field into the allowed-value table. star, when
// non-nil, is set to true if the field is "*" (used for day-of-month/week OR
// semantics).
func fillField(field string, min, max int, table []bool, star *bool) error {
	if field == "*" || strings.HasPrefix(field, "*/") {
		if star != nil {
			*star = field == "*"
		}
	}
	for _, part := range strings.Split(field, ",") {
		if err := fillRange(part, min, max, table); err != nil {
			return err
		}
	}
	return nil
}

func fillRange(part string, min, max int, table []bool) error {
	step := 1
	if slash := strings.IndexByte(part, '/'); slash >= 0 {
		stepValue, err := strconv.Atoi(part[slash+1:])
		if err != nil || stepValue <= 0 {
			return fmt.Errorf("invalid step in %q", part)
		}
		step = stepValue
		part = part[:slash]
	}

	lo, hi := min, max
	switch {
	case part == "*":
		// full range
	case strings.IndexByte(part, '-') >= 0:
		bounds := strings.SplitN(part, "-", 2)
		var err error
		if lo, err = strconv.Atoi(bounds[0]); err != nil {
			return fmt.Errorf("invalid range start in %q", part)
		}
		if hi, err = strconv.Atoi(bounds[1]); err != nil {
			return fmt.Errorf("invalid range end in %q", part)
		}
	default:
		value, err := strconv.Atoi(part)
		if err != nil {
			return fmt.Errorf("invalid value %q", part)
		}
		lo, hi = value, value
	}

	if lo < min || hi > max || lo > hi {
		return fmt.Errorf("value out of range [%d,%d] in %q", min, max, part)
	}
	for value := lo; value <= hi; value += step {
		table[value] = true
	}
	return nil
}

// fillDOW parses the day-of-week field, normalising Sunday given as 7 to 0.
func fillDOW(field string, table []bool, star *bool) error {
	*star = field == "*"
	normalized := strings.ReplaceAll(field, "7", "0")
	for _, part := range strings.Split(normalized, ",") {
		if err := fillRange(part, 0, 6, table); err != nil {
			return err
		}
	}
	return nil
}
