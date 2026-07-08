package datafiles

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/extrame/xls"
	"github.com/xuri/excelize/v2"
)

// ingestCSV loads a delimited text file with DuckDB's native reader, which
// infers column types and streams arbitrarily large files.
//
// DuckDB's parallel scanner rejects null_padding when quoted fields contain
// newlines (multiline cells such as task descriptions), so ragged-row
// tolerance and multiline support need different option sets: fall back to a
// single-threaded scan (keeps null_padding), then to dropping null_padding.
func ingestCSV(ctx context.Context, db *sql.DB, path string) ([]string, error) {
	optionSets := []string{
		`header=true, null_padding=true, ignore_errors=true`,
		`header=true, null_padding=true, ignore_errors=true, parallel=false`,
		`header=true, ignore_errors=true`,
	}
	var firstErr error
	for _, options := range optionSets {
		_, err := db.ExecContext(ctx, fmt.Sprintf(
			`CREATE TABLE %s AS SELECT * FROM read_csv_auto(%s, %s)`,
			quoteIdent("data"), quoteString(path), options))
		if err == nil {
			return nil, nil
		}
		if firstErr == nil {
			firstErr = err
		}
		db.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, quoteIdent("data")))
	}
	return nil, fmt.Errorf("parse CSV: %w", firstErr)
}

// sheetWriter ingests one worksheet's rows (header first) into a VARCHAR
// table, shared by the XLSX and XLS paths.
type sheetWriter struct {
	db        *sql.DB
	tableName string
	columns   []string
	batch     [][]string
	rowCount  int64
	warnings  []string
}

const sheetInsertBatch = 500

func newSheetWriter(ctx context.Context, db *sql.DB, sheetName string, header []string) (*sheetWriter, []string, error) {
	var warnings []string
	if len(header) > MaxColumns {
		warnings = append(warnings, fmt.Sprintf("Sheet %q has %d columns; only the first %d were ingested.", sheetName, len(header), MaxColumns))
		header = header[:MaxColumns]
	}
	columns := normalizeColumnNames(header)
	if len(columns) == 0 {
		return nil, warnings, nil
	}
	tableName := sanitizeTableName(sheetName)
	defs := make([]string, len(columns))
	for i, column := range columns {
		defs[i] = quoteIdent(column) + " VARCHAR"
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %s (%s)`, quoteIdent(tableName), strings.Join(defs, ", "))); err != nil {
		return nil, warnings, fmt.Errorf("create table for sheet %q: %w", sheetName, err)
	}
	return &sheetWriter{db: db, tableName: tableName, columns: columns}, warnings, nil
}

func (w *sheetWriter) add(ctx context.Context, cells []string) error {
	if w.rowCount >= MaxSheetRows {
		return nil
	}
	row := make([]string, len(w.columns))
	empty := true
	for i := range w.columns {
		if i < len(cells) {
			row[i] = cells[i]
			if strings.TrimSpace(cells[i]) != "" {
				empty = false
			}
		}
	}
	if empty {
		return nil
	}
	w.batch = append(w.batch, row)
	w.rowCount++
	if len(w.batch) >= sheetInsertBatch {
		return w.flush(ctx)
	}
	return nil
}

func (w *sheetWriter) flush(ctx context.Context) error {
	if len(w.batch) == 0 {
		return nil
	}
	placeholderRow := "(" + strings.TrimSuffix(strings.Repeat("?,", len(w.columns)), ",") + ")"
	placeholders := make([]string, len(w.batch))
	args := make([]any, 0, len(w.batch)*len(w.columns))
	for i, row := range w.batch {
		placeholders[i] = placeholderRow
		for _, cell := range row {
			if cell == "" {
				args = append(args, nil)
			} else {
				args = append(args, cell)
			}
		}
	}
	query := fmt.Sprintf(`INSERT INTO %s VALUES %s`, quoteIdent(w.tableName), strings.Join(placeholders, ","))
	if _, err := w.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("insert rows into %s: %w", w.tableName, err)
	}
	w.batch = w.batch[:0]
	return nil
}

func (w *sheetWriter) finish(ctx context.Context, sheetName string) ([]string, error) {
	if err := w.flush(ctx); err != nil {
		return w.warnings, err
	}
	if w.rowCount >= MaxSheetRows {
		w.warnings = append(w.warnings, fmt.Sprintf("Sheet %q was truncated at %d rows.", sheetName, MaxSheetRows))
	}
	return w.warnings, nil
}

func ingestXLSX(ctx context.Context, db *sql.DB, path string) ([]string, error) {
	workbook, err := excelize.OpenFile(path)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "encrypt") {
			return nil, fmt.Errorf("workbook is encrypted or password-protected and cannot be ingested")
		}
		return nil, fmt.Errorf("parse XLSX: %w", err)
	}
	defer workbook.Close()

	var warnings []string
	ingested := 0
	for _, sheetName := range workbook.GetSheetList() {
		rows, err := workbook.Rows(sheetName)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("Sheet %q could not be read: %v", sheetName, err))
			continue
		}
		var writer *sheetWriter
		for rows.Next() {
			cells, err := rows.Columns()
			if err != nil {
				rows.Close()
				return warnings, fmt.Errorf("read sheet %q: %w", sheetName, err)
			}
			if writer == nil {
				if rowIsEmpty(cells) {
					continue
				}
				created, headerWarnings, err := newSheetWriter(ctx, db, sheetName, cells)
				warnings = append(warnings, headerWarnings...)
				if err != nil {
					rows.Close()
					return warnings, err
				}
				if created == nil {
					break
				}
				writer = created
				continue
			}
			if err := writer.add(ctx, cells); err != nil {
				rows.Close()
				return warnings, err
			}
		}
		rows.Close()
		if writer == nil {
			warnings = append(warnings, fmt.Sprintf("Sheet %q is empty and was skipped.", sheetName))
			continue
		}
		sheetWarnings, err := writer.finish(ctx, sheetName)
		warnings = append(warnings, sheetWarnings...)
		if err != nil {
			return warnings, err
		}
		ingested++
	}
	if ingested == 0 {
		return warnings, fmt.Errorf("workbook has no non-empty sheets")
	}
	return warnings, nil
}

func ingestXLS(ctx context.Context, db *sql.DB, path string) ([]string, error) {
	workbook, err := xls.Open(path, "utf-8")
	if err != nil {
		return nil, fmt.Errorf("parse XLS (if the workbook is encrypted or protected it cannot be ingested): %w", err)
	}

	var warnings []string
	ingested := 0
	for sheetIndex := 0; sheetIndex < workbook.NumSheets(); sheetIndex++ {
		sheet := workbook.GetSheet(sheetIndex)
		if sheet == nil {
			continue
		}
		sheetName := sheet.Name
		if strings.TrimSpace(sheetName) == "" {
			sheetName = fmt.Sprintf("sheet%d", sheetIndex+1)
		}
		var writer *sheetWriter
		for rowIndex := 0; rowIndex <= int(sheet.MaxRow); rowIndex++ {
			row := sheet.Row(rowIndex)
			if row == nil {
				continue
			}
			cells := make([]string, 0, row.LastCol())
			for col := 0; col < row.LastCol(); col++ {
				cells = append(cells, row.Col(col))
			}
			if writer == nil {
				if rowIsEmpty(cells) {
					continue
				}
				created, headerWarnings, err := newSheetWriter(ctx, db, sheetName, cells)
				warnings = append(warnings, headerWarnings...)
				if err != nil {
					return warnings, err
				}
				if created == nil {
					break
				}
				writer = created
				continue
			}
			if err := writer.add(ctx, cells); err != nil {
				return warnings, err
			}
		}
		if writer == nil {
			warnings = append(warnings, fmt.Sprintf("Sheet %q is empty and was skipped.", sheetName))
			continue
		}
		sheetWarnings, err := writer.finish(ctx, sheetName)
		warnings = append(warnings, sheetWarnings...)
		if err != nil {
			return warnings, err
		}
		ingested++
	}
	if ingested == 0 {
		return warnings, fmt.Errorf("workbook has no non-empty sheets")
	}
	return warnings, nil
}

func rowIsEmpty(cells []string) bool {
	for _, cell := range cells {
		if strings.TrimSpace(cell) != "" {
			return false
		}
	}
	return true
}

var identCleaner = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func sanitizeTableName(name string) string {
	cleaned := identCleaner.ReplaceAllString(strings.TrimSpace(name), "_")
	cleaned = strings.Trim(cleaned, "_")
	if cleaned == "" {
		return "data"
	}
	return cleaned
}

// normalizeColumnNames turns a header row into unique non-empty identifiers.
func normalizeColumnNames(header []string) []string {
	if rowIsEmpty(header) {
		return nil
	}
	seen := map[string]int{}
	columns := make([]string, 0, len(header))
	for i, raw := range header {
		name := strings.TrimSpace(raw)
		if name == "" {
			name = fmt.Sprintf("column_%d", i+1)
		}
		key := strings.ToLower(name)
		count := seen[key]
		seen[key] = count + 1
		if count > 0 {
			name = fmt.Sprintf("%s_%d", name, count+1)
		}
		columns = append(columns, name)
	}
	return columns
}
