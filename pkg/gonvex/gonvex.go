package gonvex

type App struct{}

type QueryCtx struct{}
type MutationCtx struct{}
type ActionCtx struct{}
type HTTPContext struct{}

func (a *App) Query(path string, handler any)            {}
func (a *App) Mutation(path string, handler any)         {}
func (a *App) Action(path string, handler any)           {}
func (a *App) HTTP(path string, handler any)             {}
func (a *App) InternalMutation(path string, handler any) {}
func (a *App) LiveGrid(path string, handler any)         {}

type Schema struct{}
type Table struct{}

type ColumnOption func(*columnOptions)

type columnOptions struct {
	nullable bool
}

func Nullable(options *columnOptions) {
	options.nullable = true
}

func (s *Schema) Table(name string, define func(*Table)) {}

func (t *Table) ID(name string)                               {}
func (t *Table) String(name string, options ...ColumnOption)  {}
func (t *Table) Text(name string, options ...ColumnOption)    {}
func (t *Table) Int(name string, options ...ColumnOption)     {}
func (t *Table) Int64(name string, options ...ColumnOption)   {}
func (t *Table) Float64(name string, options ...ColumnOption) {}
func (t *Table) Bool(name string, options ...ColumnOption)    {}
func (t *Table) Time(name string, options ...ColumnOption)    {}
func (t *Table) JSON(name string, options ...ColumnOption)    {}

func (t *Table) Index(name string, columns ...string)       {}
func (t *Table) UniqueIndex(name string, columns ...string) {}
