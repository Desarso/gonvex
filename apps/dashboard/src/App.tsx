import {
  CompactSelection,
  DataEditor,
  GridCellKind,
  type GridCell,
  type GridColumn,
  type CustomCell,
  type CustomRenderer,
  type Item,
  type Rectangle,
  type GridSelection,
  type SpriteMap,
  type Theme,
} from "@glideapps/glide-data-grid";
import "@glideapps/glide-data-grid/dist/index.css";
import { GonvexClient } from "@gonvex/client";
import { Button, Card, Chip, Separator } from "@heroui/react";
import { useEffect, useRef, useState, type ReactNode } from "react";
import type { JsonValue } from "@gonvex/protocol";
import { api } from "../gonvex/_generated/api";

type PageID = "overview" | "functions" | "data" | "test" | "files" | "realtime" | "settings";

type GridRow = string[];

type Page = {
  id: PageID;
  label: string;
  eyebrow: string;
  title: string;
  description: string;
};

type DataTableInfo = {
  name: string;
  columns: string[];
  rowCount: number;
};

type DataRowsResponse = {
  table: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total?: number;
  offset?: number;
  limit?: number;
};

type FunctionInfo = {
  name: string;
  kind: string;
  realtime: string;
  source: string;
  status: string;
};

type FileInfo = {
  id: string;
  size: string;
  contentType: string;
  uploadedAt: string;
  objectUrl?: string;
  source: "runtime" | "local";
};

type ThemeMode = "dark" | "light";

type ActionHandler = (message: string) => void;

type SortDirection = "asc" | "desc";

type SortState<T extends string> = {
  key: T;
  direction: SortDirection;
};

type TestSortState = {
  key: string;
  direction: SortDirection | "default";
};

type TestTaskGridArgs = {
  offset: number;
  limit: number;
  columns: string[];
  count: "false" | "estimate";
  search?: string;
  sort?: string;
  direction?: SortDirection;
};

type FileSortKey = "id" | "size" | "contentType" | "uploadedAt";

type DataFilter = {
  id: string;
  column: string;
  operator: "contains" | "equals" | "notEquals" | "startsWith" | "endsWith" | "empty" | "notEmpty";
  value: string;
};

type InsertRowResponse = {
  table: string;
  row: Record<string, unknown>;
};

type RandomizeTasksResponse = {
  updated: number;
  requested: number;
  durationMs: number;
};

const pages: Page[] = [
  {
    id: "overview",
    label: "Overview",
    eyebrow: "Runtime pulse",
    title: "Realtime control room",
    description: "Watch the local project runtime, generated bindings, schema sync, and grid surface from one place.",
  },
  {
    id: "functions",
    label: "Functions",
    eyebrow: "Function manifest",
    title: "Live backend surface",
    description: "Queries, mutations, storage helpers, and LiveGrid registrations extracted from app-local Go files.",
  },
  {
    id: "data",
    label: "Data",
    eyebrow: "Schema sync",
    title: "Postgres project schema",
    description: "Tables, columns, and indexes defined in gonvex/schema.go and applied safely by the Gonvex Runtime.",
  },
  {
    id: "test",
    label: "Test",
    eyebrow: "Task table lab",
    title: "Whagons-style Glide table",
    description: "Temporary sandbox for rendering task rows with custom Glide Data Grid cells inspired by the Whagons workspace table.",
  },
  {
    id: "files",
    label: "Files",
    eyebrow: "Object storage",
    title: "MinIO upload lab",
    description: "Storage routes and bucket configuration for S3-compatible upload testing.",
  },
  {
    id: "realtime",
    label: "Realtime",
    eyebrow: "Subscriptions",
    title: "Invalidation timeline",
    description: "The upcoming query dependency and LiveGrid patch stream dashboard.",
  },
  {
    id: "settings",
    label: "Settings",
    eyebrow: "Project config",
    title: "Local runtime settings",
    description: "Environment and runtime target configuration used by gonvex dev.",
  },
];

const functionColumns: GridColumn[] = [
  { title: "Function", id: "function", width: 180 },
  { title: "Kind", id: "kind", width: 120 },
  { title: "Realtime", id: "realtime", width: 120 },
  { title: "Source", id: "source", width: 220 },
  { title: "Status", id: "status", width: 160 },
];

const functionRows: GridRow[] = [
  ["tasks.list", "query", "live", "gonvex/tasks.go", "ready"],
  ["tasks.create", "mutation", "invalidates", "gonvex/tasks.go", "ready"],
  ["tasks.randomizeStatusPriority", "mutation", "coalesced", "gonvex/tasks.go", "ready"],
  ["files.createUploadUrl", "mutation", "storage", "gonvex/files.go", "planned"],
  ["tasks.grid", "liveGrid", "patch stream", "gonvex/tasks.go", "ready"],
];

const functions: FunctionInfo[] = functionRows.map(([name, kind, realtime, source, status]) => ({
  name,
  kind,
  realtime,
  source,
  status,
}));

const functionStats = [
  ["Function Calls", "0", "blue"],
  ["Errors", "0", "red"],
  ["Execution Time", "0 ms", "orange"],
  ["Cache Hit Rate", "100%", "blue"],
];

const schemaColumns: GridColumn[] = [
  { title: "Table", id: "table", width: 130 },
  { title: "Column", id: "column", width: 150 },
  { title: "Type", id: "type", width: 120 },
  { title: "Nullable", id: "nullable", width: 110 },
  { title: "Index", id: "index", width: 220 },
];

const schemaRows: GridRow[] = [
  ["tasks", "id", "id", "no", "primary key"],
  ["tasks", "title", "string", "no", ""],
  ["tasks", "status", "string", "no", "tasks_by_status"],
  ["tasks", "description", "text", "yes", ""],
  ["tasks", "priority", "string", "yes", "tasks_by_priority"],
  ["tasks", "assignee", "string", "yes", "tasks_by_assignee"],
  ["tasks", "project", "string", "yes", "tasks_by_project"],
  ["tasks", "label", "string", "yes", ""],
  ["tasks", "due_at", "time", "yes", "tasks_by_due_at"],
  ["tasks", "completed", "bool", "yes", ""],
  ["tasks", "estimate_minutes", "int", "yes", ""],
  ["tasks", "progress", "int", "yes", ""],
  ["tasks", "created_at", "time", "no", "tasks_by_created_at"],
  ["tasks", "updated_at", "time", "yes", ""],
  ["files", "id", "id", "no", "primary key"],
  ["files", "key", "string", "no", "files_by_key unique"],
  ["files", "content_type", "string", "yes", ""],
  ["files", "size", "int64", "yes", ""],
  ["files", "created_at", "time", "no", ""],
];

const fallbackTables: DataTableInfo[] = [
  {
    name: "tasks",
    columns: [
      "id",
      "assignee",
      "completed",
      "created_at",
      "description",
      "due_at",
      "estimate_minutes",
      "label",
      "priority",
      "progress",
      "project",
      "status",
      "title",
      "updated_at",
    ],
    rowCount: 0,
  },
  { name: "files", columns: ["id", "key", "content_type", "size", "created_at"], rowCount: 0 },
];

const fallbackRows: Record<string, Record<string, unknown>[]> = {
  tasks: [],
  files: [],
};

const runtimeBaseURL = import.meta.env.MODE === "test" ? "" : "http://localhost:8080";
const gonvexClient = runtimeBaseURL ? new GonvexClient(`${runtimeBaseURL.replace(/^http/, "ws")}/ws`) : null;
const pageSize = 300;
const rowFetchPadding = 80;
const rowFetchStride = 150;
const maxFrontendCachedRows = 100_000;

function offsetForVisibleRow(row: number): number {
  return Math.max(0, Math.floor(Math.max(0, row - rowFetchPadding) / rowFetchStride) * rowFetchStride);
}

function mergeRowsIntoCache<T>(current: Record<number, T>, rows: T[], offset: number, maxRows = maxFrontendCachedRows) {
  const next = { ...current };
  rows.forEach((row, index) => {
    next[offset + index] = row;
  });

  const keys = Object.keys(next);
  if (keys.length <= maxRows) return next;

  const center = offset + Math.floor(rows.length / 2);
  let left = 0;
  let right = keys.length - 1;
  let toDelete = keys.length - maxRows;
  while (toDelete > 0 && left <= right) {
    const leftKey = Number(keys[left]);
    const rightKey = Number(keys[right]);
    if (Math.abs(leftKey - center) > Math.abs(rightKey - center)) {
      delete next[leftKey];
      left += 1;
    } else {
      delete next[rightKey];
      right -= 1;
    }
    toDelete -= 1;
  }

  return next;
}

function replaceRowsInCache<T>(current: Record<number, T>, rows: T[], offset: number, limit: number) {
  const next = { ...current };
  for (let index = offset; index < offset + limit; index += 1) {
    delete next[index];
  }
  return mergeRowsIntoCache(next, rows, offset);
}

const metrics = [
  ["Queries", "1 live"],
  ["Mutations", "2 wired"],
  ["Storage", "planned"],
  ["Tables", "2 synced"],
];

const activity = [
  ["schema", "tasks/files applied to gonvex_dev"],
  ["codegen", "4 generated function bindings"],
  ["grid", "Glide Data Grid mounted"],
  ["runtime", "safe migrations only"],
];

const settings = [
  ["Runtime URL", "http://localhost:8080"],
  ["Database", "gonvex_dev"],
  ["Storage bucket", "gonvex-dev"],
  ["Dev script", "gonvex dev -- vite"],
];

const darkGridTheme: Partial<Theme> = {
  accentColor: "#e5482f",
  accentFg: "#ffffff",
  accentLight: "#3a2320",
  bgCell: "#221d1c",
  bgCellMedium: "#292322",
  bgHeader: "#302927",
  bgHeaderHasFocus: "#3a302d",
  bgHeaderHovered: "#3a302d",
  borderColor: "#433936",
  cellHorizontalPadding: 12,
  cellVerticalPadding: 8,
  fgIconHeader: "#f5efec",
  fontFamily: "Geist, ui-sans-serif, system-ui, sans-serif",
  headerIconSize: 16,
  headerFontStyle: "600 12px",
  lineHeight: 1.45,
  markerFontStyle: "11px",
  roundingRadius: 6,
  textDark: "#f5efec",
  textHeader: "#d8ceca",
  textHeaderSelected: "#ffffff",
  textLight: "#8f837e",
  textMedium: "#c8beb9",
};

const lightGridTheme: Partial<Theme> = {
  accentColor: "#d93f45",
  accentFg: "#ffffff",
  accentLight: "#f4d2d0",
  bgCell: "#f8f5f3",
  bgCellMedium: "#eee8e5",
  bgHeader: "#ebe5e2",
  bgHeaderHasFocus: "#e2d9d5",
  bgHeaderHovered: "#e2d9d5",
  borderColor: "#d9d0cc",
  cellHorizontalPadding: 12,
  cellVerticalPadding: 8,
  fgIconHeader: "#211c1b",
  fontFamily: "Geist, ui-sans-serif, system-ui, sans-serif",
  headerIconSize: 16,
  headerFontStyle: "600 12px",
  lineHeight: 1.45,
  markerFontStyle: "11px",
  roundingRadius: 6,
  textDark: "#211c1b",
  textHeader: "#5e5551",
  textHeaderSelected: "#211c1b",
  textLight: "#867b76",
  textMedium: "#4c4440",
};

function gridThemeFor(mode: ThemeMode): Partial<Theme> {
  return mode === "dark" ? darkGridTheme : lightGridTheme;
}

function emptyGridSelection(): GridSelection {
  return {
    columns: CompactSelection.empty(),
    rows: CompactSelection.empty(),
  };
}

const glideHeaderIcons: SpriteMap = {
  listFilter: ({ fgColor }) => `
    <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 30 24" fill="none" stroke="${fgColor}" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round">
      <path d="M2 5h20"/>
      <path d="M6 12h12"/>
      <path d="M9 19h6"/>
    </svg>
  `,
};

function withHeaderFilterIcon(column: GridColumn): GridColumn {
  return {
    ...column,
    hasMenu: column.hasMenu ?? true,
    menuIcon: column.menuIcon ?? "listFilter",
  };
}

function createCellGetter(rows: GridRow[]) {
  return ([column, row]: Item): GridCell => ({
    kind: GridCellKind.Text,
    allowOverlay: false,
    displayData: rows[row]?.[column] ?? "",
    data: rows[row]?.[column] ?? "",
  });
}

function createCachedCellGetter(columns: string[], rowCache: Record<number, Record<string, unknown>>) {
  return ([column, row]: Item): GridCell => {
    const value = rowCache[row]?.[columns[column]];
    const displayData = value === undefined ? "" : formatCellValue(value);
    return {
      kind: GridCellKind.Text,
      allowOverlay: false,
      displayData,
      data: displayData,
    };
  };
}

type WhTaskCellData = {
  kind: "taskName" | "statusPill" | "priorityPill" | "dateStack" | "progressBar" | "flag";
  primary: string;
  secondary?: string;
  color?: string;
  textColor?: string;
  muted?: boolean;
  progress?: number;
};

function whTaskCell(data: WhTaskCellData): CustomCell<WhTaskCellData> {
  return {
    kind: GridCellKind.Custom,
    allowOverlay: false,
    copyData: [data.primary, data.secondary].filter(Boolean).join(" "),
    data,
  };
}

function drawRoundRect(ctx: CanvasRenderingContext2D, x: number, y: number, width: number, height: number, radius: number) {
  const r = Math.min(radius, width / 2, height / 2);
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + width, y, x + width, y + height, r);
  ctx.arcTo(x + width, y + height, x, y + height, r);
  ctx.arcTo(x, y + height, x, y, r);
  ctx.arcTo(x, y, x + width, y, r);
  ctx.closePath();
}

function canvasEllipsize(ctx: CanvasRenderingContext2D, text: string, maxWidth: number): string {
  if (ctx.measureText(text).width <= maxWidth) return text;
  let next = text;
  while (next.length > 1 && ctx.measureText(`${next}...`).width > maxWidth) {
    next = next.slice(0, -1);
  }
  return `${next}...`;
}

const whTaskRenderer: CustomRenderer = {
  kind: GridCellKind.Custom,
  isMatch: (cell): cell is CustomCell<WhTaskCellData> => {
    const data = cell.data as Partial<WhTaskCellData> | undefined;
    return data?.kind === "taskName" || data?.kind === "statusPill" || data?.kind === "priorityPill" || data?.kind === "dateStack" || data?.kind === "progressBar" || data?.kind === "flag";
  },
  draw: (args, cell) => {
    const { ctx, rect, theme } = args;
    const data = cell.data as WhTaskCellData;
    ctx.save();
    ctx.textBaseline = "middle";
    if (data.kind === "taskName") {
      const compact = rect.width < 260 || rect.height < 58;
      ctx.fillStyle = theme.textDark;
      ctx.font = "650 14px " + theme.fontFamily;
      const primary = canvasEllipsize(ctx, data.primary, rect.width - 28);
      ctx.fillText(primary, rect.x + 14, rect.y + (compact ? rect.height / 2 : 22), rect.width - 28);
      if (data.secondary && !compact) {
        ctx.fillStyle = theme.textMedium;
        ctx.font = "12px " + theme.fontFamily;
        const secondary = canvasEllipsize(ctx, data.secondary, rect.width - 28);
        ctx.fillText(secondary, rect.x + 14, rect.y + 43, rect.width - 28);
      }
    } else if (data.kind === "statusPill" || data.kind === "priorityPill") {
      const label = data.primary || "None";
      ctx.font = "650 12px " + theme.fontFamily;
      const width = Math.min(rect.width - 18, Math.max(68, ctx.measureText(label).width + 24));
      const x = rect.x + 10;
      const y = rect.y + Math.round((rect.height - 26) / 2);
      drawRoundRect(ctx, x, y, width, 26, 13);
      ctx.fillStyle = data.color ?? theme.bgBubble;
      ctx.fill();
      ctx.fillStyle = data.textColor ?? "#111827";
      ctx.fillText(label, x + 12, y + 13, width - 20);
    } else if (data.kind === "dateStack") {
      ctx.fillStyle = data.muted ? theme.textLight : theme.textDark;
      ctx.font = "650 12px " + theme.fontFamily;
      ctx.fillText(data.primary, rect.x + 12, rect.y + 21, rect.width - 24);
      if (data.secondary) {
        ctx.fillStyle = data.muted ? theme.textLight : theme.textMedium;
        ctx.font = "11px " + theme.fontFamily;
        ctx.fillText(data.secondary, rect.x + 12, rect.y + 42, rect.width - 24);
      }
    } else if (data.kind === "progressBar") {
      const x = rect.x + 12;
      const y = rect.y + Math.round((rect.height - 10) / 2);
      const width = rect.width - 24;
      drawRoundRect(ctx, x, y, width, 10, 5);
      ctx.fillStyle = theme.bgCellMedium;
      ctx.fill();
      drawRoundRect(ctx, x, y, Math.max(6, width * Math.min(100, Math.max(0, data.progress ?? 0)) / 100), 10, 5);
      ctx.fillStyle = data.color ?? theme.accentColor;
      ctx.fill();
      ctx.fillStyle = theme.textMedium;
      ctx.font = "11px " + theme.fontFamily;
      ctx.fillText(`${data.progress ?? 0}%`, x, y + 24, width);
    } else if (data.kind === "flag") {
      const color = data.color || "#94a3b8";
      drawRoundRect(ctx, rect.x + 20, rect.y + 16, 18, 28, 5);
      ctx.fillStyle = color;
      ctx.fill();
    }
    ctx.restore();
  },
};

function columnsForDataTable(columns: string[]): GridColumn[] {
  return columns.map((column) => ({
    title: column,
    id: column,
    width: Math.max(150, Math.min(260, column.length * 14 + 80)),
  }));
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function compareValues(left: unknown, right: unknown, direction: SortDirection): number {
  const leftValue = formatCellValue(left).toLowerCase();
  const rightValue = formatCellValue(right).toLowerCase();
  const comparison = leftValue.localeCompare(rightValue, undefined, { numeric: true, sensitivity: "base" });
  return direction === "asc" ? comparison : -comparison;
}

function nextSort<T extends string>(current: SortState<T> | null, key: T): SortState<T> | null {
  if (current?.key === key && current.direction === "asc") return { key, direction: "desc" };
  if (current?.key === key && current.direction === "desc") return null;
  return { key, direction: "asc" };
}

function titleWithSort(title: string, sort: SortState<string> | null, key: string): string {
  if (sort?.key !== key) return title;
  return `${title} ${sort.direction === "asc" ? "↑" : "↓"}`;
}

function titleWithTestSort(title: string, sort: TestSortState, key: string): string {
  if (sort.key !== key) return title;
  if (sort.direction === "default") return title;
  return `${title} ${sort.direction === "asc" ? "↑" : "↓"}`;
}

function nextTestSort(current: TestSortState, key: string): TestSortState {
  if (current.key !== key) return { key, direction: "asc" };
  if (current.direction === "default") return { key, direction: "asc" };
  if (current.direction === "asc") return { key, direction: "desc" };
  return { key, direction: "default" };
}

function defaultValueForColumn(column: string): string {
  if (column.endsWith("_at")) return new Date().toISOString();
  return "";
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
  return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
}

function fileFromRuntimeRow(row: Record<string, unknown>): FileInfo {
  const id = formatCellValue(row.id ?? row.key);
  const sizeValue = Number(row.size ?? 0);
  return {
    id: id || "unknown-file",
    size: Number.isFinite(sizeValue) && sizeValue > 0 ? formatBytes(sizeValue) : formatCellValue(row.size),
    contentType: formatCellValue(row.content_type) || "application/octet-stream",
    uploadedAt: formatCellValue(row.created_at) || "unknown",
    source: "runtime",
  };
}

function sortFiles(files: FileInfo[], sort: SortState<FileSortKey> | null): FileInfo[] {
  if (!sort) return files;
  return [...files].sort((left, right) => compareValues(left[sort.key], right[sort.key], sort.direction));
}

function getPage(id: PageID) {
  return pages.find((page) => page.id === id) ?? pages[0];
}

function pageFromPath(pathname: string): PageID {
  const candidate = pathname.replace(/^\/+/, "").split("/")[0] || "overview";
  return pages.some((page) => page.id === candidate) ? candidate as PageID : "overview";
}

function pathForPage(id: PageID): string {
  return `/${id}`;
}

function storedTheme(): ThemeMode {
  if (typeof window === "undefined") return "dark";
  return window.localStorage.getItem("gonvex-theme") === "light" ? "light" : "dark";
}

function ManifestGrid(props: {
  columns: GridColumn[];
  rows?: GridRow[];
  rowCount?: number;
  getCellContent?: (item: Item) => GridCell;
  customRenderers?: readonly CustomRenderer[];
  height?: number;
  rowHeight?: number;
  themeMode: ThemeMode;
  clearSelection?: boolean;
  onHeaderClick?: (column: number) => void;
  onHeaderMenuClick?: (column: number) => void;
  onColumnResize?: (column: GridColumn, newSize: number, columnIndex: number) => void;
  onVisibleRegionChanged?: (range: Rectangle) => void;
}) {
  const rows = props.rows ?? [];
  const [gridSelection, setGridSelection] = useState<GridSelection>(() => emptyGridSelection());

  useEffect(() => {
    if (props.clearSelection) setGridSelection(emptyGridSelection());
  }, [props.clearSelection]);

  return (
    <div className="grid-frame" data-testid="function-grid">
      <DataEditor
        columns={props.columns.map(withHeaderFilterIcon)}
        customRenderers={props.customRenderers}
        getCellContent={props.getCellContent ?? createCellGetter(rows)}
        gridSelection={gridSelection}
        headerIcons={glideHeaderIcons}
        headerHeight={38}
        height={props.height ?? 360}
        maxColumnWidth={700}
        minColumnWidth={80}
        onColumnResize={props.onColumnResize}
        onGridSelectionChange={setGridSelection}
        onHeaderClicked={props.onHeaderClick ? (column) => props.onHeaderClick?.(column) : undefined}
        onHeaderMenuClick={props.onHeaderMenuClick ?? props.onHeaderClick}
        onVisibleRegionChanged={props.onVisibleRegionChanged ? (range) => props.onVisibleRegionChanged?.(range) : undefined}
        rowHeight={props.rowHeight ?? 40}
        rowMarkers="number"
        rows={props.rowCount ?? rows.length}
        smoothScrollX
        smoothScrollY
        theme={gridThemeFor(props.themeMode)}
        width="100%"
      />
    </div>
  );
}

export function App() {
  const [activePage, setActivePage] = useState<PageID>(() => pageFromPath(window.location.pathname));
  const [theme, setTheme] = useState<ThemeMode>(() => storedTheme());
  const [actionMessage, setActionMessage] = useState("");
  const page = getPage(activePage);
  const themeLabel = theme === "dark" ? "Light mode" : "Dark mode";
  const toggleTheme = () => setTheme((current) => (current === "dark" ? "light" : "dark"));
  const reportAction: ActionHandler = (message) => setActionMessage(message);

  const navigatePage = (id: PageID) => {
    setActivePage(id);
    const nextPath = pathForPage(id);
    if (window.location.pathname !== nextPath) {
      window.history.pushState(null, "", nextPath);
    }
  };

  useEffect(() => {
    const currentPath = pathForPage(activePage);
    if (window.location.pathname !== currentPath) {
      window.history.replaceState(null, "", currentPath);
    }
    const onPopState = () => setActivePage(pageFromPath(window.location.pathname));
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    window.localStorage.setItem("gonvex-theme", theme);
  }, [theme]);

  useEffect(() => {
    if (!actionMessage) return;
    const timeout = window.setTimeout(() => setActionMessage(""), 2600);
    return () => window.clearTimeout(timeout);
  }, [actionMessage]);

  return (
    <main className="app-shell" data-theme={theme}>
      <aside className="sidebar" aria-label="Dashboard navigation">
        <div className="brand-lockup" aria-label="Gonvex dashboard">
          <span className="brand-mark">G</span>
          <span className="brand-name">Gonvex</span>
        </div>

        <nav className="sidebar-nav" aria-label="Primary sections">
          {pages.map((item) => (
            <Button
              key={item.id}
              className="nav-button"
              data-active={item.id === activePage ? "true" : undefined}
              onPress={() => navigatePage(item.id)}
              size="sm"
              variant={item.id === activePage ? "secondary" : "ghost"}
            >
              <span className="nav-dot" aria-hidden="true" />
              {item.label}
            </Button>
          ))}
        </nav>

        <div className="sidebar-status">
          <Chip color="accent" size="sm" variant="soft">
            local dev
          </Chip>
          <span>Vite harness</span>
        </div>
      </aside>

      <section className="workspace" aria-labelledby={activePage === "files" ? "file-storage-title" : "app-title"}>
        {activePage === "files" ? null : (
          <header className="topbar">
            <div className="crumbs" aria-label="Breadcrumb">
              <span>Dashboard</span>
              <span>/</span>
              <strong>{page.label}</strong>
            </div>

            <div className="topbar-actions">
              <Chip color="success" size="sm" variant="soft">
                realtime on
              </Chip>
              <Button size="sm" variant="secondary" onPress={() => {
                toggleTheme();
                reportAction(`Switched to ${theme === "dark" ? "light" : "dark"} mode`);
              }}>
                {themeLabel}
              </Button>
              <Button size="sm" variant="primary" onPress={() => reportAction("Dashboard test harness is mounted")}> 
                Ready for tests
              </Button>
            </div>
          </header>
        )}

        <div className={activePage === "files" ? "content-stack content-stack--flush" : "content-stack"}>
          {activePage === "files" ? null : (
            <section className="page-heading">
              <div>
                <p className="eyebrow">{page.eyebrow}</p>
                <h1 id="app-title">{page.title}</h1>
                <p className="lede">{page.description}</p>
              </div>
              <Chip color="accent" size="lg" variant="soft">
                {page.label}
              </Chip>
            </section>
          )}

          {activePage === "overview" ? <OverviewPage themeMode={theme} /> : null}
          {activePage === "functions" ? <FunctionsPage themeMode={theme} onAction={reportAction} /> : null}
          {activePage === "data" ? <DataPage themeMode={theme} onAction={reportAction} /> : null}
          {activePage === "test" ? <TestPage themeMode={theme} onAction={reportAction} /> : null}
          {activePage === "files" ? <FilesPage themeLabel={themeLabel} onToggleTheme={toggleTheme} onAction={reportAction} /> : null}
          {activePage === "realtime" ? <RealtimePage /> : null}
          {activePage === "settings" ? <SettingsPage /> : null}
        </div>
      </section>
      {actionMessage ? <div className="action-toast" role="status">{actionMessage}</div> : null}
    </main>
  );
}

function OverviewPage(props: { themeMode: ThemeMode }) {
  return (
    <div className="dashboard-layout">
      <section className="main-column">
        <section className="metric-row" aria-label="Function summary">
          {metrics.map(([label, value]) => (
            <Card key={label} className="metric-card" variant="default">
              <span>{label}</span>
              <strong>{value}</strong>
            </Card>
          ))}
        </section>

        <GridCard title="LiveGrid test surface" eyebrow="Function manifest" chip="Glide Data Grid">
          <ManifestGrid columns={functionColumns} rows={functionRows} height={330} themeMode={props.themeMode} />
        </GridCard>
      </section>

      <aside className="right-rail" aria-label="Runtime activity">
        <ListCard title="Runtime activity" rows={activity} />
        <ListCard title="Project target" rows={settings.slice(0, 3)} />
      </aside>
    </div>
  );
}

function FunctionsPage(props: { themeMode: ThemeMode; onAction: ActionHandler }) {
  const [search, setSearch] = useState("");
  const [selectedName, setSelectedName] = useState(functions[0].name);
  const selectedFunction = functions.find((item) => item.name === selectedName) ?? functions[0];
  const visibleFunctions = functions.filter((item) =>
    [item.name, item.kind, item.source].some((value) => value.toLowerCase().includes(search.toLowerCase())),
  );

  return (
    <div className="function-browser">
      <aside className="function-list-panel" aria-label="Functions">
        <div className="table-browser-heading">
          <span>Functions</span>
          <Chip color="default" size="sm" variant="secondary">
            {functions.length}
          </Chip>
        </div>
        <input
          className="table-search"
          onChange={(event) => setSearch(event.target.value)}
          placeholder="Search functions..."
          value={search}
          aria-label="Search functions"
        />
        <div className="function-list">
          {visibleFunctions.map((item) => (
            <Button
              key={item.name}
              className="function-button"
              data-active={item.name === selectedName ? "true" : undefined}
              onPress={() => setSelectedName(item.name)}
              variant={item.name === selectedName ? "secondary" : "ghost"}
            >
              <span className="function-kind" aria-hidden="true">
                {item.kind === "mutation" ? "fn" : "q"}
              </span>
              <span>{item.name}</span>
            </Button>
          ))}
        </div>
      </aside>

      <section className="function-detail-panel" aria-labelledby="function-detail-title">
        <header className="function-detail-header">
          <div>
            <div className="function-title-row">
              <h2 id="function-detail-title">{selectedFunction.name}</h2>
              <Chip color="warning" size="sm" variant="soft">
                {selectedFunction.kind}
              </Chip>
            </div>
            <p>{selectedFunction.source}</p>
          </div>
          <Button size="sm" variant="primary" onPress={() => props.onAction(`${selectedFunction.name} queued for local execution MVP`)}>
            Run Function
          </Button>
        </header>

        <div className="function-tabs" role="tablist" aria-label="Function detail tabs">
          <Button size="sm" variant="secondary">
            Statistics
          </Button>
          <Button size="sm" variant="ghost">
            Logs
          </Button>
        </div>

        <div className="function-stat-grid" aria-label="Function statistics">
          {functionStats.map(([label, value, tone]) => (
            <Card className="function-stat-card" key={label} variant="default">
              <Card.Header className="list-card-heading">
                <Card.Title>{label}</Card.Title>
                <strong>{value}</strong>
              </Card.Header>
              <Card.Content>
                <div className="mini-chart" data-tone={tone}>
                  <span />
                  <span />
                  <span />
                  <span />
                </div>
              </Card.Content>
            </Card>
          ))}
        </div>

        <GridCard title="Registered functions" eyebrow="Generated API" chip="manifest.json">
          <ManifestGrid columns={functionColumns} rows={functionRows} height={260} themeMode={props.themeMode} />
        </GridCard>
      </section>
    </div>
  );
}

function DataPage(props: { themeMode: ThemeMode; onAction: ActionHandler }) {
  const [tables, setTables] = useState<DataTableInfo[]>(fallbackTables);
  const [selectedTable, setSelectedTable] = useState(fallbackTables[0].name);
  const [rowCache, setRowCache] = useState<Record<number, Record<string, unknown>>>({});
  const [requestedOffset, setRequestedOffset] = useState(0);
  const [visibleOffset, setVisibleOffset] = useState(0);
  const [matchingRows, setMatchingRows] = useState(0);
  const [tableSearch, setTableSearch] = useState("");
  const [rowSearchInput, setRowSearchInput] = useState("");
  const [rowSearch, setRowSearch] = useState("");
  const [rowSort, setRowSort] = useState<SortState<string> | null>(null);
  const [filters, setFilters] = useState<DataFilter[]>([]);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const [filterOpen, setFilterOpen] = useState(false);
  const [addOpen, setAddOpen] = useState(false);
  const [addValues, setAddValues] = useState<Record<string, string>>({});
  const [addError, setAddError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [status, setStatus] = useState("Local schema fallback");
  const [runtimeAvailable, setRuntimeAvailable] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const activeTable = tables.find((table) => table.name === selectedTable) ?? tables[0];
  const filtersKey = JSON.stringify(filters.filter((filter) => activeTable.columns.includes(filter.column)));

  useEffect(() => {
    let cancelled = false;
    if (!runtimeBaseURL) {
      setStatus("Runtime offline, showing schema fallback");
      setRuntimeAvailable(false);
      return;
    }

    setStatus("Loading tables...");
    fetch(`${runtimeBaseURL}/dev/data/tables`)
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error(response.statusText))))
      .then((payload: { tables: DataTableInfo[] }) => {
        if (cancelled || payload.tables.length === 0) return;
        setTables(payload.tables);
        setStatus("Connected to Gonvex Runtime");
        setRuntimeAvailable(true);
        setSelectedTable((current) => (payload.tables.some((table) => table.name === current) ? current : payload.tables[0].name));
      })
      .catch(() => {
        if (!cancelled) {
          setStatus("Runtime offline, showing schema fallback");
          setRuntimeAvailable(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [refreshKey]);

  useEffect(() => {
    setRequestedOffset(0);
    setVisibleOffset(0);
    setRowCache({});
  }, [filtersKey, rowSearch, rowSort, selectedTable]);

  useEffect(() => {
    const timeout = window.setTimeout(() => setRowSearch(rowSearchInput), 300);
    return () => window.clearTimeout(timeout);
  }, [rowSearchInput]);

  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();
    if (!runtimeBaseURL) {
      const fallback = fallbackRows[selectedTable] ?? [];
      setRowCache(Object.fromEntries(fallback.map((row, index) => [index, row])));
      setMatchingRows(fallbackRows[selectedTable]?.length ?? 0);
      setRuntimeAvailable(false);
      return;
    }
    const params = new URLSearchParams({
      offset: String(requestedOffset),
      limit: String(pageSize),
    });
    if (rowSearch.trim()) params.set("search", rowSearch.trim());
    if (rowSort) {
      params.set("sort", rowSort.key);
      params.set("direction", rowSort.direction);
    }
    if (filtersKey !== "[]") params.set("filters", filtersKey);

    fetch(`${runtimeBaseURL}/dev/data/tables/${selectedTable}/rows?${params.toString()}`, { signal: controller.signal })
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error(response.statusText))))
      .then((payload: DataRowsResponse) => {
        if (cancelled) return;
        const offset = payload.offset ?? requestedOffset;
        setRowCache((current) => mergeRowsIntoCache(current, payload.rows, offset));
        setMatchingRows(payload.total ?? (rowSearch.trim() || filtersKey !== "[]" ? payload.rows.length : activeTable.rowCount));
        setStatus(payload.total === undefined ? "Connected to Gonvex Runtime (restart needed for server sort)" : "Connected to Gonvex Runtime");
        setRuntimeAvailable(true);
      })
      .catch(() => {
        if (controller.signal.aborted) return;
        if (!cancelled) {
          const fallback = fallbackRows[selectedTable] ?? [];
          setRowCache(Object.fromEntries(fallback.map((row, index) => [index, row])));
          setMatchingRows(fallbackRows[selectedTable]?.length ?? 0);
          setRuntimeAvailable(false);
        }
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [activeTable.rowCount, filtersKey, refreshKey, requestedOffset, rowSearch, rowSort, selectedTable]);

  useEffect(() => {
    setAddValues(Object.fromEntries(activeTable.columns.map((column) => [column, defaultValueForColumn(column)])));
    setAddError("");
  }, [selectedTable]);

  const visibleTables = tables.filter((table) => table.name.toLowerCase().includes(tableSearch.toLowerCase()));
  const dataColumns = columnsForDataTable(activeTable.columns).map((column) => ({
    ...column,
    width: columnWidths[String(column.id)] ?? ("width" in column ? column.width : 150),
    title: titleWithSort(column.title, rowSort, String(column.id)),
  }));
  const gridRowCount = runtimeAvailable ? matchingRows : Object.keys(rowCache).length;
  const dataCellGetter = createCachedCellGetter(activeTable.columns, rowCache);
  const openAddDocument = () => {
    if (!runtimeAvailable) {
      props.onAction("Start Gonvex Runtime with `pnpm dev:runtime` before inserting documents");
      return;
    }
    setAddValues(Object.fromEntries(activeTable.columns.map((column) => [column, defaultValueForColumn(column)])));
    setAddError("");
    setAddOpen(true);
  };
  const addFilter = () => {
    setFilters((current) => [
      ...current,
      { id: String(Date.now()), column: activeTable.columns[0] ?? "id", operator: "contains", value: "" },
    ]);
  };
  const insertDocument = async () => {
    if (!runtimeAvailable) {
      setAddError("Gonvex Runtime is offline. Start it with `pnpm dev:runtime`, then refresh this page.");
      return;
    }
    setSubmitting(true);
    setAddError("");
    try {
      const response = await fetch(`${runtimeBaseURL}/dev/data/tables/${selectedTable}/rows`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(addValues),
      });
      if (!response.ok) {
        const payload = await response.json().catch(() => ({ error: response.statusText }));
        throw new Error(payload.error ?? response.statusText);
      }
      const payload = (await response.json()) as InsertRowResponse;
      setRowCache((current) => mergeRowsIntoCache(current, [payload.row], 0));
      setTables((current) => current.map((table) => (
        table.name === activeTable.name ? { ...table, rowCount: table.rowCount + 1 } : table
      )));
      setMatchingRows((current) => current + 1);
      setAddOpen(false);
      props.onAction(`Inserted ${activeTable.name} row into the runtime database`);
    } catch (error) {
      const message = error instanceof TypeError
        ? "Cannot reach Gonvex Runtime at http://localhost:8080. Start it with `pnpm dev:runtime`."
        : error instanceof Error ? error.message : "Insert failed";
      setAddError(message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="data-browser">
      <aside className="table-browser" aria-label="Tables">
        <div className="table-browser-heading">
          <span>Tables</span>
          <Chip color="default" size="sm" variant="secondary">
            {tables.length}
          </Chip>
        </div>
        <input
          className="table-search"
          onChange={(event) => setTableSearch(event.target.value)}
          placeholder="Search tables..."
          value={tableSearch}
          aria-label="Search tables"
        />
        <div className="table-list">
          {visibleTables.map((table) => (
            <Button
              key={table.name}
              className="table-button"
              data-active={table.name === selectedTable ? "true" : undefined}
              onPress={() => setSelectedTable(table.name)}
              variant={table.name === selectedTable ? "secondary" : "ghost"}
            >
              <span>{table.name}</span>
              <small>{table.rowCount}</small>
            </Button>
          ))}
          {visibleTables.length === 0 ? <span className="empty-list-note">No matching tables</span> : null}
        </div>
        <Button className="schema-button" size="sm" variant="secondary" onPress={() => props.onAction(`${activeTable.name}: ${activeTable.columns.join(", ")}`)}>
          Schema
        </Button>
      </aside>

      <section className="data-table-panel" aria-labelledby="data-table-title">
        <header className="data-table-toolbar">
          <div>
            <p className="eyebrow">{status}</p>
            <h2 id="data-table-title">{activeTable.name}</h2>
            <span className="row-count">{activeTable.rowCount} rows · {activeTable.columns.length} columns</span>
          </div>
          <div className="topbar-actions">
            <Button size="sm" variant="secondary" onPress={() => {
              setRowCache({});
              setRequestedOffset(0);
              setVisibleOffset(0);
              setRefreshKey((key) => key + 1);
              props.onAction("Refreshing runtime table data");
            }}>
              Refresh
            </Button>
            <Button size="sm" variant="secondary" onPress={() => {
              setFilterOpen((open) => !open);
              props.onAction(filterOpen ? "Closed filters" : "Opened filters");
            }}>
              Filter & Sort{filters.length > 0 ? ` (${filters.length})` : ""}
            </Button>
            <Button size="sm" variant="primary" onPress={openAddDocument} isDisabled={!runtimeAvailable}>
              + Add Document
            </Button>
          </div>
        </header>

        <div className="data-controls">
            <input
              className="table-search"
              onChange={(event) => setRowSearchInput(event.target.value)}
              placeholder="Search rows by substring..."
              value={rowSearchInput}
              aria-label="Search rows"
            />
          <span>{matchingRows} matching rows</span>
        </div>

        {filterOpen ? (
          <div className="filter-panel" role="region" aria-label="Filter and sort">
            <div className="filter-toolbar">
              <span>{rowSort ? `Sorting by ${rowSort.key} ${rowSort.direction}` : "Click a grid header to sort, or configure filters below."}</span>
              <Button size="sm" variant="secondary" onPress={addFilter}>+ Add Filter</Button>
            </div>
            <div className="sort-controls">
              <label>
                Sort column
                <select value={rowSort?.key ?? ""} onChange={(event) => {
                  setRowSort(event.target.value ? { key: event.target.value, direction: rowSort?.direction ?? "asc" } : null);
                }}>
                  <option value="">None</option>
                  {activeTable.columns.map((column) => <option key={column} value={column}>{column}</option>)}
                </select>
              </label>
              <label>
                Direction
                <select
                  value={rowSort?.direction ?? "asc"}
                  onChange={(event) => {
                    if (rowSort) setRowSort({ key: rowSort.key, direction: event.target.value as SortDirection });
                  }}
                  disabled={!rowSort}
                >
                  <option value="asc">Ascending</option>
                  <option value="desc">Descending</option>
                </select>
              </label>
            </div>
            {filters.length === 0 ? <p>No filters applied.</p> : null}
            {filters.map((filter) => (
              <div className="filter-row" key={filter.id}>
                <select value={filter.column} onChange={(event) => {
                  setFilters((current) => current.map((item) => item.id === filter.id ? { ...item, column: event.target.value } : item));
                }}>
                  {activeTable.columns.map((column) => <option key={column} value={column}>{column}</option>)}
                </select>
                <select value={filter.operator} onChange={(event) => {
                  setFilters((current) => current.map((item) => item.id === filter.id ? { ...item, operator: event.target.value as DataFilter["operator"] } : item));
                }}>
                  <option value="contains">contains</option>
                  <option value="equals">equals</option>
                  <option value="notEquals">does not equal</option>
                  <option value="startsWith">starts with</option>
                  <option value="endsWith">ends with</option>
                  <option value="empty">is empty</option>
                  <option value="notEmpty">is not empty</option>
                </select>
                <input
                  value={filter.value}
                  onChange={(event) => {
                    setFilters((current) => current.map((item) => item.id === filter.id ? { ...item, value: event.target.value } : item));
                  }}
                  placeholder="Value"
                  disabled={filter.operator === "empty" || filter.operator === "notEmpty"}
                  aria-label={`Filter ${filter.column} value`}
                />
                <Button size="sm" variant="ghost" onPress={() => setFilters((current) => current.filter((item) => item.id !== filter.id))}>Remove</Button>
              </div>
            ))}
          </div>
        ) : null}

        <div className="data-grid-wrap">
          <ManifestGrid
            columns={dataColumns}
            getCellContent={dataCellGetter}
            height={520}
            rowCount={gridRowCount}
            themeMode={props.themeMode}
            onHeaderClick={(columnIndex) => {
              const column = activeTable.columns[columnIndex];
              if (column) {
                setRowCache({});
                setRequestedOffset(0);
                setRowSort((current) => nextSort(current, column));
                props.onAction(`Sorting ${activeTable.name} by ${column}`);
              }
            }}
            onColumnResize={(column, newSize) => {
              setColumnWidths((current) => ({ ...current, [String(column.id)]: newSize }));
            }}
            onVisibleRegionChanged={(range) => {
              const nextOffset = offsetForVisibleRow(Math.floor(range.y));
              const hasVisibleRows = Array.from({ length: Math.ceil(range.height) }, (_, index) => rowCache[Math.floor(range.y) + index])
                .every(Boolean);
              if (!hasVisibleRows && nextOffset !== visibleOffset) {
                setVisibleOffset(nextOffset);
                setRequestedOffset(nextOffset);
              }
            }}
          />
          {gridRowCount === 0 ? (
            <div className="data-empty-state" role="status">
              <div className="empty-icon">▦</div>
              <strong>{runtimeAvailable ? "This table is empty." : "Runtime is offline."}</strong>
              <span>
                {runtimeAvailable
                  ? "Create a document or run a mutation to start storing data."
                  : "Start Gonvex Runtime with `pnpm dev:runtime` to read and insert real database rows."}
              </span>
              <Button size="sm" variant="primary" onPress={openAddDocument} isDisabled={!runtimeAvailable}>
                {runtimeAvailable ? "+ Add Document" : "Runtime Required"}
              </Button>
            </div>
          ) : null}
        </div>
      </section>
      {addOpen ? (
        <div className="modal-backdrop" role="presentation" onMouseDown={() => setAddOpen(false)}>
          <section className="document-modal" role="dialog" aria-modal="true" aria-labelledby="add-document-title" onMouseDown={(event) => event.stopPropagation()}>
            <header>
              <div>
                <p className="eyebrow">Insert into {activeTable.name}</p>
                <h2 id="add-document-title">Add Document</h2>
              </div>
              <Button size="sm" variant="ghost" onPress={() => setAddOpen(false)}>Close</Button>
            </header>
            <div className="document-form">
              {activeTable.columns.map((column) => (
                <label key={column}>
                  <span>{column}{column === "id" ? " (auto)" : ""}</span>
                  <input
                    value={addValues[column] ?? ""}
                    onChange={(event) => setAddValues((current) => ({ ...current, [column]: event.target.value }))}
                    placeholder={column === "id" ? "Leave blank to auto-increment" : column.endsWith("_at") ? "ISO timestamp" : `Value for ${column}`}
                  />
                </label>
              ))}
            </div>
            {!runtimeAvailable ? (
              <div className="form-warning" role="status">
                Runtime is offline. This form inserts into the real database only when `pnpm dev:runtime` is running.
              </div>
            ) : null}
            {addError ? <div className="form-error" role="alert">{addError}</div> : null}
            <footer>
              <Button size="sm" variant="secondary" onPress={() => setAddOpen(false)}>Cancel</Button>
              <Button size="sm" variant="primary" onPress={insertDocument} isDisabled={submitting}>
                {submitting ? "Inserting..." : "Insert Document"}
              </Button>
            </footer>
          </section>
        </div>
      ) : null}
    </div>
  );
}

const testTaskColumns: GridColumn[] = [
  { title: "ID", id: "pg_id", width: 72 },
  { title: "Task", id: "name", width: 420 },
  { title: "Status", id: "status", width: 180 },
  { title: "Priority", id: "priority", width: 140 },
  { title: "Owner", id: "assignee", width: 150 },
  { title: "Dates", id: "due_date", width: 180 },
  { title: "Location", id: "spot", width: 150 },
  { title: "Progress", id: "progress", width: 130 },
  { title: "Flag", id: "flag_color", width: 76 },
];

const testTaskDataColumns = [
  "id",
  "pg_id",
  "name",
  "title",
  "description",
  "status",
  "priority",
  "assignee",
  "due_date",
  "due_at",
  "start_date",
  "spot_id",
  "progress",
  "flag_color",
];

const testSortColumns: Record<string, string> = {
  spot: "spot_id",
};

function testTaskGridArgs(offset: number, search: string, sort: TestSortState): TestTaskGridArgs {
  const trimmedSearch = search.trim();
  return {
    offset,
    limit: 300,
    columns: testTaskDataColumns,
    count: trimmedSearch ? "false" : "estimate",
    ...(sort.direction !== "default" ? { sort: testSortColumns[sort.key] ?? sort.key, direction: sort.direction } : {}),
    ...(trimmedSearch ? { search: trimmedSearch } : {}),
  };
}

function testTaskGridStateKey(search: string, sort: TestSortState): string {
  return JSON.stringify({ search: search.trim(), sort });
}

const statusColors: Record<string, string> = {
  new: "#dbeafe",
  in_progress: "#ccfbf1",
  waiting: "#fef3c7",
  review: "#ede9fe",
  done: "#dcfce7",
};

const priorityColors: Record<string, string> = {
  low: "#dbeafe",
  medium: "#fef3c7",
  high: "#fed7aa",
  urgent: "#fecaca",
};

const flagColors: Record<string, string> = {
  red: "#ef4444",
  orange: "#f97316",
  yellow: "#f59e0b",
  green: "#22c55e",
  blue: "#3b82f6",
  purple: "#8b5cf6",
};

function prettify(value: unknown): string {
  return formatCellValue(value).replace(/_/g, " ").replace(/\b\w/g, (char) => char.toUpperCase());
}

function formatShortDate(value: unknown): string {
  const raw = formatCellValue(value);
  if (!raw) return "No date";
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return raw;
  return date.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function createWhagonsTaskCellGetter(rowCache: Record<number, Record<string, unknown>>) {
  return ([column, rowIndex]: Item): GridCell => {
    const row = rowCache[rowIndex];
    const columnId = String(testTaskColumns[column]?.id ?? "");
    if (!row) return whTaskCell({ kind: "taskName", primary: "Loading...", muted: true });
    switch (columnId) {
      case "pg_id":
        return { kind: GridCellKind.Text, allowOverlay: false, displayData: `#${formatCellValue(row.pg_id || row.id)}`, data: `#${formatCellValue(row.pg_id || row.id)}` };
      case "name":
        return whTaskCell({ kind: "taskName", primary: formatCellValue(row.name || row.title), secondary: formatCellValue(row.description) });
      case "status": {
        const value = formatCellValue(row.status);
        return whTaskCell({ kind: "statusPill", primary: prettify(value), color: statusColors[value] ?? "#e5e7eb" });
      }
      case "priority": {
        const value = formatCellValue(row.priority);
        return whTaskCell({ kind: "priorityPill", primary: prettify(value), color: priorityColors[value] ?? "#e5e7eb" });
      }
      case "due_date":
        return whTaskCell({ kind: "dateStack", primary: formatShortDate(row.due_date || row.due_at), secondary: `Started ${formatShortDate(row.start_date)}` });
      case "progress":
        return whTaskCell({ kind: "progressBar", primary: formatCellValue(row.progress), progress: Number(row.progress ?? 0), color: "#22c55e" });
      case "flag_color": {
        const color = formatCellValue(row.flag_color);
        return whTaskCell({ kind: "flag", primary: color || "none", color: flagColors[color] });
      }
      case "spot":
        return { kind: GridCellKind.Text, allowOverlay: false, displayData: formatCellValue(row.spot_id), data: formatCellValue(row.spot_id) };
      default:
        return { kind: GridCellKind.Text, allowOverlay: false, displayData: formatCellValue(row[columnId]), data: formatCellValue(row[columnId]) };
    }
  };
}

function TestPage(props: { themeMode: ThemeMode; onAction: ActionHandler }) {
  const [rowCache, setRowCache] = useState<Record<number, Record<string, unknown>>>({});
  const [requestedOffset, setRequestedOffset] = useState(0);
  const [visibleOffset, setVisibleOffset] = useState(0);
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const [sort, setSort] = useState<TestSortState>({ key: "name", direction: "default" });
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const [total, setTotal] = useState(0);
  const [status, setStatus] = useState("Loading Whagons-style task rows...");
  const [randomizing, setRandomizing] = useState(false);
  const sortKeyRef = useRef(`${sort.key}:${sort.direction}`);
  const mountedRef = useRef(true);
  const gridStateKey = testTaskGridStateKey(search, sort);
  const gridStateKeyRef = useRef(gridStateKey);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  useEffect(() => {
    gridStateKeyRef.current = gridStateKey;
  }, [gridStateKey]);

  useEffect(() => {
    const timeout = window.setTimeout(() => setSearch(searchInput), 300);
    return () => window.clearTimeout(timeout);
  }, [searchInput]);

  useEffect(() => {
    setRequestedOffset(0);
    setVisibleOffset(0);
    setRowCache({});
  }, [search]);

  useEffect(() => {
    const nextSortKey = `${sort.key}:${sort.direction}`;
    if (sortKeyRef.current === nextSortKey) return;
    sortKeyRef.current = nextSortKey;
    setRowCache({});
    setRequestedOffset(visibleOffset);
  }, [sort, visibleOffset]);

  useEffect(() => {
    if (!gonvexClient) return;
    const args = testTaskGridArgs(requestedOffset, search, sort);
    const stateKey = gridStateKey;
    return gonvexClient.subscribeQuery(api["tasks.grid"], args as unknown as JsonValue, (message) => {
      if (!mountedRef.current || gridStateKeyRef.current !== stateKey) return;
      if (message.type === "query.error") {
        setStatus(message.error);
        return;
      }
      if (message.type === "query.result") {
        const payload = message.result as DataRowsResponse;
        const offset = payload.offset ?? requestedOffset;
        setRowCache((current) => replaceRowsInCache(current, payload.rows, offset, payload.limit ?? pageSize));
        setTotal(payload.total ?? payload.rows.length);
        setStatus("Live via Gonvex binding");
      }
    });
  }, [gridStateKey, requestedOffset, search, sort]);

  const randomizeVisibleTaskFields = async () => {
    if (!gonvexClient || randomizing) return;
    setRandomizing(true);
    setStatus("Randomizing 3k task statuses/priorities...");
    try {
      const result = await gonvexClient.mutation<RandomizeTasksResponse>(
        api["tasks.randomizeStatusPriority"],
        { count: 3000 } as unknown as JsonValue,
      );
      setStatus(`Randomized ${result.updated.toLocaleString()} tasks in ${result.durationMs.toLocaleString()}ms`);
      props.onAction(`Randomized ${result.updated.toLocaleString()} task rows`);
    } catch (error) {
      setStatus(error instanceof Error ? error.message : "Randomize mutation failed");
    } finally {
      setRandomizing(false);
    }
  };

  return (
    <section className="test-table-shell">
      <header className="test-table-toolbar">
        <div>
          <p className="eyebrow">{status}</p>
          <h2>Task rendering lab</h2>
          <span>{total} matching rows · Glide custom renderers · Whagons-like fields</span>
        </div>
        <div className="test-table-actions">
          <Button size="sm" variant="secondary" onPress={randomizeVisibleTaskFields} isDisabled={randomizing}>
            {randomizing ? "Randomizing..." : "Randomize 3k"}
          </Button>
          <input
            className="table-search test-table-search"
            onChange={(event) => setSearchInput(event.target.value)}
            placeholder="Search task name/description..."
            value={searchInput}
            aria-label="Search test tasks"
          />
        </div>
      </header>
      <ManifestGrid
        columns={testTaskColumns.map((column) => ({
          ...column,
          title: titleWithTestSort(column.title, sort, String(column.id)),
          width: columnWidths[String(column.id)] ?? ("width" in column ? column.width : 150),
        }))}
        customRenderers={[whTaskRenderer]}
        clearSelection={sort.direction === "default"}
        getCellContent={createWhagonsTaskCellGetter(rowCache)}
        height={620}
        rowCount={total}
        rowHeight={64}
        themeMode={props.themeMode}
        onHeaderClick={(columnIndex) => {
          const column = String(testTaskColumns[columnIndex]?.id ?? "");
          if (!column) return;
          setSort((current) => nextTestSort(current, column));
          props.onAction(`Test table sorting by ${column}`);
        }}
        onColumnResize={(column, newSize) => {
          setColumnWidths((current) => ({ ...current, [String(column.id)]: newSize }));
        }}
        onVisibleRegionChanged={(range) => {
          const nextOffset = offsetForVisibleRow(Math.floor(range.y));
          if (nextOffset !== visibleOffset) {
            setVisibleOffset(nextOffset);
            setRequestedOffset(nextOffset);
          }
        }}
      />
    </section>
  );
}

function FilesPage(props: { themeLabel: string; onToggleTheme: () => void; onAction: ActionHandler }) {
  const [search, setSearch] = useState("");
  const [runtimeFiles, setRuntimeFiles] = useState<FileInfo[]>([]);
  const [localFiles, setLocalFiles] = useState<FileInfo[]>([]);
  const [status, setStatus] = useState("Loading runtime files...");
  const [fileSort, setFileSort] = useState<SortState<FileSortKey> | null>(null);
  const files = [...localFiles, ...runtimeFiles];
  const visibleFiles = sortFiles(files.filter((file) =>
    [file.id, file.contentType].some((value) => value.toLowerCase().includes(search.toLowerCase())),
  ), fileSort);

  useEffect(() => {
    let cancelled = false;
    if (!runtimeBaseURL) {
      setRuntimeFiles([]);
      setStatus("Runtime offline. Select files to preview local uploads.");
      return;
    }

    fetch(`${runtimeBaseURL}/dev/data/tables/files/rows?limit=100`)
      .then((response) => (response.ok ? response.json() : Promise.reject(new Error(response.statusText))))
      .then((payload: DataRowsResponse) => {
        if (cancelled) return;
        setRuntimeFiles(payload.rows.map(fileFromRuntimeRow));
        setStatus("Connected to Gonvex Runtime");
      })
      .catch(() => {
        if (!cancelled) {
          setRuntimeFiles([]);
          setStatus("Runtime offline. Select files to preview local uploads.");
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const uploadLocalFiles = (fileList: FileList | null) => {
    const selected = Array.from(fileList ?? []);
    if (selected.length === 0) return;
    const uploadedAt = new Date().toLocaleString();
    const previews = selected.map((file) => ({
      id: `local:${file.name}`,
      size: formatBytes(file.size),
      contentType: file.type || "application/octet-stream",
      uploadedAt,
      objectUrl: URL.createObjectURL(file),
      source: "local" as const,
    }));
    setLocalFiles((current) => [...previews, ...current]);
    props.onAction(`Added ${selected.length} local file preview${selected.length === 1 ? "" : "s"}`);
  };

  const downloadFile = (file: FileInfo) => {
    if (!file.objectUrl) {
      props.onAction("Runtime download URLs are not implemented yet");
      return;
    }
    const link = document.createElement("a");
    link.href = file.objectUrl;
    link.download = file.id.replace(/^local:/, "");
    link.click();
  };

  const deleteFile = (file: FileInfo) => {
    if (file.objectUrl) URL.revokeObjectURL(file.objectUrl);
    if (file.source === "local") {
      setLocalFiles((current) => current.filter((item) => item.id !== file.id));
      props.onAction(`Removed ${file.id} from local previews`);
      return;
    }
    props.onAction("Runtime file deletion is not implemented yet");
  };

  const renderHeader = (key: FileSortKey, label: string) => (
    <button className="file-sort-button" onClick={() => setFileSort((current) => nextSort(current, key))}>
      {label}
      <span>{fileSort?.key === key ? (fileSort.direction === "asc" ? "↑" : "↓") : ""}</span>
    </button>
  );

  return (
    <section
      className="file-storage-page"
      aria-labelledby="file-storage-title"
      onDragOver={(event) => event.preventDefault()}
      onDrop={(event) => {
        event.preventDefault();
        uploadLocalFiles(event.dataTransfer.files);
      }}
    >
      <header className="file-storage-header">
        <div>
          <div className="file-storage-title-row">
            <h1 id="file-storage-title">File Storage</h1>
            <select className="project-select" aria-label="Project">
              <option>app</option>
            </select>
          </div>
          <p>Total Files {files.length}</p>
          <p>{status}</p>
        </div>
        <Button className="file-theme-toggle" size="sm" variant="secondary" onPress={() => {
          props.onToggleTheme();
          props.onAction(`Switched to ${props.themeLabel.toLowerCase().replace(" mode", "")}`);
        }}>
          {props.themeLabel}
        </Button>
      </header>

      <div className="file-storage-toolbar">
        <div className="file-filter-group">
          <label className="file-search">
            <span aria-hidden="true">ID</span>
            <input
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Lookup by ID"
              value={search}
              aria-label="Lookup by ID"
            />
          </label>
          <Button
            className="uploaded-filter"
            size="sm"
            variant="secondary"
            onPress={() => setFileSort((current) => nextSort(current, "uploadedAt"))}
          >
            Uploaded at: <span>Any time</span>
          </Button>
        </div>

        <div className="upload-actions">
          <label className="upload-label">
            <input type="file" multiple onChange={(event) => uploadLocalFiles(event.target.files)} />
            Upload Files
          </label>
          <span>or drag files here</span>
        </div>
      </div>

      <div className="file-table-shell" role="table" aria-label="Stored files">
        <div className="file-grid-row file-grid-head" role="row">
          <div className="check-cell" role="columnheader"><span aria-hidden="true" /></div>
          <div role="columnheader">{renderHeader("id", "ID")} <span className="help-dot">?</span></div>
          <div role="columnheader">{renderHeader("size", "Size")}</div>
          <div role="columnheader">{renderHeader("contentType", "Content type")}</div>
          <div role="columnheader">{renderHeader("uploadedAt", "Uploaded at")}</div>
          <div className="action-cell" role="columnheader"><Button size="sm" variant="secondary" aria-label="Column settings" onPress={() => props.onAction("All file columns are visible")}>Cols</Button></div>
        </div>
        {visibleFiles.map((file) => (
          <div className="file-grid-row" role="row" key={file.id}>
            <div className="check-cell" role="cell"><span aria-hidden="true" /></div>
            <div className="file-id-cell" role="cell"><code>{file.id}</code><button aria-label={`Copy ${file.id}`} onClick={() => {
              void navigator.clipboard?.writeText(file.id);
              props.onAction(`Copied ${file.id}`);
            }}>Copy</button></div>
            <div role="cell">{file.size}</div>
            <div role="cell">{file.contentType}</div>
            <div role="cell">{file.uploadedAt}</div>
            <div className="file-actions" role="cell">
              <button aria-label={`Download ${file.id}`} onClick={() => downloadFile(file)}>Download</button>
              <button aria-label={`Delete ${file.id}`} onClick={() => deleteFile(file)}>Delete</button>
            </div>
          </div>
        ))}
        {visibleFiles.length === 0 ? <div className="file-empty-state">No files yet. Upload a file locally or start the runtime with rows in the files table.</div> : null}
      </div>
    </section>
  );
}

function RealtimePage() {
  return (
    <div className="dashboard-layout dashboard-layout--wide">
      <Card className="timeline-card" variant="default">
        <Card.Header className="panel-heading">
          <div>
            <p className="eyebrow">Dependency graph</p>
            <Card.Title>Realtime pipeline</Card.Title>
          </div>
          <Chip color="warning" size="sm" variant="soft">
            next MVP
          </Chip>
        </Card.Header>
        <Separator />
        <Card.Content className="timeline-content">
          {[
            "query subscribes from React",
            "runtime records rows, predicates, and windows",
            "mutation commits a write set",
            "affected subscriptions rerun",
            "Glide receives row patches",
          ].map((item, index) => (
            <div className="timeline-step" key={item}>
              <span>{String(index + 1).padStart(2, "0")}</span>
              <strong>{item}</strong>
            </div>
          ))}
        </Card.Content>
      </Card>
      <aside className="right-rail" aria-label="Realtime counters">
        <ListCard
          title="Counters"
          rows={[["subscriptions", "0 active"], ["invalidations", "planned"], ["patch stream", "planned"]]}
        />
      </aside>
    </div>
  );
}

function SettingsPage() {
  const [activeSection, setActiveSection] = useState<"general" | "environment" | "authentication">("general");

  return (
    <div className="settings-shell">
      <aside className="settings-nav" aria-label="Settings sections">
        <Button
          data-active={activeSection === "general" ? "true" : undefined}
          onPress={() => setActiveSection("general")}
          variant={activeSection === "general" ? "secondary" : "ghost"}
        >
          General
        </Button>
        <Button
          data-active={activeSection === "environment" ? "true" : undefined}
          onPress={() => setActiveSection("environment")}
          variant={activeSection === "environment" ? "secondary" : "ghost"}
        >
          Environment Variables
        </Button>
        <Button
          data-active={activeSection === "authentication" ? "true" : undefined}
          onPress={() => setActiveSection("authentication")}
          variant={activeSection === "authentication" ? "secondary" : "ghost"}
        >
          Authentication
        </Button>
      </aside>

      <section className="settings-panel" aria-live="polite">
        {activeSection === "general" ? (
          <SettingsCard title="General" description="Local deployment details used by gonvex dev and the runtime.">
            <div className="settings-grid">
              <SettingField label="Project" value="app" />
              <SettingField label="Runtime URL" value="http://localhost:8080" />
              <SettingField label="Dev script" value="gonvex dev -- vite" />
              <SettingField label="Database" value="gonvex_dev" />
            </div>
          </SettingsCard>
        ) : null}

        {activeSection === "environment" ? (
          <SettingsCard title="Environment Variables" description="Variables loaded by the local Gonvex Runtime from .env.">
            <div className="env-table" role="table" aria-label="Environment variables">
              {["DATABASE_URL", "S3_ENDPOINT", "S3_BUCKET", "S3_FORCE_PATH_STYLE"].map((name) => (
                <div className="env-row" role="row" key={name}>
                  <code role="cell">{name}</code>
                  <span role="cell">Configured locally</span>
                </div>
              ))}
            </div>
          </SettingsCard>
        ) : null}

        {activeSection === "authentication" ? (
          <SettingsCard
            title="Authentication Configuration"
            description="These are the authentication providers configured for this deployment."
          >
            <div className="auth-provider-grid">
              <SettingField label="Domain" value="https://securetoken.google.com/gonvex-dev" />
              <SettingField label="Application ID" value="gonvex-dev" />
              <SettingField label="Type" value="OIDC provider" muted />
            </div>
            <Separator />
            <div className="auth-provider-grid auth-provider-grid--wide">
              <SettingField label="Issuer" value="https://gonvex.local/dev-auth" />
              <SettingField label="JWKS URL" value="data:application/json;base64,eyJrZXlzIjpbXX0" />
              <SettingField label="Algorithm" value="RS256" />
              <SettingField label="Application ID" value="gonvex-dev-local" />
              <SettingField label="Type" value="Custom JWT provider" muted />
            </div>
            <p className="settings-note">Deploy keys will live in General Deployment Settings later.</p>
          </SettingsCard>
        ) : null}
      </section>
    </div>
  );
}

function SettingsCard(props: { title: string; description: string; children: ReactNode }) {
  return (
    <Card className="settings-card" variant="default">
      <Card.Header className="settings-card-header">
        <Card.Title>{props.title}</Card.Title>
        <p>{props.description}</p>
      </Card.Header>
      <Card.Content className="settings-card-content">{props.children}</Card.Content>
    </Card>
  );
}

function SettingField(props: { label: string; value: string; muted?: boolean }) {
  return (
    <label className="setting-field">
      <span>{props.label}</span>
      <output data-muted={props.muted ? "true" : undefined}>{props.value}</output>
    </label>
  );
}

function GridCard(props: { title: string; eyebrow: string; chip: string; children: ReactNode }) {
  return (
    <Card className="grid-panel" variant="default" aria-labelledby="grid-title">
      <Card.Header className="panel-heading">
        <div>
          <p className="eyebrow">{props.eyebrow}</p>
          <Card.Title id="grid-title">{props.title}</Card.Title>
        </div>
        <Chip color="default" size="sm" variant="secondary">
          {props.chip}
        </Chip>
      </Card.Header>
      <Separator />
      <Card.Content className="grid-content">{props.children}</Card.Content>
    </Card>
  );
}

function ListCard(props: { title: string; rows: string[][]; large?: boolean }) {
  return (
    <Card className={props.large ? "list-card list-card--large" : "list-card"} variant="default">
      <Card.Header className="list-card-heading">
        <Card.Title>{props.title}</Card.Title>
      </Card.Header>
      <Card.Content>
        {props.rows.map(([label, value]) => (
          <div className="kv-row" key={label}>
            <span>{label}</span>
            <strong>{value}</strong>
          </div>
        ))}
      </Card.Content>
    </Card>
  );
}
