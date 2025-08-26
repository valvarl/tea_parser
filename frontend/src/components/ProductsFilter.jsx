// components/ProductsFilter.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import PropTypes from "prop-types";

import { GiBroom } from "react-icons/gi";
import {
  RxChevronDown,
  RxTriangleRight,
  RxTriangleDown,
} from "react-icons/rx";
import {
  FiClock,
  FiCalendar,
  FiType,
  FiDollarSign,
  FiStar,
  FiSearch,
} from "react-icons/fi";

/* ——— скрываемые характеристики ——— */
const HIDDEN_CHAR_IDS = new Set([
  "Sku",
  "Brand",
  "ServiceMaxTemperature",
  "ServiceMinTemperature",
  "ShelfLife",
]);

/* ——— сортировка ——— */
const SORT_OPTIONS = [
  { label: "Updated ↓", sort_by: "updated_at", sort_dir: "desc", key: "updated_desc" },
  { label: "Updated ↑", sort_by: "updated_at", sort_dir: "asc",  key: "updated_asc"  },
  { label: "Created ↓", sort_by: "created_at", sort_dir: "desc", key: "created_desc" },
  { label: "Created ↑", sort_by: "created_at", sort_dir: "asc",  key: "created_asc"  },
  { label: "Title A→Z", sort_by: "title",      sort_dir: "asc",  key: "title_asc"    },
  { label: "Title Z→A", sort_by: "title",      sort_dir: "desc", key: "title_desc"   },
  { label: "Price ↑",   sort_by: "price",      sort_dir: "asc",  key: "price_asc"    },
  { label: "Price ↓",   sort_by: "price",      sort_dir: "desc", key: "price_desc"   },
  { label: "Rating ↓",  sort_by: "rating",     sort_dir: "desc", key: "rating_desc"  },
  { label: "Rating ↑",  sort_by: "rating",     sort_dir: "asc",  key: "rating_asc"   },
];

const SORT_ICONS = {
  updated_at: <FiClock className="w-4 h-4" />,
  created_at: <FiCalendar className="w-4 h-4" />,
  title: <FiType className="w-4 h-4" />,
  price: <FiDollarSign className="w-4 h-4" />,
  rating: <FiStar className="w-4 h-4" />,
};

function keyForSort(sort_by, sort_dir) {
  const f = SORT_OPTIONS.find((s) => s.sort_by === sort_by && s.sort_dir === sort_dir);
  return f?.key || "updated_desc";
}

function allNumeric(vals) {
  if (!Array.isArray(vals) || !vals.length) return false;
  return vals.every((v) => Number.isFinite(Number(String(v).replace(",", "."))));
}

function sortValues(vals) {
  const arr = (vals || []).filter(Boolean).map(String);
  if (allNumeric(arr)) {
    return [...arr].sort((a, b) => parseFloat(a.replace(",", ".")) - parseFloat(b.replace(",", ".")));
  }
  return [...arr].sort((a, b) => a.localeCompare(b, "ru", { numeric: true, sensitivity: "base" }));
}

export default function ProductsFilter({
  mode,                 // "all" | "byTask"
  taskId,
  scope,                // "task" | "pipeline"
  onScopeChange,
  onClearTask,          // сброс «task»-контекста
  characteristics,
  value,                // { q, sort_by, sort_dir, filters }
  onChange,
  onReset,
  loading = false,
  disabled = false,
  loadingFacets = false,
}) {
  const [searchLocal, setSearchLocal] = useState(value?.q || "");
  const [isComposing, setIsComposing] = useState(false);

  const [filtersOpen, setFiltersOpen] = useState(false);
  const [openGroups, setOpenGroups] = useState({});

  const INITIAL_SHOW = 8;
  const SHOW_STEP = 12;
  const [showCountByChar, setShowCountByChar] = useState({});

  useEffect(() => setSearchLocal(value?.q || ""), [value?.q]);

  // при смене набора характеристик — мягко пересчитываем лимиты
  useEffect(() => {
    const next = {};
    for (const c of characteristics || []) next[c.id] = showCountByChar[c.id] || INITIAL_SHOW;
    setShowCountByChar(next);
  }, [characteristics]);

  const commitSearch = () => {
    if ((value?.q || "") !== (searchLocal || "")) onChange?.({ ...value, q: searchLocal || "" });
  };

  const currentSortKey = useMemo(
    () => keyForSort(value?.sort_by || "updated_at", value?.sort_dir || "desc"),
    [value?.sort_by, value?.sort_dir]
  );

  const chips = useMemo(() => {
    const out = [];
    if (value?.q) out.push({ type: "q", id: "_q", label: `Search: “${value.q}”` });
    const f = value?.filters || {};
    Object.entries(f).forEach(([cid, vals]) => (vals || []).forEach((v) => {
      out.push({ type: "char", id: `${cid}::${v}`, label: `${cid}: ${v}`, cid, v });
    }));
    return out;
  }, [value]);

  const visibleCharacteristics = useMemo(() => {
    return (characteristics || [])
      .filter((c) => c && !HIDDEN_CHAR_IDS.has(c.id))
      .map((c) => ({ ...c, values: sortValues(c.values) }))
      .filter((c) => c.values?.length)
      .sort((a, b) => (a.title || a.id).localeCompare(b.title || b.id, "ru", { sensitivity: "base" }));
  }, [characteristics]);

  const toggleGroup = (cid) => setOpenGroups((p) => ({ ...p, [cid]: !p[cid] }));

  const updateSort = (key) => {
    const s = SORT_OPTIONS.find((x) => x.key === key) || SORT_OPTIONS[0];
    onChange?.({ ...value, sort_by: s.sort_by, sort_dir: s.sort_dir });
  };

  const toggleFilterValue = (cid, val) => {
    const filters = { ...(value?.filters || {}) };
    const set = new Set(filters[cid] || []);
    set.has(val) ? set.delete(val) : set.add(val);
    const next = Array.from(set);
    if (next.length) filters[cid] = next;
    else delete filters[cid];
    onChange?.({ ...value, filters });
  };

  const clearChip = (chip) => {
    if (chip.type === "q") {
      setSearchLocal("");
      onChange?.({ ...value, q: "" });
      return;
    }
    toggleFilterValue(chip.cid, chip.v);
  };

  const clearAll = () => {
    onReset?.();
    setSearchLocal("");
  };

  const isDisabled = disabled || loading;
  const activeFiltersCount = Object.values(value?.filters || {}).reduce((s, arr) => s + (arr?.length || 0), 0);

  /* ——— выпадашка сортировки: автозакрытие по клику вне / blur / Esc ——— */
  const sortRef = useRef(null);
  const [sortOpen, setSortOpen] = useState(false);
  const sortCurrent = SORT_OPTIONS.find((s) => s.key === currentSortKey) || SORT_OPTIONS[0];

  useEffect(() => {
    const onDocClick = (e) => {
      if (!sortRef.current) return;
      if (!sortRef.current.contains(e.target)) setSortOpen(false);
    };
    const onKey = (e) => { if (e.key === "Escape") setSortOpen(false); };
    document.addEventListener("mousedown", onDocClick);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDocClick);
      document.removeEventListener("keydown", onKey);
    };
  }, []);

  const SortTrigger = (
    <button
      type="button"
      onClick={() => setSortOpen((v) => !v)}
      className="form-input input-md control-md w-full pl-2 pr-9 flex items-center justify-between"
      aria-haspopup="listbox"
      aria-expanded={sortOpen}
      onBlur={(e) => {
        const next = e.relatedTarget;
        if (sortRef.current && next && sortRef.current.contains(next)) return;
        setSortOpen(false);
      }}
    >
      <span className="flex items-center gap-2 truncate">
        {SORT_ICONS[sortCurrent.sort_by]}
        <span className="truncate">{sortCurrent.label}</span>
      </span>
      {/* прижимаем плотнее к правому краю */}
      <RxChevronDown className="w-4 h-4 mr-1" />
    </button>
  );

  return (
    <div className="bg-white rounded-lg shadow-md p-6 mb-6">
      {/* локальные стили для masony колонок (решение «звёздочки») */}
      <style>{`
        .facet-columns{column-gap:16px}
        @media (min-width: 768px){ .facet-columns{column-count:2} }
        @media (min-width: 1024px){ .facet-columns{column-count:3} }
        .facet-card{break-inside:avoid; -webkit-column-break-inside:avoid; margin-bottom:16px}
      `}</style>

      {/* верхняя панель: Search | Sort | (Scope) */}
      <div className="flex flex-col md:flex-row md:items-end gap-4">
        {/* Search */}
        <div className="flex-1">
          <label className="form-label">Search</label>
          <div className="flex items-center gap-2">
            <div className="relative flex-1">
              <input
                type="text"
                value={searchLocal}
                onChange={(e) => setSearchLocal(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter") commitSearch(); }}
                onCompositionStart={() => setIsComposing(true)}
                onCompositionEnd={() => setIsComposing(false)}
                placeholder="Search title, description, specs…"
                className="form-input input-md pr-10"
                disabled={isDisabled}
                aria-label="Search products"
              />
              {searchLocal ? (
                <button
                  type="button"
                  onClick={() => setSearchLocal("")}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-700" /* метлу чуть левее */
                  aria-label="Clear search"
                  title="Clear"
                >
                  <GiBroom className="w-5 h-5" />
                </button>
              ) : null}
            </div>
            <button
              type="button"
              onClick={commitSearch}
              className="btn-search"
              title="Apply search"
            >
              <FiSearch className="w-4 h-4" />
              <span>Search</span>
            </button>
          </div>
        </div>

        {/* Sort by — фиксированная ширина на desktop, full-width на mobile */}
        <div ref={sortRef} className="w-full md:w-56 shrink-0">
          <label className="form-label">Sort by</label>
          <div className="relative w-full">
            {SortTrigger}
            {sortOpen && (
              <ul
                role="listbox"
                className="absolute z-[60] mt-1 w-full bg-white border rounded-lg shadow-lg p-1"
                tabIndex={-1}
                onBlur={(e) => {
                  const next = e.relatedTarget;
                  if (sortRef.current && next && sortRef.current.contains(next)) return;
                  setSortOpen(false);
                }}
              >
                {SORT_OPTIONS.map((o) => (
                  <li
                    key={o.key}
                    role="option"
                    aria-selected={o.key === currentSortKey}
                    className={`px-2 py-2 rounded cursor-pointer flex items-center gap-2 hover:bg-gray-100 ${o.key === currentSortKey ? "bg-gray-50" : ""}`}
                    onClick={() => { updateSort(o.key); setSortOpen(false); }}
                    tabIndex={0}
                  >
                    {SORT_ICONS[o.sort_by]}
                    <span>{o.label}</span>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </div>

        {/* Scope — такая же высота, как и у строки поиска/сортировки */}
        {mode === "byTask" ? (
          <div className="w-full md:w-48 shrink-0">
            <label className="form-label">Scope</label>
            <div className="flex rounded-lg overflow-hidden border">
              <button
                type="button"
                className={`flex-1 px-3 text-sm control-md ${
                  scope === "task" ? "bg-blue-600 text-white" : "bg-white text-gray-700"
                }`}
                onClick={() => onScopeChange?.("task")}
                disabled={isDisabled}
                aria-pressed={scope === "task"}
              >
                Task
              </button>
              <button
                type="button"
                className={`flex-1 px-3 text-sm border-l control-md ${
                  scope === "pipeline" ? "bg-blue-600 text-white" : "bg-white text-gray-700"
                }`}
                onClick={() => onScopeChange?.("pipeline")}
                disabled={isDisabled}
                aria-pressed={scope === "pipeline"}
              >
                Pipeline
              </button>
            </div>
          </div>
        ) : null}
      </div>

      {/* активные фильтры (chips) */}
      {chips.length > 0 && (
        <div className="mt-4 flex flex-wrap gap-2">
          {chips.map((ch) => (
            <span
              key={ch.id}
              className="px-2 py-1 bg-blue-50 text-blue-700 rounded-full text-xs flex items-center gap-1"
            >
              {ch.label}
              <button
                type="button"
                className="ml-1 text-blue-800 hover:text-blue-900"
                onClick={() => clearChip(ch)}
                aria-label={`Remove ${ch.label}`}
                title="Remove"
              >
                ✕
              </button>
            </span>
          ))}
          <button
            type="button"
            onClick={clearAll}
            className="px-2 py-1 bg-gray-100 text-gray-700 rounded-full text-xs hover:bg-gray-200"
          >
            Clear all
          </button>
        </div>
      )}

      {/* Filters (суммари с иконками) + «Clear task» справа */}
      <details className="mt-3" open={filtersOpen} onToggle={(e) => setFiltersOpen(e.currentTarget.open)}>
        <summary className="text-sm text-gray-600 cursor-pointer select-none hover:text-gray-800 flex items-center justify-between">
          <span className="inline-flex items-center gap-2">
            {filtersOpen ? <RxTriangleDown /> : <RxTriangleRight />}
            <span>Filters {activeFiltersCount ? `(${activeFiltersCount})` : ""}</span>
          </span>
          {mode === "byTask" && (
            <button
              type="button"
              onClick={onClearTask}
              className="text-xs px-2 py-1 rounded bg-gray-100 hover:bg-gray-200"
              title="Reset task context"
            >
              Clear task
            </button>
          )}
        </summary>

        {/* ——— Masonry-like список характеристик: не растягивает соседей в «строке» ——— */}
        <div className="mt-3 facet-columns">
          {/* skeleton для фасетов */}
          {loadingFacets &&
            Array.from({ length: 6 }).map((_, i) => (
              <div key={`facet-sk-${i}`} className="facet-card bg-white border rounded-lg p-3">
                <div className="h-5 w-2/3 skeleton shimmer mb-3"></div>
                <div className="space-y-2">
                  <div className="h-4 w-5/6 skeleton shimmer"></div>
                  <div className="h-4 w-4/6 skeleton shimmer"></div>
                  <div className="h-4 w-3/6 skeleton shimmer"></div>
                </div>
              </div>
            ))}

          {!loadingFacets &&
            visibleCharacteristics.map((c) => {
              const selected = new Set(value?.filters?.[c.id] || []);
              const isOpen = openGroups[c.id] ?? false;
              const limit = showCountByChar[c.id] ?? INITIAL_SHOW;
              const allValues = c.values || [];
              const values = allValues.slice(0, limit);
              const hasMore = allValues.length > values.length;

              return (
                <div key={c.id} className="facet-card bg-white border rounded-lg">
                  <button
                    type="button"
                    className="w-full flex items-center justify-between px-3 py-2 bg-gray-50 rounded-t-lg hover:bg-gray-100"
                    onClick={() => toggleGroup(c.id)}
                    aria-expanded={isOpen}
                    aria-controls={`char-panel-${c.id}`}
                    disabled={isDisabled}
                  >
                    <span className="text-sm font-medium text-gray-800 truncate">
                      {c.title || c.id}
                    </span>
                    <span className="text-gray-500 inline-flex items-center">
                      {isOpen ? <RxTriangleDown /> : <RxTriangleRight />}
                    </span>
                  </button>

                  <div
                    id={`char-panel-${c.id}`}
                    className={`transition-all ${isOpen ? "max-h-[480px]" : "max-h-0"} overflow-hidden`}
                  >
                    <div className="p-3">
                      <div className="flex flex-col gap-2 max-h-64 overflow-auto pr-1">
                        {values.map((v) => {
                          const checked = selected.has(v);
                          return (
                            <label key={v} className="inline-flex items-center gap-2 text-sm">
                              <input
                                type="checkbox"
                                checked={checked}
                                onChange={() => toggleFilterValue(c.id, v)}
                                disabled={isDisabled}
                              />
                              <span className="text-gray-800 truncate">{v}</span>
                            </label>
                          );
                        })}
                        {allValues.length === 0 && (
                          <div className="text-xs text-gray-500">No values</div>
                        )}
                      </div>

                      {hasMore && (
                        <div className="mt-2">
                          <button
                            type="button"
                            className="px-3 py-1 text-xs rounded-lg bg-gray-100 hover:bg-gray-200"
                            onClick={() =>
                              setShowCountByChar((s) => ({ ...s, [c.id]: (s[c.id] || INITIAL_SHOW) + SHOW_STEP }))
                            }
                          >
                            Показать ещё
                          </button>
                          <button
                            type="button"
                            className="ml-2 px-3 py-1 text-xs rounded-lg bg-gray-100 hover:bg-gray-200"
                            onClick={() => setShowCountByChar((s) => ({ ...s, [c.id]: INITIAL_SHOW }))}
                          >
                            Свернуть
                          </button>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
        </div>

        <div className="mt-4 text-xs text-gray-500">
          {mode === "byTask" ? (
            <>
              Filtering products for <b>task</b> {taskId ? `#${taskId}` : ""}. Task selection is disabled here.
            </>
          ) : (
            <>Filtering global product list.</>
          )}
        </div>
      </details>
    </div>
  );
}

ProductsFilter.propTypes = {
  mode: PropTypes.oneOf(["all", "byTask"]).isRequired,
  taskId: PropTypes.string,
  scope: PropTypes.oneOf(["task", "pipeline"]),
  onScopeChange: PropTypes.func,
  onClearTask: PropTypes.func,
  characteristics: PropTypes.arrayOf(
    PropTypes.shape({
      id: PropTypes.string.isRequired,
      title: PropTypes.string,
      values: PropTypes.arrayOf(PropTypes.string).isRequired,
    })
  ),
  value: PropTypes.shape({
    q: PropTypes.string,
    sort_by: PropTypes.string,
    sort_dir: PropTypes.oneOf(["asc", "desc"]),
    filters: PropTypes.objectOf(PropTypes.arrayOf(PropTypes.string)),
  }).isRequired,
  onChange: PropTypes.func.isRequired,
  onReset: PropTypes.func,
  loading: PropTypes.bool,
  disabled: PropTypes.bool,
  loadingFacets: PropTypes.bool,
};
