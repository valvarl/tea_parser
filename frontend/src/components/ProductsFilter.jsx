// components/ProductsFilter.jsx
import React, { useEffect, useMemo, useState } from "react";
import PropTypes from "prop-types";

const SORT_OPTIONS = [
  { label: "Updated ↓", sort_by: "updated_at", sort_dir: "desc", key: "updated_desc" },
  { label: "Updated ↑", sort_by: "updated_at", sort_dir: "asc",  key: "updated_asc"  },
  { label: "Created ↓", sort_by: "created_at", sort_dir: "desc", key: "created_desc" },
  { label: "Created ↑", sort_by: "created_at", sort_dir: "asc",  key: "created_asc"  },
  { label: "Title A→Z", sort_by: "title",      sort_dir: "asc",  key: "title_asc"    },
  { label: "Title Z→A", sort_by: "title",      sort_dir: "desc", key: "title_desc"   },
  { label: "SKU ↑",     sort_by: "sku",        sort_dir: "asc",  key: "sku_asc"      },
  { label: "SKU ↓",     sort_by: "sku",        sort_dir: "desc", key: "sku_desc"     },
  { label: "Price ↑",   sort_by: "price",      sort_dir: "asc",  key: "price_asc"    },
  { label: "Price ↓",   sort_by: "price",      sort_dir: "desc", key: "price_desc"   },
  { label: "Rating ↓",  sort_by: "rating",     sort_dir: "desc", key: "rating_desc"  },
  { label: "Rating ↑",  sort_by: "rating",     sort_dir: "asc",  key: "rating_asc"   },
];

function keyForSort(sort_by, sort_dir) {
  const found = SORT_OPTIONS.find(s => s.sort_by === sort_by && s.sort_dir === sort_dir);
  return found?.key || "updated_desc";
}

export default function ProductsFilter({
  mode,                // "all" | "byTask"
  taskId,              // string | undefined
  scope,               // "task" | "pipeline"
  onScopeChange,       // fn | undefined
  characteristics,     // Array<{id,title,values[]}>
  value,               // { q, sort_by, sort_dir, filters: Record<id, string[]> }
  onChange,            // (nextValue) => void
  onReset,             // () => void
  loading = false,
  disabled = false,
}) {
  const [searchLocal, setSearchLocal] = useState(value?.q || "");
  const [openGroups, setOpenGroups] = useState({}); // { [charId]: boolean }

  // синхронизируем локальное поле поиска, если внешнее состояние сменилось
  useEffect(() => {
    setSearchLocal(value?.q || "");
  }, [value?.q]);

  // debounce поисковой строки
  useEffect(() => {
    const t = setTimeout(() => {
      if ((value?.q || "") !== (searchLocal || "")) {
        onChange?.({ ...value, q: searchLocal || "" });
      }
    }, 350);
    return () => clearTimeout(t);
  }, [searchLocal]);

  const currentSortKey = useMemo(
    () => keyForSort(value?.sort_by || "updated_at", value?.sort_dir || "desc"),
    [value?.sort_by, value?.sort_dir]
  );

  const chips = useMemo(() => {
    const out = [];
    if (value?.q) out.push({ type: "q", id: "_q", label: `Search: “${value.q}”` });
    const f = value?.filters || {};
    Object.entries(f).forEach(([cid, vals]) => {
      (vals || []).forEach(v => {
        out.push({ type: "char", id: `${cid}::${v}`, label: `${cid}: ${v}`, cid, v });
      });
    });
    return out;
  }, [value]);

  const toggleGroup = (cid) => setOpenGroups(prev => ({ ...prev, [cid]: !prev[cid] }));

  const updateSort = (key) => {
    const s = SORT_OPTIONS.find(x => x.key === key) || SORT_OPTIONS[0];
    onChange?.({ ...value, sort_by: s.sort_by, sort_dir: s.sort_dir });
  };

  const toggleFilterValue = (cid, val) => {
    const filters = { ...(value?.filters || {}) };
    const arr = new Set(filters[cid] || []);
    if (arr.has(val)) arr.delete(val); else arr.add(val);
    filters[cid] = Array.from(arr);
    if (filters[cid].length === 0) delete filters[cid];
    onChange?.({ ...value, filters });
  };

  const clearChip = (chip) => {
    if (chip.type === "q") {
      setSearchLocal("");
      onChange?.({ ...value, q: "" });
      return;
    }
    const { cid, v } = chip;
    toggleFilterValue(cid, v); // переиспользуем логику
  };

  const clearAll = () => {
    onReset?.();
    setSearchLocal("");
  };

  const isDisabled = disabled || loading;

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      {/* Верхняя панель: поиск | сортировка | (scope) | reset */}
      <div className="grid grid-cols-1 md:grid-cols-12 md:items-center gap-4">
        {/* Search */}
        <div className="md:col-span-6">
          <label className="form-label">Search</label>
          <div className="relative">
            <input
              type="text"
              value={searchLocal}
              onChange={(e) => setSearchLocal(e.target.value)}
              placeholder='Search title, description, specs…'
              className="form-input pr-9"
              disabled={isDisabled}
              aria-label="Search products"
            />
            {searchLocal ? (
              <button
                type="button"
                onClick={() => setSearchLocal("")}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-gray-500 hover:text-gray-700"
                aria-label="Clear search"
                title="Clear"
              >
                ✕
              </button>
            ) : null}
          </div>
        </div>

        {/* Sort */}
        <div className="md:col-span-3">
          <label className="form-label">Sort by</label>
          <select
            value={currentSortKey}
            onChange={(e) => updateSort(e.target.value)}
            className="form-input"
            disabled={isDisabled}
            aria-label="Sort products"
          >
            {SORT_OPTIONS.map(o => (
              <option key={o.key} value={o.key}>{o.label}</option>
            ))}
          </select>
        </div>

        {/* Scope (только в режиме byTask) */}
        {mode === "byTask" ? (
          <div className="md:col-span-2">
            <label className="form-label">Scope</label>
            <div className="flex rounded-lg overflow-hidden border">
              <button
                type="button"
                className={`flex-1 px-3 py-2 text-sm ${scope === "task" ? "bg-blue-600 text-white" : "bg-white text-gray-700"}`}
                onClick={() => onScopeChange?.("task")}
                disabled={isDisabled}
                aria-pressed={scope === "task"}
              >
                Task
              </button>
              <button
                type="button"
                className={`flex-1 px-3 py-2 text-sm border-l ${scope === "pipeline" ? "bg-blue-600 text-white" : "bg-white text-gray-700"}`}
                onClick={() => onScopeChange?.("pipeline")}
                disabled={isDisabled}
                aria-pressed={scope === "pipeline"}
              >
                Pipeline
              </button>
            </div>
          </div>
        ) : <div className="md:col-span-2" />}

        {/* Reset */}
        <div className="md:col-span-1 flex md:justify-end">
          <button
            type="button"
            onClick={clearAll}
            className="btn-secondary w-full md:w-auto"
            disabled={isDisabled}
            aria-label="Reset filters"
            title="Reset filters"
          >
            ↺ Reset
          </button>
        </div>
      </div>

      {/* Чипсы выбранных фильтров */}
      {(chips.length > 0) && (
        <div className="mt-4 flex flex-wrap gap-2">
          {chips.map(ch => (
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

      {/* Секции характеристик */}
      <div className="mt-6 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {(characteristics || []).map((c) => {
          const selected = new Set((value?.filters?.[c.id] || []));
          const isOpen = openGroups[c.id] ?? false;
          const values = Array.isArray(c.values) ? c.values : [];
          const head = (
            <button
              type="button"
              className="w-full flex items-center justify-between px-3 py-2 bg-gray-50 border rounded-lg hover:bg-gray-100"
              onClick={() => toggleGroup(c.id)}
              aria-expanded={isOpen}
              aria-controls={`char-panel-${c.id}`}
              disabled={isDisabled}
            >
              <span className="text-sm font-medium text-gray-800 truncate">{c.title || c.id}</span>
              <span className="text-gray-500">{isOpen ? "▾" : "▸"}</span>
            </button>
          );

          return (
            <div key={c.id} className="bg-white border rounded-lg">
              {head}
              <div
                id={`char-panel-${c.id}`}
                className={`transition-all ${isOpen ? "max-h-[480px]" : "max-h-0"} overflow-hidden`}
              >
                <div className="p-3">
                  {values.length === 0 ? (
                    <div className="text-xs text-gray-500">No values</div>
                  ) : (
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
                    </div>
                  )}
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* Подвал: подсказки */}
      <div className="mt-4 text-xs text-gray-500">
        {mode === "byTask" ? (
          <span>
            Filtering products for <b>task</b> {taskId ? `#${taskId}` : ""}. Task selection is disabled here.
          </span>
        ) : (
          <span>Filtering global product list.</span>
        )}
      </div>
    </div>
  );
}

ProductsFilter.propTypes = {
  mode: PropTypes.oneOf(["all", "byTask"]).isRequired,
  taskId: PropTypes.string,
  scope: PropTypes.oneOf(["task", "pipeline"]),
  onScopeChange: PropTypes.func,
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
};
