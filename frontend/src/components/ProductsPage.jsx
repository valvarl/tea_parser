// components/ProductsPage.jsx
import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import PropTypes from "prop-types";
import { RxUpdate } from "react-icons/rx";
import ProductsFilter from "./ProductsFilter";
import Pagination from "./Pagination";
import ProductCard from "./ProductCard";
import ProductModal from "./ProductModal";

const DEFAULT_FILTER = {
  q: "",
  sort_by: "updated_at",
  sort_dir: "desc",
  filters: {}, // Record<charId, string[]>
};

const DEFAULT_QUERY = {
  mode: "all", // "all" | "byTask"
  taskId: "",
  scope: "task", // "task" | "pipeline"
  page: 1,
  limit: 24, // 12|24|48|96
  ...DEFAULT_FILTER,
};

// --- helpers: URL <-> query ---
function hasProductsParamsInUrl(search) {
  const sp = new URLSearchParams(search);
  const baseKeys = ["mode", "taskId", "scope", "q", "sort_by", "sort_dir", "page", "limit"];
  if (baseKeys.some((k) => sp.has(k))) return true;
  for (const k of sp.keys()) if (k.startsWith("char_")) return true;
  return false;
}

function readQueryFromUrl(search) {
  const sp = new URLSearchParams(search);
  const filters = {};
  for (const [k, v] of sp.entries()) {
    if (k.startsWith("char_")) {
      const cid = k.slice(5);
      (filters[cid] ??= []).push(v);
    }
  }
  const allowedLimits = [12, 24, 48, 96];
  const limitNum = parseInt(sp.get("limit") || `${DEFAULT_QUERY.limit}`, 10);
  const pageNum = parseInt(sp.get("page") || `${DEFAULT_QUERY.page}`, 10);

  return {
    mode: sp.get("mode") === "byTask" ? "byTask" : "all",
    taskId: sp.get("taskId") || "",
    scope: sp.get("scope") === "pipeline" ? "pipeline" : "task",
    page: Number.isFinite(pageNum) && pageNum > 0 ? pageNum : DEFAULT_QUERY.page,
    limit: allowedLimits.includes(limitNum) ? limitNum : DEFAULT_QUERY.limit,
    q: sp.get("q") || "",
    sort_by: sp.get("sort_by") || DEFAULT_QUERY.sort_by,
    sort_dir: sp.get("sort_dir") === "asc" ? "asc" : "desc",
    filters,
  };
}

function writeQueryToUrl(query) {
  const sp = new URLSearchParams(window.location.search);
  // tab оставляем как есть (им управляет App)
  ["mode", "taskId", "scope", "q", "sort_by", "sort_dir", "page", "limit"].forEach((k) => sp.delete(k));
  [...sp.keys()].forEach((k) => k.startsWith("char_") && sp.delete(k));

  sp.set("mode", query.mode);
  if (query.mode === "byTask" && query.taskId) {
    sp.set("taskId", query.taskId);
    sp.set("scope", query.scope || "task");
  }
  sp.set("page", String(query.page || 1));
  sp.set("limit", String(query.limit || 24));
  if (query.q) sp.set("q", query.q);
  sp.set("sort_by", query.sort_by || "updated_at");
  sp.set("sort_dir", query.sort_dir || "desc");

  Object.entries(query.filters || {}).forEach(([cid, vals]) => {
    (vals || []).forEach((v) => sp.append(`char_${cid}`, v));
  });

  const next = `${window.location.pathname}?${sp.toString()}`;
  const cur = `${window.location.pathname}${window.location.search}`;
  if (next !== cur) window.history.replaceState(null, "", next);
}

export default function ProductsPage({ api, initialQuery, onPersist }) {
  // ----- state -----
  const [products, setProducts] = useState([]);
  const [totalProducts, setTotalProducts] = useState(0);

  const [page, setPage] = useState(DEFAULT_QUERY.page);
  const [pageSize, setPageSize] = useState(DEFAULT_QUERY.limit);

  const [productsMode, setProductsMode] = useState(DEFAULT_QUERY.mode);
  const [productsTaskId, setProductsTaskId] = useState(DEFAULT_QUERY.taskId);
  const [productsScope, setProductsScope] = useState(DEFAULT_QUERY.scope);

  const [filterAll, setFilterAll] = useState({ ...DEFAULT_FILTER });
  const [filterByTask, setFilterByTask] = useState({}); // { [key]: FilterValue }

  const filterKey = useMemo(
    () => (productsMode === "byTask" ? `${productsTaskId || ""}::${productsScope}` : "all"),
    [productsMode, productsTaskId, productsScope],
  );
  const currentFilter = useMemo(
    () => (productsMode === "byTask" ? filterByTask[filterKey] || DEFAULT_FILTER : filterAll),
    [productsMode, filterByTask, filterKey, filterAll],
  );

  const [charAll, setCharAll] = useState([]);
  const [charByTask, setCharByTask] = useState({});
  const currentCharacteristics = useMemo(
    () => (productsMode === "byTask" ? charByTask[filterKey] || [] : charAll),
    [productsMode, charByTask, filterKey, charAll],
  );

  const [isFetching, setIsFetching] = useState(false);
  const [loadingFacets, setLoadingFacets] = useState(false);

  // URL sync flags
  const [didInit, setDidInit] = useState(false);
  const [isSearchFocused, setIsSearchFocused] = useState(false);
  const [qLive, setQLive] = useState("");

  // modal
  const [isProductModalOpen, setIsProductModalOpen] = useState(false);
  const [modalProduct, setModalProduct] = useState(null);

  // abort controllers
  const productsAbortRef = useRef(null);
  const facetsAbortRef = useRef(null);

  const totalPages = useMemo(() => Math.max(1, Math.ceil(Number(totalProducts || 0) / Number(pageSize || 1))), [totalProducts, pageSize]);

  // --- apply a full query object into component state
  const applyQuery = useCallback((q) => {
    const normalized = { ...DEFAULT_QUERY, ...(q || {}) };

    setProductsMode(normalized.mode);
    setProductsTaskId(normalized.taskId || "");
    setProductsScope(normalized.scope || "task");
    setPage(Number(normalized.page) || 1);
    setPageSize([12, 24, 48, 96].includes(Number(normalized.limit)) ? Number(normalized.limit) : 24);

    const parsedFilter = {
      q: normalized.q || "",
      sort_by: normalized.sort_by || "updated_at",
      sort_dir: normalized.sort_dir === "asc" ? "asc" : "desc",
      filters: normalized.filters || {},
    };

    if (normalized.mode === "byTask" && normalized.taskId) {
      const key = `${normalized.taskId}::${normalized.scope === "pipeline" ? "pipeline" : "task"}`;
      setFilterByTask((prev) => ({ ...prev, [key]: parsedFilter }));
    } else {
      setFilterAll(parsedFilter);
    }
  }, []);

  // --- compose current query object from state (for URL/persist)
  const buildCurrentQuery = useCallback(() => {
    const f = currentFilter || DEFAULT_FILTER;
    return {
      mode: productsMode,
      taskId: productsTaskId,
      scope: productsScope,
      page,
      limit: pageSize,
      q: f.q || "",
      sort_by: f.sort_by || "updated_at",
      sort_dir: f.sort_dir || "desc",
      filters: f.filters || {},
    };
  }, [productsMode, productsTaskId, productsScope, page, pageSize, currentFilter]);

  // ----- initialization (sync to avoid гонки) -----
  useLayoutEffect(() => {
    const urlHas = hasProductsParamsInUrl(window.location.search);
    const snap = urlHas ? readQueryFromUrl(window.location.search) : initialQuery || DEFAULT_QUERY;
    applyQuery(snap);
    // стандартизируем URL под выбранный снапшот
    writeQueryToUrl(snap);
    setDidInit(true);
  }, []); // один раз на маунт

  // ----- sync URL + persist to parent when state changes -----
  useEffect(() => {
    if (!didInit) return;
    const query = buildCurrentQuery();
    // во время набора показываем «черновик» q в URL, чтобы линк копировался корректно
    writeQueryToUrl(isSearchFocused ? { ...query, q: qLive } : query);
    if (onPersist) onPersist(query);
  }, [didInit, buildCurrentQuery, isSearchFocused, qLive, onPersist]);

  // ----- data fetching -----
  const fetchProducts = useCallback(async () => {
    // гард: не ходим в /tasks//products
    if (productsMode === "byTask" && !productsTaskId) return;

    setIsFetching(true);
    try {
      if (productsAbortRef.current) productsAbortRef.current.abort();
      productsAbortRef.current = new AbortController();

      const f = currentFilter || DEFAULT_FILTER;
      const params = new URLSearchParams();
      params.set("limit", String(pageSize));
      params.set("skip", String((page - 1) * pageSize));
      if (f.q) params.set("q", f.q);
      params.set("sort_by", f.sort_by || "updated_at");
      params.set("sort_dir", f.sort_dir || "desc");
      Object.entries(f.filters || {}).forEach(([cid, vals]) => {
        (vals || []).forEach((v) => params.append(`char_${cid}`, v));
      });

      if (productsMode === "byTask") {
        params.set("scope", productsScope);
        const url = `/tasks/${encodeURIComponent(productsTaskId)}/products?${params.toString()}`;
        const res = await api.get(url, { signal: productsAbortRef.current.signal });
        setProducts(res.data?.items || []);
        setTotalProducts(res.data?.total || 0);
      } else {
        const url = `/products?${params.toString()}`;
        const res = await api.get(url, { signal: productsAbortRef.current.signal });
        setProducts(res.data?.items || []);
        setTotalProducts(res.data?.total || 0);
      }
    } finally {
      setIsFetching(false);
    }
  }, [api, page, pageSize, productsMode, productsTaskId, productsScope, currentFilter]);

  const fetchFilterCharacteristics = useCallback(async () => {
    // гард: ждём taskId
    if (productsMode === "byTask" && !productsTaskId) return;

    try {
      setLoadingFacets(true);
      if (facetsAbortRef.current) facetsAbortRef.current.abort();
      facetsAbortRef.current = new AbortController();

      const f = currentFilter || DEFAULT_FILTER;
      const facetParams = new URLSearchParams();
      if (f.q) facetParams.set("q", f.q);
      Object.entries(f.filters || {}).forEach(([cid, vals]) => {
        (vals || []).forEach((v) => facetParams.append(`char_${cid}`, v));
      });

      if (productsMode === "byTask") {
        facetParams.set("scope", productsScope);
        const url = `/tasks/${encodeURIComponent(productsTaskId)}/products/characteristics?${facetParams.toString()}`;
        const res = await api.get(url, { signal: facetsAbortRef.current.signal });
        const arr = Array.isArray(res.data) ? res.data : [];
        setCharByTask((prev) => ({ ...prev, [filterKey]: arr }));
      } else {
        const url = `/products/characteristics?${facetParams.toString()}`;
        const res = await api.get(url, { signal: facetsAbortRef.current.signal });
        setCharAll(Array.isArray(res.data) ? res.data : res.data?.items || []);
      }
    } catch {
      /* noop */
    } finally {
      setLoadingFacets(false);
    }
  }, [api, productsMode, productsTaskId, productsScope, filterKey, currentFilter]);

  useEffect(() => {
    if (!didInit) return;
    fetchProducts().catch(() => {});
    fetchFilterCharacteristics().catch(() => {});
  }, [didInit, fetchProducts, fetchFilterCharacteristics, currentFilter, page, pageSize, productsMode, productsTaskId, productsScope]);

  // ----- modal helpers -----
  const openProductModal = useCallback(
    (prodOrSku) => {
      if (!prodOrSku) return;
      if (typeof prodOrSku === "object") {
        setModalProduct(prodOrSku);
        setIsProductModalOpen(true);
        return;
      }
      const found = products.find((p) => String(p.sku) === String(prodOrSku));
      if (found) {
        setModalProduct(found);
        setIsProductModalOpen(true);
        return;
      }
      api
        .get(`/products/sku/${encodeURIComponent(String(prodOrSku))}`)
        .then((res) => {
          if (res?.data) {
            setModalProduct(res.data);
            setIsProductModalOpen(true);
          }
        })
        .catch(() => {});
    },
    [api, products],
  );
  const closeProductModal = useCallback(() => {
    setIsProductModalOpen(false);
    setModalProduct(null);
  }, []);

  // ----- filter handlers (для ProductsFilter) -----
  const handleFilterChange = useCallback(
    (next) => {
      setPage(1);
      if (productsMode === "byTask") {
        setFilterByTask((prev) => ({ ...prev, [filterKey]: next }));
      } else {
        setFilterAll(next);
      }
    },
    [productsMode, filterKey],
  );

  const handleFilterReset = useCallback(() => {
    setPage(1);
    if (productsMode === "byTask") {
      setFilterByTask((prev) => ({ ...prev, [filterKey]: { ...DEFAULT_FILTER } }));
    } else {
      setFilterAll({ ...DEFAULT_FILTER });
    }
  }, [productsMode, filterKey]);

  const handleClearTask = useCallback(() => {
    setProductsMode("all");
    setProductsTaskId("");
    setPage(1);
  }, []);

  // ----- render -----
  return (
    <>
      <div className="space-y-6">
        <div className="bg-white rounded-lg shadow-md p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold">Products ({totalProducts})</h3>
            <button
              onClick={() => {
                setPage(1);
                fetchProducts().catch(() => {});
              }}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2"
              title="Refresh results">
              <RxUpdate className="w-5 h-5" />
              Refresh
            </button>
          </div>

          <ProductsFilter
            mode={productsMode}
            taskId={productsTaskId}
            scope={productsScope}
            onScopeChange={setProductsScope}
            onClearTask={handleClearTask}
            characteristics={currentCharacteristics}
            value={currentFilter}
            onChange={handleFilterChange}
            onReset={handleFilterReset}
            loadingFacets={loadingFacets}
            onSearchFocusChange={setIsSearchFocused}
            onSearchCommit={() => {
              const q = buildCurrentQuery();
              writeQueryToUrl(q);
            }}
            onSearchTyping={setQLive}
          />

          <div className="relative">
            {isFetching && (
              <div className="absolute inset-0 z-10 bg-white/60 backdrop-blur-[1px] flex items-center justify-center">
                <div className="loading-spinner" aria-label="Loading" />
              </div>
            )}

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {products.map((p) => (
                <ProductCard
                  key={p.id}
                  product={p}
                  onDelete={async (id) => {
                    if (!window.confirm("Delete this product?")) return;
                    await api.delete(`/products/${id}`);
                    fetchProducts().catch(() => {});
                  }}
                  onOpen={() => openProductModal(p)}
                />
              ))}
            </div>
          </div>

          {products.length === 0 && !isFetching && (
            <div className="text-center py-12">
              <div className="text-6xl mb-4">🍃</div>
              <p className="text-gray-500">No products found</p>
              <p className="text-sm text-gray-400 mt-2">Start scraping to populate the list</p>
            </div>
          )}

          <Pagination
            page={page}
            totalPages={totalPages}
            pageSize={pageSize}
            onPageChange={(p) => setPage(p)}
            onPageSizeChange={(sz) => {
              setPage(1);
              setPageSize(sz);
            }}
          />
        </div>
      </div>
      <ProductModal
        isOpen={isProductModalOpen}
        product={modalProduct || {}}
        onClose={closeProductModal}
        onSelectSku={(sku) => openProductModal(sku)}
      />
    </>
  );
}

ProductsPage.propTypes = {
  api: PropTypes.object.isRequired, // axios instance
  initialQuery: PropTypes.shape({
    mode: PropTypes.oneOf(["all", "byTask"]),
    taskId: PropTypes.string,
    scope: PropTypes.oneOf(["task", "pipeline"]),
    page: PropTypes.number,
    limit: PropTypes.oneOf([12, 24, 48, 96]),
    q: PropTypes.string,
    sort_by: PropTypes.string,
    sort_dir: PropTypes.oneOf(["asc", "desc"]),
    filters: PropTypes.object, // Record<string, string[]>
  }),
  onPersist: PropTypes.func, // (queryObj) => void
};

ProductsPage.defaultProps = {
  initialQuery: DEFAULT_QUERY,
  onPersist: undefined,
};
