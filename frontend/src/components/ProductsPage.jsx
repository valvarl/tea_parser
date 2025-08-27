// components/ProductsPage.jsx
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
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

export default function ProductsPage({ api }) {
  // ----- state -----
  const [products, setProducts] = useState([]);
  const [totalProducts, setTotalProducts] = useState(0);

  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(24);

  const [productsMode, setProductsMode] = useState("all"); // "all" | "byTask"
  const [productsTaskId, setProductsTaskId] = useState("");
  const [productsScope, setProductsScope] = useState("task"); // "task" | "pipeline"

  const [filterAll, setFilterAll] = useState({ ...DEFAULT_FILTER });
  const [filterByTask, setFilterByTask] = useState({}); // {[key]: FilterValue}
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

  // URL sync helpers
  const [didInitFromUrl, setDidInitFromUrl] = useState(false);
  const [isSearchFocused, setIsSearchFocused] = useState(false);
  const [qLive, setQLive] = useState("");
  const [qDebounced, setQDebounced] = useState("");
  useEffect(() => setQDebounced(currentFilter.q || ""), [currentFilter.q]);

  // modal
  const [isProductModalOpen, setIsProductModalOpen] = useState(false);
  const [modalProduct, setModalProduct] = useState(null);

  // abort controllers
  const productsAbortRef = useRef(null);
  const facetsAbortRef = useRef(null);

  const totalPages = useMemo(() => Math.max(1, Math.ceil(Number(totalProducts || 0) / Number(pageSize || 1))), [totalProducts, pageSize]);

  // ----- URL <-> state -----
  const parseUrlToState = useCallback(() => {
    const sp = new URLSearchParams(window.location.search);
    const mode = sp.get("mode");
    const taskId = sp.get("taskId");
    const scope = sp.get("scope");
    const q = sp.get("q") || "";
    const sort_by = sp.get("sort_by") || "updated_at";
    const sort_dir = (sp.get("sort_dir") || "desc") === "asc" ? "asc" : "desc";
    const pageQ = parseInt(sp.get("page") || "1", 10);
    const limitQ = parseInt(sp.get("limit") || "24", 10);

    // собрать char_* -> Record<string,string[]>
    const filters = {};
    for (const [k, v] of sp.entries()) {
      if (k.startsWith("char_") && v) {
        const cid = k.slice(5);
        (filters[cid] ??= []).push(v);
      }
    }

    if (Number.isFinite(pageQ) && pageQ > 0) setPage(pageQ);
    if (Number.isFinite(limitQ) && [12, 24, 48, 96].includes(limitQ)) setPageSize(limitQ);

    const parsedFilter = { q, sort_by, sort_dir, filters };
    if (mode === "byTask" && taskId) {
      setProductsMode("byTask");
      setProductsTaskId(taskId);
      setProductsScope(scope === "pipeline" ? "pipeline" : "task");
      const key = `${taskId}::${scope === "pipeline" ? "pipeline" : "task"}`;
      setFilterByTask((prev) => ({ ...prev, [key]: parsedFilter }));
    } else {
      setProductsMode("all");
      setFilterAll(parsedFilter);
    }
  }, []);

  const writeStateToUrl = useCallback(() => {
    if (!didInitFromUrl) return;
    const sp = new URLSearchParams(window.location.search);

    // NB: tab управляется App; здесь трогаем только product-параметры
    sp.set("mode", productsMode);
    if (productsMode === "byTask" && productsTaskId) {
      sp.set("taskId", productsTaskId);
      sp.set("scope", productsScope);
    } else {
      sp.delete("taskId");
      sp.delete("scope");
    }

    sp.set("page", String(page));
    sp.set("limit", String(pageSize));
    const qParam = isSearchFocused ? qLive : currentFilter.q || "";
    if (qParam) sp.set("q", qParam);
    else sp.delete("q");

    sp.set("sort_by", currentFilter.sort_by || "updated_at");
    sp.set("sort_dir", currentFilter.sort_dir || "desc");

    // сбросить старые char_*, затем записать заново
    [...sp.keys()].forEach((k) => k.startsWith("char_") && sp.delete(k));
    Object.entries(currentFilter.filters || {}).forEach(([cid, vals]) => {
      (vals || []).forEach((v) => sp.append(`char_${cid}`, v));
    });

    const nextUrl = `${window.location.pathname}?${sp.toString()}`;
    if (nextUrl !== `${window.location.pathname}${window.location.search}`) {
      window.history.replaceState(null, "", nextUrl);
    }
  }, [didInitFromUrl, productsMode, productsTaskId, productsScope, page, pageSize, isSearchFocused, qLive, currentFilter]);

  // ----- data fetching -----
  const fetchProducts = useCallback(async () => {
    setIsFetching(true);
    try {
      if (productsAbortRef.current) productsAbortRef.current.abort();
      productsAbortRef.current = new AbortController();

      // build common params
      const params = new URLSearchParams();
      params.set("limit", String(pageSize));
      params.set("skip", String((page - 1) * pageSize));
      if (currentFilter.q) params.set("q", currentFilter.q);
      params.set("sort_by", currentFilter.sort_by || "updated_at");
      params.set("sort_dir", currentFilter.sort_dir || "desc");
      Object.entries(currentFilter.filters || {}).forEach(([cid, vals]) => {
        (vals || []).forEach((v) => params.append(`char_${cid}`, v));
      });

      if (productsMode === "byTask" && productsTaskId) {
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
    try {
      setLoadingFacets(true);
      if (facetsAbortRef.current) facetsAbortRef.current.abort();
      facetsAbortRef.current = new AbortController();

      const facetParams = new URLSearchParams();
      if (currentFilter.q) facetParams.set("q", currentFilter.q);
      Object.entries(currentFilter.filters || {}).forEach(([cid, vals]) => {
        (vals || []).forEach((v) => facetParams.append(`char_${cid}`, v));
      });

      if (productsMode === "byTask" && productsTaskId) {
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

  // ----- modal helpers -----
  const openProductModal = useCallback(
    (prodOrSku) => {
      if (!prodOrSku) return;
      if (typeof prodOrSku === "object") {
        setModalProduct(prodOrSku);
        setIsProductModalOpen(true);
        return;
      }
      // try find on page
      const found = products.find((p) => String(p.sku) === String(prodOrSku));
      if (found) {
        setModalProduct(found);
        setIsProductModalOpen(true);
        return;
      }
      // fallback: fetch by SKU
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

  // ----- handlers passed to children -----
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

  const deleteProduct = useCallback(
    async (productId) => {
      if (!window.confirm("Delete this product?")) return;
      await api.delete(`/products/${productId}`);
      fetchProducts().catch(() => {});
    },
    [api, fetchProducts],
  );

  // ----- lifecycle -----
  useEffect(() => {
    parseUrlToState();
    setDidInitFromUrl(true);
  }, [parseUrlToState]);

  useEffect(() => {
    writeStateToUrl();
  }, [writeStateToUrl]);

  useEffect(() => {
    if (!didInitFromUrl) return;
    fetchProducts().catch(() => {});
    fetchFilterCharacteristics().catch(() => {});
  }, [
    didInitFromUrl,
    fetchProducts,
    fetchFilterCharacteristics,
    currentFilter,
    page,
    pageSize,
    productsMode,
    productsTaskId,
    productsScope,
  ]);

  // ----- render -----
  return (
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
          onSearchCommit={writeStateToUrl}
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
              <ProductCard key={p.id} product={p} onDelete={deleteProduct} onOpen={() => openProductModal(p)} />
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

      <ProductModal
        isOpen={isProductModalOpen}
        product={modalProduct || {}}
        onClose={closeProductModal}
        onSelectSku={(sku) => openProductModal(sku)}
      />
    </div>
  );
}

ProductsPage.propTypes = {
  api: PropTypes.object.isRequired, // axios instance
};
