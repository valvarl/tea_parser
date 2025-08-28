import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import PropTypes from "prop-types";
import { RxUpdate } from "react-icons/rx";
import Pagination from "./Pagination";
import TeaCard from "./ProductCard";
import ProductModal from "./ProductModal";

const TYPE_META = {
  collection: { label: "Collection", chip: "bg-sky-100 text-sky-800" },
  aspect: { label: "Aspect", chip: "bg-purple-100 text-purple-800" },
  other_offer: { label: "Offer", chip: "bg-amber-100 text-amber-800" },
};

// Compact “filter” area (API supports only structured params, no free text)
function CollectionsFilter({ value, onChange, onReset, loading }) {
  const v = value || {};
  return (
    <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
      <div className="grid grid-cols-1 md:grid-cols-5 gap-3 flex-1">
        <div>
          <label className="block text-xs text-gray-600 mb-1">Type</label>
          <select
            className="w-full px-2 py-2 border rounded-lg"
            value={v.type || ""}
            onChange={(e) => onChange({ ...v, type: e.target.value || undefined })}>
            <option value="">All</option>
            <option value="collection">Collections</option>
            <option value="aspect">Aspects</option>
            <option value="other_offer">Offers</option>
          </select>
        </div>

        <div>
          <label className="block text-xs text-gray-600 mb-1">Status</label>
          <select
            className="w-full px-2 py-2 border rounded-lg"
            value={v.status || ""}
            onChange={(e) => onChange({ ...v, status: e.target.value || undefined })}>
            <option value="">All</option>
            <option value="pending">Pending</option>
            <option value="accepted">Accepted</option>
            <option value="rejected">Rejected</option>
          </select>
        </div>

        <div>
          <label className="block text-xs text-gray-600 mb-1">Task ID</label>
          <input
            className="w-full px-2 py-2 border rounded-lg"
            placeholder="task_id…"
            value={v.task_id || ""}
            onChange={(e) => onChange({ ...v, task_id: e.target.value || undefined })}
          />
        </div>

        <div>
          <label className="block text-xs text-gray-600 mb-1">Source SKU</label>
          <input
            className="w-full px-2 py-2 border rounded-lg"
            placeholder="come_from…"
            value={v.come_from || ""}
            onChange={(e) => onChange({ ...v, come_from: e.target.value || undefined })}
          />
        </div>

        <div>
          <label className="block text-xs text-gray-600 mb-1">Inside SKU</label>
          <input
            className="w-full px-2 py-2 border rounded-lg"
            placeholder="sku in collection…"
            value={v.sku || ""}
            onChange={(e) => onChange({ ...v, sku: e.target.value || undefined })}
          />
        </div>
      </div>

      <div className="flex gap-2">
        <button onClick={onReset} className="px-3 py-2 border rounded-lg bg-white hover:bg-gray-50" disabled={loading}>
          Reset
        </button>
        <button
          onClick={() => onChange({ ...v })} // noop: parent reacts anyway
          className="px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 flex items-center gap-2"
          disabled={loading}
          title="Refresh">
          <RxUpdate className="w-5 h-5" />
          Refresh
        </button>
      </div>
    </div>
  );
}

CollectionsFilter.propTypes = {
  value: PropTypes.object.isRequired,
  onChange: PropTypes.func.isRequired,
  onReset: PropTypes.func.isRequired,
  loading: PropTypes.bool,
};

// --- helper: lightweight concurrency limiter for SKU fetches
async function withConcurrency(items, limit, worker) {
  const ret = [];
  let i = 0;
  const running = new Set();
  async function runOne(idx) {
    const p = Promise.resolve(worker(items[idx], idx))
      .then((r) => (ret[idx] = r))
      .finally(() => running.delete(p));
    running.add(p);
  }
  while (i < items.length) {
    while (running.size < limit && i < items.length) {
      await runOne(i++);
    }
    await Promise.race(Array.from(running));
  }
  await Promise.all(Array.from(running));
  return ret;
}

export default function CollectionsPage({ api }) {
  // list + pagination
  const [collections, setCollections] = useState([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(24);

  // filter
  const [filter, setFilter] = useState({
    type: undefined,
    status: undefined,
    task_id: undefined,
    come_from: undefined,
    sku: undefined,
  });

  const [loading, setLoading] = useState(false);

  // product cache by SKU (for rows)
  const [productBySku, setProductBySku] = useState(() => Object.create(null));
  const fetchingSkusRef = useRef(new Set()); // guard against duplicate in-flight calls

  // modal
  const [isProductModalOpen, setIsProductModalOpen] = useState(false);
  const [modalProduct, setModalProduct] = useState(null);

  const totalPages = useMemo(() => Math.max(1, Math.ceil(Number(total || 0) / Number(pageSize || 1))), [total, pageSize]);

  // Build query string
  const buildQuery = useCallback(() => {
    const params = new URLSearchParams();
    params.set("skip", String((page - 1) * pageSize));
    params.set("limit", String(pageSize));
    if (filter.type) params.set("type", filter.type);
    if (filter.status) params.set("status", filter.status);
    if (filter.task_id) params.set("task_id", filter.task_id);
    if (filter.come_from) params.set("come_from", filter.come_from);
    if (filter.sku) params.set("sku", filter.sku);
    return params.toString();
  }, [page, pageSize, filter]);

  const loadCollections = useCallback(async () => {
    setLoading(true);
    try {
      const qs = buildQuery();
      const res = await api.get(`/collections?${qs}`);
      const items = res.data?.items || [];
      setCollections(items);
      setTotal(res.data?.total ?? items.length);
    } finally {
      setLoading(false);
    }
  }, [api, buildQuery]);

  // Fetch product objects for SKUs we don't have yet (concurrency-limited)
  const hydrateMissingProducts = useCallback(
    async (cols) => {
      const missing = [];
      for (const col of cols) {
        for (const it of col.skus || []) {
          const sku = String(it?.sku || "");
          if (!sku) continue;
          if (!productBySku[sku] && !fetchingSkusRef.current.has(sku)) {
            missing.push(sku);
          }
        }
      }
      if (!missing.length) return;

      // Optimistically mark in-flight
      missing.forEach((s) => fetchingSkusRef.current.add(s));

      try {
        await withConcurrency(missing, 6, async (sku) => {
          try {
            const r = await api.get(`/products/sku/${encodeURIComponent(sku)}`);
            const prod = r?.data || null;
            setProductBySku((prev) => ({ ...prev, [sku]: prod }));
          } catch {
            setProductBySku((prev) => ({ ...prev, [sku]: null })); // cache miss to avoid refetch loop
          }
        });
      } finally {
        missing.forEach((s) => fetchingSkusRef.current.delete(s));
      }
    },
    [api, productBySku],
  );

  // initial + every change of page/filter
  useEffect(() => {
    loadCollections().catch(() => {});
  }, [loadCollections]);

  // hydrate products for newly loaded collections
  useEffect(() => {
    hydrateMissingProducts(collections).catch(() => {});
  }, [collections, hydrateMissingProducts]);

  // Open modal: accept product object if present, else fetch by SKU
  const openProductModal = useCallback(
    async (prodOrSku) => {
      if (!prodOrSku) return;
      if (typeof prodOrSku === "object") {
        setModalProduct(prodOrSku);
        setIsProductModalOpen(true);
        return;
      }
      const sku = String(prodOrSku);
      const cached = productBySku[sku];
      if (cached) {
        setModalProduct(cached);
        setIsProductModalOpen(true);
        return;
      }
      try {
        const r = await api.get(`/products/sku/${encodeURIComponent(sku)}`);
        setModalProduct(r?.data || {});
        setIsProductModalOpen(true);
      } catch {
        // noop
      }
    },
    [api, productBySku],
  );
  const closeProductModal = useCallback(() => {
    setIsProductModalOpen(false);
    setModalProduct(null);
  }, []);

  // Accept / Reject all — optimistic update + placeholder API call
  const bulkSetStatus = useCallback(async (collection, nextStatus) => {
    // optimistic UI
    setCollections((prev) =>
      prev.map((c) =>
        c.collection_hash === collection.collection_hash
          ? {
              ...c,
              status: nextStatus, // collection-level
              skus: (c.skus || []).map((s) => ({ ...s, status: nextStatus })),
            }
          : c,
      ),
    );

    // Try to hit a backend endpoint if you add one later.
    // Examples (uncomment one when backend supports it):
    // await api.post(`/collections/${collection.collection_hash}/accept-all`);
    // await api.post(`/collections/${collection.collection_hash}/reject-all`);
    // or a generic:
    // await api.post(`/collections/${collection.collection_hash}/bulk-status`, { status: nextStatus });
    // If it fails, we keep optimistic state; you can refetch here if preferred.
    try {
      // no-op by default
    } catch {
      // Optionally: await loadCollections();
    }
  }, []);

  const resetFilters = useCallback(() => {
    setFilter({ type: undefined, status: undefined, task_id: undefined, come_from: undefined, sku: undefined });
    setPage(1);
  }, []);

  // --- render one collection row
  const renderCollectionRow = (col) => {
    const meta = TYPE_META[col.type] || { label: col.type || "Unknown", chip: "bg-gray-100 text-gray-800" };
    const title =
      col.type === "aspect"
        ? `${meta.label}${col.aspect_name ? `: ${col.aspect_name}` : col.aspect_key ? `: ${col.aspect_key}` : ""}`
        : meta.label;

    const pills = [
      { key: "type", text: title, cls: meta.chip },
      col.status ? { key: "status", text: col.status, cls: "bg-gray-100 text-gray-800" } : null,
      col.task_id ? { key: "task", text: `task:${col.task_id}`, cls: "bg-blue-100 text-blue-800" } : null,
      col.come_from ? { key: "from", text: `from:${col.come_from}`, cls: "bg-emerald-100 text-emerald-800" } : null,
      { key: "count", text: `${(col.skus || []).length} items`, cls: "bg-gray-100 text-gray-800" },
    ].filter(Boolean);

    return (
      <div key={col.collection_hash} className="bg-white rounded-lg shadow p-4">
        <div className="flex items-center justify-between gap-3 mb-3">
          <div className="flex flex-wrap gap-2">
            {pills.map((p) => (
              <span key={p.key} className={`px-2 py-1 rounded-full text-xs font-medium ${p.cls}`}>
                {p.text}
              </span>
            ))}
          </div>
          <div className="flex gap-2">
            <button
              className="px-3 py-1.5 rounded-lg border bg-white hover:bg-gray-50"
              onClick={() => bulkSetStatus(col, "rejected")}
              title="Reject all products in this collection">
              Reject all
            </button>
            <button
              className="px-3 py-1.5 rounded-lg bg-green-600 text-white hover:bg-green-700"
              onClick={() => bulkSetStatus(col, "accepted")}
              title="Accept all products in this collection">
              Accept all
            </button>
          </div>
        </div>

        {/* Horizontal scroll of small product cards */}
        <div className="overflow-x-auto -mx-2 px-2">
          <div className="flex gap-3 pb-2">
            {(col.skus || []).map((s) => {
              const sku = String(s?.sku || "");
              const prod = productBySku[sku];
              // Fallback stub object for card until product arrives
              const stub = prod || { id: `sku:${sku}`, sku, title: sku, characteristics: { full: [] } };
              return (
                <div key={`${col.collection_hash}:${sku}`} className="w-[280px] shrink-0">
                  <TeaCard
                    product={stub}
                    size="small"
                    onOpen={() => openProductModal(prod || sku)}
                    onDelete={() => {
                      /* optional: allow delete product here if needed */
                    }}
                  />
                </div>
              );
            })}
            {(col.skus || []).length === 0 && <div className="text-sm text-gray-500 italic py-6">No products in this collection</div>}
          </div>
        </div>
      </div>
    );
  };

  return (
    <>
      <div className="space-y-6">
        <div className="bg-white rounded-lg shadow p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-lg font-semibold">Collections ({total})</h3>
            <button
              onClick={() => loadCollections().catch(() => {})}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2"
              title="Refresh results">
              <RxUpdate className="w-5 h-5" />
              Refresh
            </button>
          </div>

          <CollectionsFilter
            value={filter}
            onChange={(v) => {
              setFilter(v);
              setPage(1);
            }}
            onReset={resetFilters}
            loading={loading}
          />

          <div className="relative mt-4">
            {loading && (
              <div className="absolute inset-0 z-10 bg-white/60 backdrop-blur-[1px] flex items-center justify-center">
                <div className="loading-spinner" aria-label="Loading" />
              </div>
            )}

            <div className="space-y-4">
              {collections.map(renderCollectionRow)}
              {collections.length === 0 && !loading && (
                <div className="text-center py-12">
                  <div className="text-6xl mb-4">🧺</div>
                  <p className="text-gray-500">No collections found</p>
                </div>
              )}
            </div>
          </div>

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

CollectionsPage.propTypes = {
  api: PropTypes.object.isRequired, // axios instance
};
