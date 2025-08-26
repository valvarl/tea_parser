import React, { useCallback, useEffect, useMemo, useState } from "react";
import axios from "axios";
import PropTypes from "prop-types";
import "./App.css";
import ProductModal from "./components/ProductModal";
import TaskItem from "./components/TaskItem";
import ProductsFilter from "./components/ProductsFilter";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || "";
const API = `${BACKEND_URL}/api/v1`;
const api = axios.create({ baseURL: API });

const DateLike = PropTypes.oneOfType([
  PropTypes.string,
  PropTypes.number,
  PropTypes.instanceOf(Date),
]);

const ProductPropType = PropTypes.shape({
  id: PropTypes.string.isRequired,
  title: PropTypes.string,
  name: PropTypes.string,
  sku: PropTypes.string,
  created_at: DateLike,
  updated_at: DateLike,
  scraped_at: DateLike,
  cover_image: PropTypes.string,
  description: PropTypes.shape({
    content_blocks: PropTypes.arrayOf(
      PropTypes.shape({
        img: PropTypes.shape({ alt: PropTypes.string, src: PropTypes.string }),
      })
    ),
  }),
  gallery: PropTypes.object,
  characteristics: PropTypes.shape({
    full: PropTypes.arrayOf(
      PropTypes.shape({
        id: PropTypes.string,
        title: PropTypes.string,
        values: PropTypes.arrayOf(PropTypes.string),
      })
    ),
  }),
});

function formatDate(input) {
  if (!input) return "N/A";
  const d = new Date(input);
  if (Number.isNaN(d.getTime())) return "N/A";
  return d.toLocaleString("en-GB");
}

function StatusBadge({ status }) {
  const cls =
    status === "finished"
      ? "bg-green-100 text-green-800"
      : status === "running"
        ? "bg-blue-100 text-blue-800"
        : status === "failed"
          ? "bg-red-100 text-red-800"
          : status === "pending"
            ? "bg-yellow-100 text-yellow-800"
            : "bg-gray-100 text-gray-800";
  return (
    <span className={`px-2 py-1 rounded-full text-xs font-medium ${cls}`}>
      {status}
    </span>
  );
}
StatusBadge.propTypes = { status: PropTypes.string };

function StatCard({ title, value, subtitle, tone = "blue" }) {
  const toneMap = {
    blue: "border-blue-500",
    red: "border-red-500",
    green: "border-green-500",
    purple: "border-purple-500",
    yellow: "border-yellow-500",
  };
  const border = toneMap[tone] || toneMap.blue;
  return (
    <div className={`bg-white rounded-lg shadow-md p-6 border-l-4 ${border}`}>
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium text-gray-600">{title}</p>
          <p className="text-2xl font-bold text-gray-900">{value}</p>
          {subtitle && <p className="text-sm text-gray-500">{subtitle}</p>}
        </div>
        <div className="text-3xl">📊</div>
      </div>
    </div>
  );
}
StatCard.propTypes = {
  title: PropTypes.string.isRequired,
  value: PropTypes.oneOfType([PropTypes.string, PropTypes.number]).isRequired,
  subtitle: PropTypes.string,
  tone: PropTypes.oneOf(["blue", "red", "green", "purple", "yellow"]),
};

function pickCover(product) {
  if (product?.cover_image) return product.cover_image;
  const blocks = product?.description?.content_blocks || [];
  for (const b of blocks) {
    if (b?.img?.src) return b.img.src;
  }
  const g = product?.gallery;
  if (g?.images?.length) return g.images[0];
  return null;
}

function extractSpecs(product) {
  const list = product?.characteristics?.full || [];
  const byId = Object.fromEntries(list.map((x) => [x.id, x]));
  const byTitle = Object.fromEntries(
    list.map((x) => [String(x.title || "").toLowerCase(), x])
  );
  const pick = (keyId, keyTitle) => {
    const item = byId[keyId] || byTitle[keyTitle];
    return item?.values?.filter(Boolean)?.join(", ");
  };
  return [
    { label: "Type", val: pick("TeaType", "вид чая") },
    { label: "Weight, g", val: pick("Weight", "вес товара, г") },
    { label: "Taste", val: pick("TeaTaste", "вкус") },
    { label: "Country", val: pick("Country", "страна-изготовитель") },
    {
      label: "Form",
      val: pick("VarietyTeaShape", "форма чая") || pick("Type", "тип"),
    },
  ].filter((x) => x.val);
}

function TeaCard({ product, onDelete, onOpen }) {
  const img = pickCover(product);
  const title = product.title || product.name || product.sku || "Untitled";
  const specs = extractSpecs(product);
  const ts = product.updated_at || product.created_at || product.scraped_at;

  return (
    <div
      className="bg-white rounded-lg shadow-md p-4 hover:shadow-lg transition-shadow cursor-pointer relative" // cursor + relative
      onClick={onOpen} // Открытие модалки при клике на карточку
    >
      {/* Крестик удаления: закрепляем сверху, выше клика по карточке */}
      <button
        onClick={(e) => {
          e.stopPropagation(); // НЕ открывать модалку
          onDelete(product.id);
        }}
        className="absolute top-2 right-2 z-10 text-red-500 hover:text-red-700 text-sm bg-white/90 rounded-full px-2 py-1 shadow"
        title="Delete"
        aria-label="Delete"
      >
        ❌
      </button>

      <div className="flex justify-between items-start mb-3 pr-8">
        <h3 className="text-base font-semibold text-gray-900 line-clamp-2">
          {title}
        </h3>
      </div>

      {img ? (
        <img
          src={img}
          alt={title}
          className="w-full h-44 object-cover rounded-lg mb-3"
        />
      ) : (
        <div className="w-full h-44 rounded-lg mb-3 bg-gray-100 flex items-center justify-center text-4xl">
          🍵
        </div>
      )}

      <div className="space-y-1 text-sm">
        {specs.map((s) => (
          <div key={s.label} className="flex justify-between">
            <span className="text-gray-600">{s.label}:</span>
            <span className="font-medium text-gray-900 text-right">
              {s.val}
            </span>
          </div>
        ))}
        <div className="flex justify-between text-xs pt-1 text-gray-500">
          <span>Updated:</span>
          <span>{formatDate(ts)}</span>
        </div>
      </div>
    </div>
  );
}
TeaCard.propTypes = {
  product: ProductPropType.isRequired,
  onDelete: PropTypes.func.isRequired,
  onOpen: PropTypes.func.isRequired, // добавили
};

function Pagination({
  page,
  totalPages,
  pageSize,
  onPageChange,
  onPageSizeChange,
}) {
  return (
    <div className="flex items-center justify-between mt-6">
      <div className="flex items-center space-x-2">
        <button
          onClick={() => onPageChange(Math.max(1, page - 1))}
          disabled={page === 1}
          className="px-3 py-1 bg-white border rounded-lg disabled:opacity-50"
        >
          ← Prev
        </button>
        <span className="text-sm text-gray-600">
          Page <strong>{page}</strong> of <strong>{totalPages}</strong>
        </span>
        <button
          onClick={() => onPageChange(Math.min(totalPages, page + 1))}
          disabled={page >= totalPages}
          className="px-3 py-1 bg-white border rounded-lg disabled:opacity-50"
        >
          Next →
        </button>
      </div>

      <div className="flex items-center space-x-2">
        <span className="text-sm text-gray-600">Per page:</span>
        <select
          value={pageSize}
          onChange={(e) => onPageSizeChange(parseInt(e.target.value, 10))}
          className="px-2 py-1 border rounded-lg"
        >
          {[12, 24, 48, 96].map((sz) => (
            <option key={sz} value={sz}>
              {sz}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}
Pagination.propTypes = {
  page: PropTypes.number.isRequired,
  totalPages: PropTypes.number.isRequired,
  pageSize: PropTypes.number.isRequired,
  onPageChange: PropTypes.func.isRequired,
  onPageSizeChange: PropTypes.func.isRequired,
};

export default function App() {
  const [activeTab, setActiveTab] = useState("dashboard");
  const [products, setProducts] = useState([]);
  const [totalProducts, setTotalProducts] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(24);
  // const [selectedTeaType, setSelectedTeaType] = useState("");
  const [tasks, setTasks] = useState([]);
  const [tasksParentFilter, setTasksParentFilter] = useState("");
  const [productsMode, setProductsMode] = useState("all"); // all | byTask
  const [productsTaskId, setProductsTaskId] = useState("");
  const [productsScope, setProductsScope] = useState("task"); // task | pipeline
  const [stats, setStats] = useState({});
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState("puer");
  const [categories, setCategories] = useState([]);
  const [isProductModalOpen, setIsProductModalOpen] = useState(false);
  const [modalProduct, setModalProduct] = useState(null);

  // фильтр: раздельно для "all" и для "byTask" по ключу taskId::scope
  const DEFAULT_FILTER = useMemo(() => ({
    q: "",
    sort_by: "updated_at",
    sort_dir: "desc",
    filters: {}, // Record<charId, string[]>
  }), []);
  const [filterAll, setFilterAll] = useState({ ...DEFAULT_FILTER });
  const [filterByTask, setFilterByTask] = useState({}); // { [key]: FilterValue }
  const filterKey = useMemo(
    () => (productsMode === "byTask" ? `${productsTaskId || ""}::${productsScope}` : "all"),
    [productsMode, productsTaskId, productsScope]
  );
  const currentFilter = useMemo(
    () => (productsMode === "byTask" ? (filterByTask[filterKey] || DEFAULT_FILTER) : filterAll),
    [productsMode, filterByTask, filterKey, filterAll, DEFAULT_FILTER]
  );
  // характеристики для фильтра (кеш)
  const [charAll, setCharAll] = useState([]);
  const [charByTask, setCharByTask] = useState({}); // { [key]: Characteristic[] }
  const currentCharacteristics = useMemo(
    () => (productsMode === "byTask" ? (charByTask[filterKey] || []) : charAll),
    [productsMode, charByTask, filterKey, charAll]
  );

  const openProductModal = useCallback(
    (prodOrSku) => {
      if (!prodOrSku) return;
      // Если пришёл объект — открываем сразу
      if (typeof prodOrSku === "object") {
        setModalProduct(prodOrSku);
        setIsProductModalOpen(true);
        return;
      }
      // Если пришёл sku — попробуем найти в текущей странице
      const found = products.find((p) => String(p.sku) === String(prodOrSku));
      if (found) {
        setModalProduct(found);
        setIsProductModalOpen(true);
        return;
      }
      // Фоллбек: получить по SKU с бэка (если эндпоинт есть)
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
    [products]
  );

  const closeProductModal = useCallback(() => {
    setIsProductModalOpen(false);
    setModalProduct(null);
  }, []);

  const totalPages = useMemo(
    () => Math.max(1, Math.ceil(totalProducts / pageSize)),
    [totalProducts, pageSize]
  );

  const fetchStats = useCallback(async () => {
    const res = await api.get("/stats");
    setStats(res.data || {});
  }, []);

  const fetchTasks = useCallback(async () => {
    const params = new URLSearchParams();
    if (tasksParentFilter) params.set("parent_task_id", tasksParentFilter);
    params.set("limit", "200");
    const res = await api.get(`/tasks?${params.toString()}`);
    const items = res.data?.items || res.data || [];
    setTasks(items);
  }, [tasksParentFilter]);

  const fetchCategories = useCallback(async () => {
    const res = await api.get("/categories");
    setCategories(res.data?.categories || []);
  }, []);

  const fetchProducts = useCallback(async () => {
    // режим: товары одной задачи
    if (productsMode === "byTask" && productsTaskId) {
      const params = new URLSearchParams();
      params.set("limit", String(pageSize));
      params.set("skip", String((page - 1) * pageSize));
      params.set("scope", productsScope); // 'task' | 'pipeline'
      // поиск/сорт/фильтры
      if (currentFilter.q) params.set("q", currentFilter.q);
      params.set("sort_by", currentFilter.sort_by || "updated_at");
      params.set("sort_dir", currentFilter.sort_dir || "desc");
      const filters = currentFilter.filters || {};
      Object.entries(filters).forEach(([cid, vals]) => {
        (vals || []).forEach(v => params.append(`char_${cid}`, v));
      });
      const res = await api.get(`/tasks/${encodeURIComponent(productsTaskId)}/products?${params.toString()}`);
      setProducts(res.data?.items || []);
      setTotalProducts(res.data?.total || 0);
      return;
    }
    // режим: общий список
    const params = new URLSearchParams();
    params.set("limit", String(pageSize));
    params.set("skip", String((page - 1) * pageSize));
    if (currentFilter.q) params.set("q", currentFilter.q);
    params.set("sort_by", currentFilter.sort_by || "updated_at");
    params.set("sort_dir", currentFilter.sort_dir || "desc");
    const filters = currentFilter.filters || {};
    Object.entries(filters).forEach(([cid, vals]) => {
      (vals || []).forEach(v => params.append(`char_${cid}`, v));
    });
    const res = await api.get(`/products?${params.toString()}`);
    setProducts(res.data?.items || []);
    setTotalProducts(res.data?.total || 0);
  }, [page, pageSize, productsMode, productsTaskId, productsScope, currentFilter]);

  const fetchFilterCharacteristics = useCallback(async () => {
    try {
      if (productsMode === "byTask" && productsTaskId) {
        const url = `/tasks/${encodeURIComponent(productsTaskId)}/products/characteristics?scope=${productsScope}`;
        const res = await api.get(url);
        const arr = Array.isArray(res.data) ? res.data : [];
        setCharByTask(prev => ({ ...prev, [filterKey]: arr }));
      } else {
        const res = await api.get("/products/characteristics");
        setCharAll(Array.isArray(res.data) ? res.data : res.data?.items || []);
      }
    } catch { /* noop */ }
  }, [productsMode, productsTaskId, productsScope, filterKey]);

  const handleFilterChange = React.useCallback((next) => {
    setPage(1);
    if (productsMode === "byTask") {
      setFilterByTask(prev => ({ ...prev, [filterKey]: next }));
    } else {
      setFilterAll(next);
    }
  }, [productsMode, filterKey]);

  const handleFilterReset = React.useCallback(() => {
    setPage(1);
    if (productsMode === "byTask") {
      setFilterByTask(prev => ({ ...prev, [filterKey]: { ...DEFAULT_FILTER } }));
    } else {
      setFilterAll({ ...DEFAULT_FILTER });
    }
  }, [productsMode, filterKey, DEFAULT_FILTER]);

  const openProductsForTask = useCallback((taskId, scope = "task") => {
    setProductsMode("byTask");
    setProductsTaskId(taskId);
    setProductsScope(scope);
    setActiveTab("products");
    setPage(1);
  }, []);

  const openChildrenTasks = useCallback((taskId) => {
    setTasksParentFilter(taskId);
    setActiveTab("tasks");
  }, []);

  useEffect(() => {
    fetchStats().catch(() => {});
    fetchTasks().catch(() => {});
    fetchCategories().catch(() => {}); // (если ещё нужно для других мест)
  }, [fetchStats, fetchTasks, fetchCategories]);

  useEffect(() => {
    fetchProducts().catch(() => {});
    fetchFilterCharacteristics().catch(() => {});
 }, [fetchProducts, fetchFilterCharacteristics]);

  const startScraping = useCallback(async () => {
    const term = String(searchTerm || "").trim();
    if (!term) return;
    setLoading(true);
    try {
      const res = await api.post(
        `/scrape/start?search_term=${encodeURIComponent(term)}`
      );
      window.alert(`Scraping started. Task ID: ${res.data?.task_id || "N/A"}`);
      fetchTasks().catch(() => {});
      fetchStats().catch(() => {});
    } catch {
      window.alert("Failed to start scraping.");
    } finally {
      setLoading(false);
    }
  }, [searchTerm, fetchStats, fetchTasks]);

  const deleteProduct = useCallback(
    async (productId) => {
      if (!window.confirm("Delete this product?")) return;
      await api.delete(`/products/${productId}`);
      fetchProducts().catch(() => {});
      fetchStats().catch(() => {});
    },
    [fetchProducts, fetchStats]
  );

  const exportCSV = useCallback(async () => {
    const res = await api.get("/export/csv");
    const rows = Array.isArray(res.data?.data) ? res.data.data : [];
    const header = "ID,Name,Price,Rating,Type,Region,Pressed,Scraped";
    const csvBody = rows
      .map((r) =>
        [
          r.id ?? "",
          r.name ?? "",
          r.price ?? "",
          r.rating ?? "",
          r.tea_type ?? "",
          r.tea_region ?? "",
          r.is_pressed ?? "",
          r.scraped_at ?? "",
        ]
          .map((v) => `"${String(v).replace(/"/g, '""')}"`)
          .join(",")
      )
      .join("\n");
    const blob = new Blob([`${header}\n${csvBody}`], {
      type: "text/csv;charset=utf-8",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "products.csv";
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
  }, []);

  return (
    <div className="min-h-screen bg-gray-50">
      <header className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-4">
            <div className="flex items-center space-x-3">
              <div className="text-2xl">🍃</div>
              <div>
                <h1 className="text-xl font-bold text-gray-900">
                  Chinese Tea Parser
                </h1>
                <p className="text-sm text-gray-600">
                  Ozon.ru • Data collection
                </p>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <button
                onClick={exportCSV}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
              >
                📥 Export CSV
              </button>
            </div>
          </div>
        </div>
      </header>

      <nav className="bg-white border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex space-x-8">
            {[
              { id: "dashboard", label: "Dashboard", icon: "📊" },
              { id: "scraping", label: "Scraping", icon: "🔍" },
              { id: "products", label: "Products", icon: "🍃" },
              { id: "tasks", label: "Tasks", icon: "⚙️" },
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center space-x-2 py-4 px-2 border-b-2 font-medium text-sm ${
                  activeTab === tab.id
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
                }`}
              >
                <span>{tab.icon}</span>
                <span>{tab.label}</span>
              </button>
            ))}
          </div>
        </div>
      </nav>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {activeTab === "dashboard" && (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              <StatCard
                title="Products total"
                value={stats.total_products || 0}
                subtitle="In database"
              />
              <StatCard
                title="Active tasks"
                value={stats.running_tasks || 0}
                subtitle="In progress"
              />
              <StatCard
                title="Finished tasks"
                value={stats.finished_tasks || 0}
                subtitle="Succeeded"
              />
              <StatCard
                title="Error rate"
                value={`${Number(stats.error_rate || 0).toFixed(1)}%`}
                subtitle="All time"
                tone="red"
              />
            </div>

            {stats.total_products === 0 && (stats.total_tasks || 0) > 0 && (
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6">
                <div className="flex items-start">
                  <div className="text-yellow-600 text-2xl mr-4">⚠️</div>
                  <div>
                    <h3 className="text-lg font-semibold text-yellow-800 mb-2">
                      Potential geo-blocking
                    </h3>
                    <p className="text-yellow-700 mb-4">
                      All scraping tasks finish with 0 products. Possible
                      reasons:
                    </p>
                    <ul className="text-yellow-700 space-y-1 mb-4 list-disc list-inside">
                      <li>Geo restrictions for non-Russian IPs</li>
                      <li>Missing tokens or region configuration</li>
                      <li>Cookie ozon_regions is not set</li>
                    </ul>
                    <div className="bg-yellow-100 p-3 rounded-lg text-sm text-yellow-800">
                      Use Russian proxies and set a valid Ozon region cookie.
                    </div>
                  </div>
                </div>
              </div>
            )}

            <div className="bg-white rounded-lg shadow-md p-6">
              <h3 className="text-lg font-semibold mb-4">Recent tasks</h3>
              <div className="space-y-3">
                {(tasks || []).slice(0, 5).map((task) => (
                  <div
                    key={task.id}
                    className="flex items-center justify-between p-3 bg-gray-50 rounded-lg"
                  >
                    <div>
                      <p className="font-medium">{task.search_term}</p>
                      <p className="text-sm text-gray-500">
                        {formatDate(task.created_at)}
                      </p>
                      {task.error_message && (
                        <p className="text-sm text-red-600 mt-1">
                          {task.error_message}
                        </p>
                      )}
                    </div>
                    <div className="flex items-center space-x-3">
                      <span className="text-sm text-gray-600">
                        {task.scraped_products || 0} items
                      </span>
                      <StatusBadge status={task.status} />
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {activeTab === "scraping" && (
          <div className="space-y-6">
            <div className="bg-white rounded-lg shadow-md p-6">
              <h3 className="text-lg font-semibold mb-4">Start scraping</h3>
              <div className="flex items-center space-x-4">
                <input
                  type="text"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  placeholder="Enter a search query (e.g. 'puer')"
                  className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
                <button
                  onClick={startScraping}
                  disabled={loading}
                  className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 transition-colors"
                >
                  {loading ? "🔄 Starting..." : "🚀 Start"}
                </button>
              </div>

              <div className="mt-4 p-4 bg-blue-50 rounded-lg">
                <h4 className="font-medium text-blue-900 mb-2">
                  Suggested queries
                </h4>
                <div className="flex flex-wrap gap-2">
                  {[
                    "puer",
                    "sheng puer",
                    "shu puer",
                    "oolong",
                    "chinese tea",
                    "green tea",
                  ].map((term) => (
                    <button
                      key={term}
                      onClick={() => setSearchTerm(term)}
                      className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm hover:bg-blue-200 transition-colors"
                    >
                      {term}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            <div className="bg-white rounded-lg shadow-md p-6">
              <h3 className="text-lg font-semibold mb-4">
                Scraping statistics
              </h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="p-4 bg-green-50 rounded-lg">
                  <div className="text-green-600 text-sm font-medium">
                    Captchas solved
                  </div>
                  <div className="text-2xl font-bold text-green-900">
                    {stats.captcha_solves || 0}
                  </div>
                </div>
                <div className="p-4 bg-blue-50 rounded-lg">
                  <div className="text-blue-600 text-sm font-medium">
                    Total tasks
                  </div>
                  <div className="text-2xl font-bold text-blue-900">
                    {stats.total_tasks || 0}
                  </div>
                </div>
                <div className="p-4 bg-purple-50 rounded-lg">
                  <div className="text-purple-600 text-sm font-medium">
                    Success rate
                  </div>
                  <div className="text-2xl font-bold text-purple-900">
                    {stats.total_tasks > 0
                      ? (
                          (Number(stats.finished_tasks || 0) /
                            Number(stats.total_tasks || 1)) *
                          100
                        ).toFixed(1)
                      : 0}
                    %
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {activeTab === "products" && (
          <div className="space-y-6">
            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold">
                  Products ({totalProducts})
                </h3>
                <button
                  onClick={() => { setPage(1); fetchProducts().catch(() => {}); }}
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                >
                  🔄 Refresh
                </button>
              </div>

              {/* ФИЛЬТР */}
              <ProductsFilter
                mode={productsMode}
                taskId={productsTaskId}
                scope={productsScope}
                onScopeChange={(sc) => setProductsScope(sc)}
                characteristics={currentCharacteristics}
                value={currentFilter}
                onChange={handleFilterChange}
                onReset={handleFilterReset}
                loading={loading}
              />

              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {products.map((p) => (
                  <TeaCard
                    key={p.id}
                    product={p}
                    onDelete={deleteProduct}
                    onOpen={() => openProductModal(p)}
                  />
                ))}
              </div>

              {products.length === 0 && (
                <div className="text-center py-12">
                  <div className="text-6xl mb-4">🍃</div>
                  <p className="text-gray-500">No products found</p>
                  <p className="text-sm text-gray-400 mt-2">
                    Start scraping to populate the list
                  </p>
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
        )}

        {activeTab === "tasks" && (
          <div className="space-y-6">
            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold">Task history</h3>
                <button
                  onClick={() => fetchTasks().catch(() => {})}
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                >
                  🔄 Refresh
                </button>
              </div>

              <div className="space-y-3">
                {tasks.map((t) => (
                  <TaskItem
                    key={t.id}
                    task={t}
                    onOpenProducts={openProductsForTask}
                    onOpenChildren={openChildrenTasks}
                  />
                ))}
                {tasks.length === 0 && (
                  <div className="text-center py-12">
                    <div className="text-6xl mb-4">⚙️</div>
                    <p className="text-gray-500">No tasks found</p>
                  </div>
                )}
              </div>
            </div>
          </div>
        )}
      <ProductModal
        isOpen={isProductModalOpen}
        product={modalProduct || {}}
        onClose={closeProductModal}
        onSelectSku={(sku) => openProductModal(sku)}
      />
      </main>
    </div>
  );
}
