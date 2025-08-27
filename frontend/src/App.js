// App.js
import React, { useCallback, useEffect, useState } from "react";
import axios from "axios";
import PropTypes from "prop-types";
import "./App.css";
import TasksPage from "./components/TasksPage";
import ProductsPage from "./components/ProductsPage";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || "";
const API = `${BACKEND_URL}/api/v1`;
const api = axios.create({ baseURL: API });

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
  return <span className={`px-2 py-1 rounded-full text-xs font-medium ${cls}`}>{status}</span>;
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

export default function App() {
  const [activeTab, setActiveTab] = useState("dashboard");
  const [tasks, setTasks] = useState([]);
  const [stats, setStats] = useState({});
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState("puer");
  const [categories, setCategories] = useState([]);

  // Храним последний набор фильтров Products (переживает переключение вкладок)
  const [lastProductsQuery, setLastProductsQuery] = useState(null);

  // Пишем в URL только после первичной инициализации
  const [didInitFromUrl, setDidInitFromUrl] = useState(false);

  // ---- URL -> state (читаем только tab)
  useEffect(() => {
    const sp = new URLSearchParams(window.location.search);
    const tab = sp.get("tab") || "dashboard";
    setActiveTab(tab);
    setDidInitFromUrl(true);
  }, []);

  // ---- state -> URL
  // Для вкладки products: оставляем product-параметры нетронутыми (ими управляет ProductsPage), но гарантируем tab=products.
  // Для остальных вкладок: в URL только ?tab=...
  useEffect(() => {
    if (!didInitFromUrl) return;
    const cur = `${window.location.pathname}${window.location.search}`;
    let next = cur;

    if (activeTab === "products") {
      const sp = new URLSearchParams(window.location.search);
      sp.set("tab", "products");
      next = `${window.location.pathname}?${sp.toString()}`;
    } else {
      const sp = new URLSearchParams();
      sp.set("tab", activeTab);
      next = `${window.location.pathname}?${sp.toString()}`;
    }

    if (next !== cur) window.history.replaceState(null, "", next);
  }, [activeTab, didInitFromUrl]);

  // Данные для дашборда/истории задач
  useEffect(() => {
    api.get("/stats").then((r) => setStats(r.data || {})).catch(() => {});
    api
      .get(`/tasks?limit=200`)
      .then((r) => setTasks(r.data?.items || r.data || []))
      .catch(() => {});
    api.get("/categories").then((r) => setCategories(r.data?.categories || [])).catch(() => {});
  }, []);

  // Переход из задач во вкладку products с предустановкой фильтров
  const openProductsForTask = useCallback((taskId, scope = "task") => {
    setLastProductsQuery((prev) => ({
      ...(prev || {}),
      mode: "byTask",
      taskId,
      scope: scope === "pipeline" ? "pipeline" : "task",
      page: 1,
    }));
    setActiveTab("products");
  }, []);

  const startScraping = useCallback(async () => {
    const term = String(searchTerm || "").trim();
    if (!term) return;
    setLoading(true);
    try {
      const res = await api.post(`/scrape/start?search_term=${encodeURIComponent(term)}`);
      window.alert(`Scraping started. Task ID: ${res.data?.task_id || "N/A"}`);
      api.get(`/tasks?limit=200`).then((r) => setTasks(r.data?.items || r.data || [])).catch(() => {});
      api.get("/stats").then((r) => setStats(r.data || {})).catch(() => {});
    } catch {
      window.alert("Failed to start scraping.");
    } finally {
      setLoading(false);
    }
  }, [searchTerm]);

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
          .join(","),
      )
      .join("\n");
    const blob = new Blob([`${header}\n${csvBody}`], { type: "text/csv;charset=utf-8" });
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
                <h1 className="text-xl font-bold text-gray-900">Chinese Tea Parser</h1>
                <p className="text-sm text-gray-600">Ozon.ru • Data collection</p>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <button onClick={exportCSV} className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors">
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
              <StatCard title="Products total" value={stats.total_products || 0} subtitle="In database" />
              <StatCard title="Active tasks" value={stats.running_tasks || 0} subtitle="In progress" />
              <StatCard title="Finished tasks" value={stats.finished_tasks || 0} subtitle="Succeeded" />
              <StatCard title="Error rate" value={`${Number(stats.error_rate || 0).toFixed(1)}%`} subtitle="All time" tone="red" />
            </div>

            {stats.total_products === 0 && (stats.total_tasks || 0) > 0 && (
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6">
                <div className="flex items-start">
                  <div className="text-yellow-600 text-2xl mr-4">⚠️</div>
                  <div>
                    <h3 className="text-lg font-semibold text-yellow-800 mb-2">Potential geo-blocking</h3>
                    <p className="text-yellow-700 mb-4">All scraping tasks finish with 0 products. Possible reasons:</p>
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
                  <div key={task.id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                    <div>
                      <p className="font-medium">{task.search_term}</p>
                      <p className="text-sm text-gray-500">{formatDate(task.created_at)}</p>
                      {task.error_message && <p className="text-sm text-red-600 mt-1">{task.error_message}</p>}
                    </div>
                    <div className="flex items-center space-x-3">
                      <span className="text-sm text-gray-600">{task.scraped_products || 0} items</span>
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
                <h4 className="font-medium text-blue-900 mb-2">Suggested queries</h4>
                <div className="flex flex-wrap gap-2">
                  {["puer", "sheng puer", "shu puer", "oolong", "chinese tea", "green tea"].map((term) => (
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
              <h3 className="text-lg font-semibold mb-4">Scraping statistics</h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="p-4 bg-green-50 rounded-lg">
                  <div className="text-green-600 text-sm font-medium">Captchas solved</div>
                  <div className="text-2xl font-bold text-green-900">{stats.captcha_solves || 0}</div>
                </div>
                <div className="p-4 bg-blue-50 rounded-lg">
                  <div className="text-blue-600 text-sm font-medium">Total tasks</div>
                  <div className="text-2xl font-bold text-blue-900">{stats.total_tasks || 0}</div>
                </div>
                <div className="p-4 bg-purple-50 rounded-lg">
                  <div className="text-purple-600 text-sm font-medium">Success rate</div>
                  <div className="text-2xl font-bold text-purple-900">
                    {stats.total_tasks > 0 ? ((Number(stats.finished_tasks || 0) / Number(stats.total_tasks || 1)) * 100).toFixed(1) : 0}%
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {activeTab === "products" && (
          <ProductsPage
            api={api}
            // сюда придут сохранённые фильтры при повторном заходе
            initialQuery={lastProductsQuery || undefined}
            // ProductsPage будет вызывать это при любом изменении фильтров/страницы/сортировки
            onPersist={setLastProductsQuery}
          />
        )}

        {activeTab === "tasks" && <TasksPage api={api} onOpenProducts={openProductsForTask} />}
      </main>
    </div>
  );
}
