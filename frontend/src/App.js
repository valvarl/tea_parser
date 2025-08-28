// App.js
import React, { useCallback, useEffect, useState } from "react";
import axios from "axios";
import "./App.css";

// страницы
import DashboardPage from "./components/DashboardPage";
import ScrapingPage from "./components/ScrapingPage";
import ProductsPage from "./components/ProductsPage";
import CollectionsPage from "./components/CollectionsPage";
import TasksPage from "./components/TasksPage";

// --- API instance ---
const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || "";
const API = `${BACKEND_URL}/api/v1`;
const api = axios.create({ baseURL: API });

export default function App() {
  const [activeTab, setActiveTab] = useState("dashboard");

  // Храним последний запрос фильтров для Products — переживает переключение вкладок
  const [lastProductsQuery, setLastProductsQuery] = useState(null);

  // Пишем в URL только после первичной инициализации из URL
  const [didInitFromUrl, setDidInitFromUrl] = useState(false);

  // ---- URL -> state (читаем только tab)
  useEffect(() => {
    const sp = new URLSearchParams(window.location.search);
    const tab = sp.get("tab") || "dashboard";
    setActiveTab(tab);
    setDidInitFromUrl(true);
  }, []);

  // ---- state -> URL
  // Для вкладки products: сохраняем существующие product-параметры (ими управляет ProductsPage), но гарантируем tab=products.
  // Для остальных вкладок: в URL оставляем ТОЛЬКО ?tab=...
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

  // Deep-link из задач во вкладку products — сразу кладём ключевые product-параметры.
  // Остальные (limit/sort/q/filters) восстановит ProductsPage из lastProductsQuery или URL.
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

  // Экспорт CSV из шапки
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
              { id: "collections", label: "Collections", icon: "🧺" },
              { id: "tasks", label: "Tasks", icon: "⚙️" },
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center space-x-2 py-4 px-2 border-b-2 font-medium text-sm ${
                  activeTab === tab.id
                    ? "border-blue-500 text-blue-600"
                    : "border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300"
                }`}>
                <span>{tab.icon}</span>
                <span>{tab.label}</span>
              </button>
            ))}
          </div>
        </div>
      </nav>

      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {activeTab === "dashboard" && <DashboardPage api={api} onOpenProducts={openProductsForTask} />}

        {activeTab === "scraping" && <ScrapingPage api={api} />}

        {activeTab === "products" && (
          <ProductsPage
            api={api}
            // сюда попадут сохранённые фильтры при повторном заходе
            initialQuery={lastProductsQuery || undefined}
            // ProductsPage будет звать это при любом изменении фильтров/страницы/сортировки
            onPersist={setLastProductsQuery}
          />
        )}

        {activeTab === "collections" && <CollectionsPage api={api} />}

        {activeTab === "tasks" && <TasksPage api={api} onOpenProducts={openProductsForTask} />}
      </main>
    </div>
  );
}
