import React, { useEffect, useState, useCallback } from "react";
import PropTypes from "prop-types";

// helpers
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

export default function DashboardPage({ api, onOpenProducts }) {
  const [stats, setStats] = useState({});
  const [tasks, setTasks] = useState([]);

  const fetchAll = useCallback(async () => {
    try {
      const [s, t] = await Promise.all([api.get("/stats"), api.get("/tasks?limit=200")]);
      setStats(s.data || {});
      setTasks(t.data?.items || t.data || []);
    } catch {
      /* noop */
    }
  }, [api]);

  useEffect(() => {
    fetchAll().catch(() => {});
  }, [fetchAll]);

  return (
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
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold">Recent tasks</h3>
          <button
            onClick={() => fetchAll().catch(() => {})}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            🔄 Refresh
          </button>
        </div>

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
                {onOpenProducts && (
                  <button
                    onClick={() => onOpenProducts(task.id, "task")}
                    className="text-sm px-3 py-1 rounded-lg bg-white border hover:bg-gray-100"
                    title="Open products for this task">
                    Open
                  </button>
                )}
              </div>
            </div>
          ))}
          {(!tasks || tasks.length === 0) && (
            <div className="text-center py-12">
              <div className="text-6xl mb-4">⚙️</div>
              <p className="text-gray-500">No tasks found</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

DashboardPage.propTypes = {
  api: PropTypes.object.isRequired, // axios instance
  onOpenProducts: PropTypes.func,
};
