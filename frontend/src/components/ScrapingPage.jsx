import React, { useCallback, useEffect, useState } from "react";
import PropTypes from "prop-types";

export default function ScrapingPage({ api }) {
  const [searchTerm, setSearchTerm] = useState("puer");
  const [loading, setLoading] = useState(false);
  const [stats, setStats] = useState({});

  const fetchStats = useCallback(async () => {
    try {
      const res = await api.get("/stats");
      setStats(res.data || {});
    } catch {
      /* noop */
    }
  }, [api]);

  useEffect(() => {
    fetchStats().catch(() => {});
  }, [fetchStats]);

  const startScraping = useCallback(async () => {
    const term = String(searchTerm || "").trim();
    if (!term) return;
    setLoading(true);
    try {
      const res = await api.post(`/scrape/start?search_term=${encodeURIComponent(term)}`);
      window.alert(`Scraping started. Task ID: ${res.data?.task_id || "N/A"}`);
      fetchStats().catch(() => {});
    } catch {
      window.alert("Failed to start scraping.");
    } finally {
      setLoading(false);
    }
  }, [api, searchTerm, fetchStats]);

  return (
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
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 transition-colors">
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
                className="px-3 py-1 bg-blue-100 text-blue-800 rounded-full text-sm hover:bg-blue-200 transition-colors">
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
  );
}

ScrapingPage.propTypes = {
  api: PropTypes.object.isRequired, // axios instance
};
