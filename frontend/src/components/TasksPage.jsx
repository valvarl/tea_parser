// components/TasksPage.jsx
import React, { useCallback, useEffect, useState } from "react";
import PropTypes from "prop-types";
import TaskItem from "./TaskItem";

export default function TasksPage({ api, onOpenProducts }) {
  const [tasks, setTasks] = useState([]);
  const [tasksParentFilter, setTasksParentFilter] = useState("");
  const [loading, setLoading] = useState(false);

  // --- URL <-> state (только для tasks_parent_id)
  const parseUrlToState = useCallback(() => {
    const sp = new URLSearchParams(window.location.search);
    const parent = sp.get("tasks_parent_id") || "";
    setTasksParentFilter(parent);
  }, []);

  const writeStateToUrl = useCallback(() => {
    const sp = new URLSearchParams(window.location.search);
    if (tasksParentFilter) sp.set("tasks_parent_id", tasksParentFilter);
    else sp.delete("tasks_parent_id");
    const next = `${window.location.pathname}?${sp.toString()}`;
    if (next !== `${window.location.pathname}${window.location.search}`) {
      window.history.replaceState(null, "", next);
    }
  }, [tasksParentFilter]);

  const fetchTasks = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (tasksParentFilter) params.set("parent_task_id", tasksParentFilter);
      params.set("limit", "200");
      const res = await api.get(`/tasks?${params.toString()}`);
      const items = res.data?.items || res.data || [];
      setTasks(items);
    } finally {
      setLoading(false);
    }
  }, [api, tasksParentFilter]);

  const openChildrenTasks = useCallback((taskId) => {
    setTasksParentFilter(taskId);
  }, []);

  const clearParent = useCallback(() => {
    setTasksParentFilter("");
  }, []);

  // init
  useEffect(() => {
    parseUrlToState();
  }, [parseUrlToState]);

  // keep URL in sync with local state
  useEffect(() => {
    writeStateToUrl();
  }, [writeStateToUrl]);

  // load data
  useEffect(() => {
    fetchTasks().catch(() => {});
  }, [fetchTasks]);

  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold">Task history</h3>
          <div className="flex items-center gap-2">
            {tasksParentFilter && (
              <button
                onClick={clearParent}
                className="px-3 py-1.5 bg-white border rounded-lg hover:bg-gray-50"
                title="Clear parent filter"
              >
                ✖ Clear parent
              </button>
            )}
            <button
              onClick={() => fetchTasks().catch(() => {})}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
            >
              🔄 Refresh
            </button>
          </div>
        </div>

        {tasksParentFilter && (
          <div className="mb-3 text-sm text-gray-600">
            Parent filter: <code className="bg-gray-100 px-1.5 py-0.5 rounded">{tasksParentFilter}</code>
          </div>
        )}

        {loading && (
          <div className="py-6 text-center text-gray-500">Loading…</div>
        )}

        <div className="space-y-3">
          {tasks.map((t) => (
            <TaskItem
              key={t.id}
              task={t}
              onOpenProducts={onOpenProducts}
              onOpenChildren={openChildrenTasks}
            />
          ))}
          {!loading && tasks.length === 0 && (
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

TasksPage.propTypes = {
  api: PropTypes.object.isRequired,                // axios instance
  onOpenProducts: PropTypes.func.isRequired,       // (taskId, scope) => void
};
