import React, { useCallback, useMemo, useState } from "react";
import PropTypes from "prop-types";
import axios from "axios";

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL || "";
const API = `${BACKEND_URL}/api/v1`;
const api = axios.create({ baseURL: API });

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

function formatDate(input) {
  if (!input) return "—";
  const d = new Date(input);
  if (Number.isNaN(d.getTime())) return "—";
  return d.toLocaleString("en-GB");
}

function makeTitle(t) {
  if (t?.title) return t.title;
  const tt = String(t?.task_type || "task").toLowerCase();
  if (tt === "indexing") {
    const term = (t?.params?.search_term || "—").trim();
    const cat = (t?.params?.category_id || "—").trim();
    const pages = t?.params?.max_pages ?? 0;
    return `Индексирование: «${term}» • категория ${cat} • ${pages} стр.`;
  }
  if (tt === "collections") {
    const base = (t?.params?.source_task_id || "—").trim();
    return `Коллекции (из задачи ${base})`;
  }
  if (tt === "enriching") return "Обогащение SKU (пакетная обработка)";
  return `${tt[0]?.toUpperCase() || ""}${tt.slice(1)} task`;
}

export default function TaskItem({
  task,
  onOpenProducts,  // (taskId: string, scope?: 'task'|'pipeline')
  onOpenChildren,  // (taskId: string)
}) {
  const [expanded, setExpanded] = useState(false);
  const [details, setDetails] = useState(null);
  const [loading, setLoading] = useState(false);
  const [fixing, setFixing] = useState(false);

  const title = useMemo(() => makeTitle(task), [task]);

  // прогресс обогащения: берём с сервера (enrich_progress), если нет — считаем из stats
  const progress = useMemo(() => {
    const ep = task?.enrich_progress || {};
    const stats = task?.stats || {};
    const fallbackEnriched =
      Number(stats?.enrich?.inserted || 0) ||
      Math.max(0, Number(stats?.enrich?.processed || 0) - Number(stats?.enrich?.dlq || 0));
    const fallbackToEnrich =
      Number(stats?.forwarded?.to_enricher || 0) ||
      Number(stats?.collections?.skus_to_process || 0) ||
      Number(stats?.index?.inserted || 0);
    const failed = Number(ep.failed ?? stats?.enrich?.dlq ?? 0);
    return {
      enriched: Number(ep.enriched ?? fallbackEnriched),
      to_enrich: Number(ep.to_enrich ?? fallbackToEnrich),
      failed,
    };
  }, [task]);

  const toggleExpand = useCallback(async (e) => {
    e.stopPropagation();
    if (!expanded) {
      try {
        setLoading(true);
        const res = await api.get(`/tasks/${encodeURIComponent(task.id)}/details`);
        setDetails(res.data || {});
      } finally {
        setLoading(false);
      }
    }
    setExpanded((v) => !v);
  }, [expanded, task?.id]);

  const fixErrors = useCallback(async (e) => {
    e.stopPropagation();
    try {
      setFixing(true);
      await api.post(`/tasks/${encodeURIComponent(task.id)}/fix_errors`, {
        mode: "retry_failed",
        limit: 500,
      });
      window.alert("Fix queued ✅");
    } catch {
      window.alert("Failed to queue fix ❌");
    } finally {
      setFixing(false);
    }
  }, [task?.id]);

  return (
    <div
      className="bg-white border rounded-lg shadow-sm hover:shadow-md transition-shadow cursor-pointer"
      onClick={() => onOpenProducts?.(task.id, "task")}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") onOpenProducts?.(task.id, "task");
      }}
    >
      {/* Верхняя полоса */}
      <div className="flex items-start justify-between p-4">
        <div className="min-w-0 pr-4">
          <div className="flex items-center gap-2">
            <StatusBadge status={task.status} />
            <span className="text-xs text-gray-500">
              {formatDate(task.created_at)} → {formatDate(task.finished_at)}
            </span>
          </div>
          <h3 className="mt-1 text-sm sm:text-base font-semibold text-gray-900 truncate">
            {title}
          </h3>

          <div className="mt-2 flex items-center gap-3 text-sm">
            <span className="px-2 py-1 rounded bg-blue-50 text-blue-700">
              Обогащено: <b>{progress.enriched}</b>
            </span>
            <span className="px-2 py-1 rounded bg-gray-50 text-gray-700">
              Требуется/Найдено: <b>{progress.to_enrich}</b>
            </span>
            {!!progress.failed && (
              <span className="px-2 py-1 rounded bg-red-50 text-red-700">
                Ошибок: <b>{progress.failed}</b>
              </span>
            )}
          </div>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          <button
            className="px-3 py-1.5 text-sm rounded-lg bg-white border hover:bg-gray-50"
            onClick={(e) => {
              e.stopPropagation();
              onOpenChildren?.(task.id);
            }}
            title="Показать дочерние задачи"
          >
            👶 Children
          </button>
          <button
            className="px-3 py-1.5 text-sm rounded-lg bg-white border hover:bg-gray-50"
            onClick={fixErrors}
            disabled={fixing}
            title="Попытаться исправить ошибки задачи"
          >
            {fixing ? "⏳ Fixing..." : "🛠 Fix errors"}
          </button>
          <button
            className={`px-3 py-1.5 text-sm rounded-lg border ${
              expanded ? "bg-gray-100" : "bg-white hover:bg-gray-50"
            }`}
            onClick={toggleExpand}
            title="Показать детали (выезжает снизу)"
          >
            {expanded ? "▾ Свернуть" : "▸ Подробнее"}
          </button>
        </div>
      </div>

      {/* Раскрывающийся блок (выезжает снизу) */}
      <div
        className={`transition-all duration-300 ease-in-out overflow-hidden ${
          expanded ? "max-h-[1200px] opacity-100" : "max-h-0 opacity-70"
        }`}
      >
        <div className="px-4 pb-4 border-t bg-gray-50/60">
          {loading ? (
            <div className="py-6 text-sm text-gray-500">Загружаю детали…</div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 py-4">
              {/* INDEXER */}
              <div className="bg-white rounded-lg border p-4">
                <div className="flex items-center justify-between mb-2">
                  <h4 className="font-semibold text-gray-900">Indexer</h4>
                  <span className="text-xs text-gray-500">
                    {details?.workers?.indexer?.status || "—"}
                  </span>
                </div>
                <div className="text-sm text-gray-700 space-y-1">
                  <div>
                    indexed:{" "}
                    <b>
                      {details?.result?.indexer?.indexed ??
                        details?.stats?.index?.indexed ??
                        "—"}
                    </b>
                  </div>
                  <div>
                    inserted:{" "}
                    <b>
                      {details?.result?.indexer?.inserted ??
                        details?.stats?.index?.inserted ??
                        "—"}
                    </b>
                  </div>
                  <div>
                    updated:{" "}
                    <b>
                      {details?.result?.indexer?.updated ??
                        details?.stats?.index?.updated ??
                        "—"}
                    </b>
                  </div>
                  <div>
                    pages:{" "}
                    <b>
                      {details?.result?.indexer?.pages ??
                        details?.stats?.index?.pages ??
                        "—"}
                    </b>
                  </div>
                </div>
              </div>

              {/* ENRICHER */}
              <div className="bg-white rounded-lg border p-4">
                <div className="flex items-center justify-between mb-2">
                  <h4 className="font-semibold text-gray-900">Enricher</h4>
                  <span className="text-xs text-gray-500">
                    {details?.workers?.enricher?.status || "—"}
                  </span>
                </div>
                <div className="text-sm text-gray-700 space-y-1">
                  <div>
                    processed:{" "}
                    <b>
                      {details?.result?.enricher?.processed ??
                        details?.stats?.enrich?.processed ??
                        "—"}
                    </b>
                  </div>
                  <div>
                    inserted:{" "}
                    <b>
                      {details?.result?.enricher?.inserted ??
                        details?.stats?.enrich?.inserted ??
                        "—"}
                    </b>
                  </div>
                  <div>
                    updated:{" "}
                    <b>
                      {details?.result?.enricher?.updated ??
                        details?.stats?.enrich?.updated ??
                        "—"}
                    </b>
                  </div>
                  <div>
                    reviews_saved:{" "}
                    <b>
                      {details?.result?.enricher?.reviews_saved ??
                        details?.stats?.enrich?.reviews_saved ??
                        "—"}
                    </b>
                  </div>
                  <div>
                    dlq (failed):{" "}
                    <b>
                      {details?.result?.enricher?.dlq ??
                        details?.stats?.enrich?.dlq ??
                        0}
                    </b>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Подпись и быстрые действия */}
          <div className="text-xs text-gray-500 pt-2">
            pipeline: <b>{details?.task?.pipeline_id || task?.pipeline_id || "—"}</b>{" "}
            • task: <b>{task?.id}</b>
          </div>
        </div>
      </div>
    </div>
  );
}

TaskItem.propTypes = {
  task: PropTypes.object.isRequired,
  onOpenProducts: PropTypes.func,
  onOpenChildren: PropTypes.func,
};
