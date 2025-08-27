// components/ProductsPage.jsx
import React from "react";
import PropTypes from "prop-types";
import { RxUpdate } from "react-icons/rx";
import ProductsFilter from "./ProductsFilter";
import Pagination from "./Pagination";
import ProductCard from "./ProductCard";

export default function ProductsPage({
  totalProducts,
  products,
  isFetching,

  // фильтрация / режим
  productsMode, // "all" | "byTask"
  productsTaskId, // string
  productsScope, // "task" | "pipeline"
  onScopeChange, // (scope) => void
  onClearTask, // () => void

  currentCharacteristics, // Characteristic[]
  currentFilter, // { q, sort_by, sort_dir, filters }
  onFilterChange, // (nextFilter) => void
  onFilterReset, // () => void
  loadingFacets, // boolean
  onSearchFocusChange, // (bool) => void
  onSearchCommit, // () => void
  onSearchTyping, // (text) => void

  // действия
  onRefresh, // () => void
  onDeleteProduct, // (id) => Promise<void>
  onOpenProduct, // (product|sku) => void

  // пагинация
  page,
  totalPages,
  pageSize,
  onPageChange,
  onPageSizeChange,
}) {
  return (
    <div className="space-y-6">
      <div className="bg-white rounded-lg shadow-md p-6">
        <div className="flex items-center justify-between mb-4">
          <h3 className="text-lg font-semibold">Products ({totalProducts})</h3>
          <button
            onClick={onRefresh}
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2"
            title="Refresh results">
            <RxUpdate className="w-5 h-5" />
            Refresh
          </button>
        </div>

        {/* ФИЛЬТР */}
        <ProductsFilter
          mode={productsMode}
          taskId={productsTaskId}
          scope={productsScope}
          onScopeChange={onScopeChange}
          onClearTask={onClearTask}
          characteristics={currentCharacteristics}
          value={currentFilter}
          onChange={onFilterChange}
          onReset={onFilterReset}
          loadingFacets={loadingFacets}
          onSearchFocusChange={onSearchFocusChange}
          onSearchCommit={onSearchCommit}
          onSearchTyping={onSearchTyping}
        />

        <div className="relative">
          {/* Оверлей загрузки — НЕ меняет layout и НЕ крадёт фокус */}
          {isFetching && (
            <div className="absolute inset-0 z-10 bg-white/60 backdrop-blur-[1px] flex items-center justify-center">
              <div className="loading-spinner" aria-label="Loading" />
            </div>
          )}

          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {products.map((p) => (
              <ProductCard key={p.id} product={p} onDelete={onDeleteProduct} onOpen={() => onOpenProduct(p)} />
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
          onPageChange={onPageChange}
          onPageSizeChange={onPageSizeChange}
        />
      </div>
    </div>
  );
}

ProductsPage.propTypes = {
  totalProducts: PropTypes.number.isRequired,
  products: PropTypes.array.isRequired,
  isFetching: PropTypes.bool.isRequired,

  productsMode: PropTypes.oneOf(["all", "byTask"]).isRequired,
  productsTaskId: PropTypes.string,
  productsScope: PropTypes.oneOf(["task", "pipeline"]).isRequired,
  onScopeChange: PropTypes.func.isRequired,
  onClearTask: PropTypes.func.isRequired,

  currentCharacteristics: PropTypes.array.isRequired,
  currentFilter: PropTypes.shape({
    q: PropTypes.string,
    sort_by: PropTypes.string,
    sort_dir: PropTypes.oneOf(["asc", "desc"]),
    filters: PropTypes.object,
  }).isRequired,
  onFilterChange: PropTypes.func.isRequired,
  onFilterReset: PropTypes.func.isRequired,
  loadingFacets: PropTypes.bool.isRequired,
  onSearchFocusChange: PropTypes.func.isRequired,
  onSearchCommit: PropTypes.func.isRequired,
  onSearchTyping: PropTypes.func.isRequired,

  onRefresh: PropTypes.func.isRequired,
  onDeleteProduct: PropTypes.func.isRequired,
  onOpenProduct: PropTypes.func.isRequired,

  page: PropTypes.number.isRequired,
  totalPages: PropTypes.number.isRequired,
  pageSize: PropTypes.number.isRequired,
  onPageChange: PropTypes.func.isRequired,
  onPageSizeChange: PropTypes.func.isRequired,
};
