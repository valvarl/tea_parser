// components/TeaProduct.jsx
import React from "react";
import PropTypes from "prop-types";

const DateLike = PropTypes.oneOfType([PropTypes.string, PropTypes.number, PropTypes.instanceOf(Date)]);

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
      }),
    ),
  }),
  gallery: PropTypes.object,
  characteristics: PropTypes.shape({
    full: PropTypes.arrayOf(
      PropTypes.shape({
        id: PropTypes.string,
        title: PropTypes.string,
        values: PropTypes.arrayOf(PropTypes.string),
      }),
    ),
  }),
});

function formatDate(input) {
  if (!input) return "N/A";
  const d = new Date(input);
  if (Number.isNaN(d.getTime())) return "N/A";
  return d.toLocaleString("en-GB");
}

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
  const byTitle = Object.fromEntries(list.map((x) => [String(x.title || "").toLowerCase(), x]));
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

export default function TeaCard({ product, onDelete, onOpen, size = "large", className = "" }) {
  const img = pickCover(product);
  const title = product.title || product.name || product.sku || "Untitled";
  const specs = extractSpecs(product);
  const ts = product.updated_at || product.created_at || product.scraped_at;

  if (size === "small") {
    // Компактная версия (~50% по высоте), горизонтальная компоновка, вертикальное изображение 3:4
    return (
      <div
        className={`bg-white rounded-lg shadow p-3 hover:shadow-md transition-shadow cursor-pointer relative ${className}`}
        onClick={onOpen}>
        {/* Кнопка удаления — уменьшенная */}
        <button
          onClick={(e) => {
            e.stopPropagation();
            onDelete(product.id);
          }}
          className="absolute top-2 right-2 z-10 text-red-500 hover:text-red-700 text-xs bg-white/90 rounded-full px-1.5 py-0.5 shadow"
          title="Delete"
          aria-label="Delete">
          ❌
        </button>

        <div className="flex gap-3">
          {/* Левая колонка: вертикальное изображение 3:4, фиксируем ширину для контроля высоты */}
          <div
            className="shrink-0 rounded-md overflow-hidden bg-gray-100"
            style={{ aspectRatio: "3 / 4", width: "6.5rem" }} // ~104px ширина -> высота ~138px
          >
            {img ? (
              <img src={img} alt={title} className="w-full h-full object-cover" />
            ) : (
              <div className="w-full h-full flex items-center justify-center text-2xl">🍵</div>
            )}
          </div>

          {/* Правая колонка: текст, укороченные спецификации */}
          <div className="min-w-0 flex-1">
            <h3 className="text-sm font-semibold text-gray-900 leading-snug line-clamp-2">{title}</h3>

            <div className="mt-1 space-y-0.5">
              {specs.slice(0, 2).map((s) => (
                <div key={s.label} className="flex justify-between gap-2 text-xs">
                  <span className="text-gray-600">{s.label}:</span>
                  <span className="font-medium text-gray-900 text-right truncate">{s.val}</span>
                </div>
              ))}
            </div>

            <div className="mt-2 flex items-center justify-between text-[11px] text-gray-500">
              <span>Updated:</span>
              <span className="truncate">{formatDate(ts)}</span>
            </div>
          </div>
        </div>
      </div>
    );
  }

  // Полноразмерная версия (как в Products сейчас)
  return (
    <div
      className={`bg-white rounded-lg shadow-md p-4 hover:shadow-lg transition-shadow cursor-pointer relative ${className}`}
      onClick={onOpen}>
      <button
        onClick={(e) => {
          e.stopPropagation();
          onDelete(product.id);
        }}
        className="absolute top-2 right-2 z-10 text-red-500 hover:text-red-700 text-sm bg-white/90 rounded-full px-2 py-1 shadow"
        title="Delete"
        aria-label="Delete">
        ❌
      </button>

      <div className="flex justify-between items-start mb-3 pr-8">
        <h3 className="text-base font-semibold text-gray-900 line-clamp-2">{title}</h3>
      </div>

      {/* В полной версии оставляем текущую широкую обложку */}
      {img ? (
        <img src={img} alt={title} className="w-full h-44 object-cover rounded-lg mb-3" />
      ) : (
        <div className="w-full h-44 rounded-lg mb-3 bg-gray-100 flex items-center justify-center text-4xl">🍵</div>
      )}

      <div className="space-y-1 text-sm">
        {specs.map((s) => (
          <div key={s.label} className="flex justify-between">
            <span className="text-gray-600">{s.label}:</span>
            <span className="font-medium text-gray-900 text-right">{s.val}</span>
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
  onOpen: PropTypes.func.isRequired,
  size: PropTypes.oneOf(["large", "small"]),
  className: PropTypes.string,
};
