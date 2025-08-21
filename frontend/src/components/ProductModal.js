// src/components/ProductModal.js
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import PropTypes from "prop-types";

/* ---------- Icons (inline SVG) ---------- */
const IconExternalLink = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M13.213 9.787a3.391 3.391 0 0 0-4.795 0l-3.425 3.426a3.39 3.39 0 0 0 4.795 4.794l.321-.304m-.321-4.49a3.39 3.39 0 0 0 4.795 0l3.424-3.426a3.39 3.39 0 0 0-4.794-4.795l-1.028.961"/>
  </svg>
);

const IconClose = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <path stroke="currentColor" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M6 18 17.94 6M18 18 6.06 6"/>
  </svg>
);

const IconImagePlaceholder = ({ className }) => (
  <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
    <rect x="3" y="5" width="18" height="14" rx="2" />
    <path d="M3 15l4-4 4 4 5-5 5 5" />
    <circle cx="8" cy="9" r="1.5" />
  </svg>
);

const CountBadge = ({ count }) => (
  <span className="ml-2 inline-flex items-center px-2 py-0.5 text-xs font-medium rounded-full bg-gray-100 text-gray-700">
    {count}
  </span>
);

/* ---------- PropTypes & utils ---------- */
const DateLike = PropTypes.oneOfType([PropTypes.string, PropTypes.number, PropTypes.instanceOf(Date)]);
const ProductPropType = PropTypes.shape({
  id: PropTypes.string,
  title: PropTypes.string,
  name: PropTypes.string,
  sku: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
  cover_image: PropTypes.string,
  gallery: PropTypes.shape({
    coverImage: PropTypes.string,
    images: PropTypes.arrayOf(
      PropTypes.oneOfType([
        PropTypes.string,
        PropTypes.shape({ src: PropTypes.string, alt: PropTypes.string }),
      ])
    ),
    videos: PropTypes.arrayOf(PropTypes.shape({ name: PropTypes.string, url: PropTypes.string, coverUrl: PropTypes.string })),
  }),
  description: PropTypes.shape({
    content_blocks: PropTypes.arrayOf(
      PropTypes.shape({
        img: PropTypes.shape({ src: PropTypes.string, alt: PropTypes.string }),
        title: PropTypes.string,
        text: PropTypes.string,
      })
    ),
    specs: PropTypes.arrayOf(PropTypes.shape({ title: PropTypes.string, content: PropTypes.string })),
  }),
  characteristics: PropTypes.shape({
    full: PropTypes.arrayOf(
      PropTypes.shape({ id: PropTypes.string, title: PropTypes.string, values: PropTypes.arrayOf(PropTypes.string) })
    ),
  }),
  aspects: PropTypes.oneOfType([PropTypes.array, PropTypes.object]),
  collections: PropTypes.oneOfType([PropTypes.array, PropTypes.object]),
  other_offers: PropTypes.arrayOf(
    PropTypes.shape({
      sku: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
      seller_id: PropTypes.string,
      card_price: PropTypes.number,
      orig_price: PropTypes.number,
      disc_price: PropTypes.number,
    })
  ),
  seo: PropTypes.object,
  created_at: DateLike,
  updated_at: DateLike,
});

const TAB_ORDER = ["description", "characteristics", "variantsAll"];
const LS_TAB_KEY = "productModal.activeTab";

const toArray = (x) => (Array.isArray(x) ? x : x && typeof x === "object" ? (Array.isArray(x.items) ? x.items : Object.values(x)) : []);
const firstImage = (product) => {
  if (product?.cover_image) return { src: product.cover_image, alt: product.title || product.name || "" };
  if (product?.gallery?.coverImage) return { src: product.gallery.coverImage, alt: product.title || "" };
  const imgs = toArray(product?.gallery?.images).map((i) => (typeof i === "string" ? { src: i, alt: "" } : i));
  if (imgs.length) return imgs[0];
  const blocks = product?.description?.content_blocks || [];
  for (const b of blocks) if (b?.img?.src) return { src: b.img.src, alt: b.img.alt || "" };
  return null;
};
const getPrice = (seo) => (seo?.offers?.price ? `${seo.offers.price} ${seo?.offers?.priceCurrency || "RUB"}` : null);
const getRating = (seo) => (seo?.aggregateRating?.ratingValue ? { rating: Number(seo.aggregateRating.ratingValue), count: seo?.aggregateRating?.reviewCount ? Number(seo.aggregateRating.reviewCount) : null } : null);
const dedupe = (arr, key = (x) => x) => { const s=new Set(); const out=[]; for(const v of arr||[]){const k=key(v); if(k==null||s.has(k)) continue; s.add(k); out.push(v);} return out; };
const joinVals = (item) => (item?.values || []).filter(Boolean).join(", ");
const copyToClipboard = (t) => { try { navigator.clipboard?.writeText(String(t)); } catch {} };

/* Текст с <br/> → переносы строк */
function TextWithBreaks({ text }) {
  const parts = String(text || "").split(/<br\s*\/?>/gi);
  return parts.map((p, i) => (
    <React.Fragment key={i}>
      {p}
      {i < parts.length - 1 && <br />}
    </React.Fragment>
  ));
}

/* ---------- Component ---------- */
export default function ProductModal({ isOpen, product, onClose, onSelectSku }) {
  const [activeTab, setActiveTab] = useState(() => {
    const saved = localStorage.getItem(LS_TAB_KEY);
    return TAB_ORDER.includes(saved) ? saved : "description";
  });
  const [activeImage, setActiveImage] = useState(null);
  const [copied, setCopied] = useState(false);
  const [mainLoaded, setMainLoaded] = useState(false);

  const dialogRef = useRef(null);
  const scrollRef = useRef(null); // чтобы прокручивать вверх при смене вкладки

  // галерея
  const gallery = useMemo(() => {
    const images = [];
    const f = firstImage(product);
    if (f) images.push(f);
    const more = toArray(product?.gallery?.images).map((i) => (typeof i === "string" ? { src: i, alt: "" } : i));
    for (const m of more) if (m?.src) images.push({ src: m.src, alt: m.alt || "" });
    const blocks = product?.description?.content_blocks || [];
    for (const b of blocks) if (b?.img?.src) images.push({ src: b.img.src, alt: b.img.alt || "" });
    return dedupe(images, (x) => x.src);
  }, [product]);

  useEffect(() => { setActiveImage(gallery[0] || null); }, [gallery]);

  // короткий структурированный блок
  const summaryPairs = useMemo(() => {
    const list = product?.characteristics?.full || [];
    const byId = Object.fromEntries(list.map((x) => [x.id, x]));
    const byTitle = Object.fromEntries(list.map((x) => [String(x.title || "").toLowerCase(), x]));
    const pick = (id, title) => byId[id] || byTitle[String(title || "").toLowerCase()];
    const rows = [
      ["Вид чая", pick("TeaType", "вид чая")],
      ["Сорт чая", pick("TeaGrade", "сорт чая")],
      ["Форма чая", pick("VarietyTeaShape", "форма чая")],
      ["Размер чайного листа", pick("TeaLeafSize", "размер чайного листа")],
      ["Вкус", pick("TeaTaste", "вкус")],
      ["Страна-изготовитель", pick("Country", "страна-изготовитель")],
      ["Вес товара, г", pick("Weight", "вес товара, г")],
      ["Особенности", pick("NewFeatures", "особенности напитков, продуктов питания")],
      ["Не содержит", pick("NotContain", "не содержит")],
    ].filter(([, v]) => v).map(([label, item]) => ({ label, value: joinVals(item) }));
    return rows.slice(0, 8);
  }, [product]);

  const price = useMemo(() => getPrice(product?.seo), [product]);
  const rating = useMemo(() => getRating(product?.seo), [product]);
  const ozonUrl = product?.seo?.offers?.url || product?.seo?.url || null;

  // Варианты: группируем по аспектам, плюс Коллекции/Предложения
  const variantsGroups = useMemo(() => {
    const groups = [];

    // аспекты отдельно (названия аспектов вернулись)
    for (const aspect of toArray(product?.aspects)) {
      const groupTitle = aspect?.aspectName || aspect?.title || aspect?.aspectKey || "Вариант";
      const items = toArray(aspect?.variants)
        .map((v) => ({ sku: v?.sku, title: v?.title || String(v?.sku || ""), coverImage: v?.coverImage || null }))
        .filter((x) => x.sku);
      if (items.length) groups.push({ group: groupTitle, items });
    }

    // Коллекции
    const col = toArray(product?.collections)
      .map((c) => ({ sku: c?.sku, title: String(c?.sku || ""), coverImage: c?.picture || c?.coverImage || null }))
      .filter((x) => x.sku);
    if (col.length) groups.push({ group: "Коллекции", items: col });

    // Предложения
    const off = toArray(product?.other_offers)
      .map((o) => ({ sku: o?.sku, title: String(o?.sku || ""), coverImage: null }))
      .filter((x) => x.sku);
    if (off.length) groups.push({ group: "Предложения", items: off });

    return groups;
  }, [product]);

  // клавиатура + закрытие
  useEffect(() => {
    if (!isOpen) return;
    const onKey = (e) => {
      if (e.key === "Escape") onClose?.();
      if (activeImage && gallery.length > 1 && !e.ctrlKey) {
        if (e.key === "ArrowRight") {
          const idx = gallery.findIndex((g) => g.src === activeImage.src);
          setActiveImage(gallery[(idx + 1) % gallery.length]);
        } else if (e.key === "ArrowLeft") {
          const idx = gallery.findIndex((g) => g.src === activeImage.src);
          setActiveImage(gallery[(idx - 1 + gallery.length) % gallery.length]);
        }
      }
      if (e.ctrlKey && (e.key === "ArrowRight" || e.key === "ArrowLeft")) {
        e.preventDefault();
        const idx = TAB_ORDER.indexOf(activeTab);
        const next = e.key === "ArrowRight" ? TAB_ORDER[(idx + 1) % TAB_ORDER.length] : TAB_ORDER[(idx - 1 + TAB_ORDER.length) % TAB_ORDER.length];
        setActiveTab(next);
        localStorage.setItem(LS_TAB_KEY, next);
        scrollRef.current?.scrollTo({ top: 0, behavior: "smooth" });
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [isOpen, onClose, activeImage, gallery, activeTab]);

  // заблокировать body
  useEffect(() => {
    if (!isOpen) return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => { document.body.style.overflow = prev || ""; };
  }, [isOpen]);

  const onBackdropClick = (e) => { if (dialogRef.current && !dialogRef.current.contains(e.target)) onClose?.(); };
  const titleText = product?.title || product?.name || `SKU ${product?.sku || ""}`;
  const fullSpecs = useMemo(() => product?.characteristics?.full || [], [product]);

  const onCopySku = useCallback(() => {
    if (!product?.sku) return;
    copyToClipboard(product.sku);
    setCopied(true);
    setTimeout(() => setCopied(false), 1200);
  }, [product?.sku]);

  const selectSku = useCallback((sku) => { if (sku) onSelectSku?.(sku); }, [onSelectSku]);

  const handleTabClick = (id) => {
    setActiveTab(id);
    localStorage.setItem(LS_TAB_KEY, id);
    scrollRef.current?.scrollTo({ top: 0, behavior: "smooth" }); // прокрутка вверх
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" onMouseDown={onBackdropClick} style={{ background: "rgba(0,0,0,0.45)", backdropFilter: "blur(2px)" }} aria-modal="true" role="dialog">
      {/* локальные стили для скроллбаров и угла */}
      <style>{`
        .thumbs-scroll{scrollbar-width:thin;scrollbar-color:rgba(0,0,0,.18) transparent}
        .thumbs-scroll::-webkit-scrollbar{width:6px}
        .thumbs-scroll::-webkit-scrollbar-track{background:transparent}
        .thumbs-scroll::-webkit-scrollbar-thumb{background:rgba(0,0,0,.18);border-radius:4px}
        .thumbs-scroll:hover::-webkit-scrollbar-thumb,.thumbs-scroll:active::-webkit-scrollbar-thumb{background:rgba(0,0,0,.36)}
        .modal-scroll{scrollbar-gutter:stable both-edges}
        .modal-scroll::-webkit-scrollbar-corner{background:transparent}
      `}</style>

      <div ref={dialogRef} className="bg-white rounded-2xl shadow-2xl w-full max-w-6xl fade-in flex flex-col" style={{ maxHeight: "92vh" }} onMouseDown={(e) => e.stopPropagation()}>
        {/* Scroll area — скруглённая и «закрытая», чтоб скроллбар не вылезал */}
        <div
          ref={scrollRef}
          className="px-6 py-5 overflow-y-auto flex-1 modal-scroll"
          style={{ borderRadius: 16, overflow: "hidden", overflowY: "auto", overflowX: "hidden" }}
        >
          <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
            {/* левая колонка: галерея сверху */}
            <div className="lg:col-span-5">
              <div className="card p-3" style={{ boxShadow: "none", transform: "none" }}>
                <div className="flex gap-3">
                  {gallery.length > 1 && (
                    <div className="thumbs-scroll overflow-y-auto" style={{ maxHeight: 560, paddingRight: 2 }}>
                      <div className="flex flex-col gap-2 pr-1">
                        {gallery.map((img, idx) => {
                          const selected = activeImage?.src === img.src;
                          return (
                            <button
                              key={img.src}
                              className={`border rounded-lg p-1 bg-white ${selected ? "border-gray-400" : "border-gray-200"}`}
                              onClick={() => { setActiveImage(img); setMainLoaded(false); }}
                              title={img.alt || `Изображение ${idx + 1}`}
                              style={{ width: 96 }}
                              tabIndex={0}
                              onKeyDown={(e) => {
                                if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setActiveImage(img); setMainLoaded(false); }
                              }}
                            >
                              <img src={img.src} alt={img.alt || ""} loading="lazy" style={{ width: 88, height: 118, objectFit: "cover", borderRadius: 6 }} />
                            </button>
                          );
                        })}
                      </div>
                    </div>
                  )}

                  <div className="flex-1 rounded-lg bg-gray-50 flex items-center justify-center relative" style={{ minHeight: 420, height: 560 }}>
                    {!mainLoaded && <div className="absolute inset-0 animate-pulse" style={{ background: "linear-gradient(180deg,#f3f4f6,#ebeef2)" }} />}
                    {activeImage ? (
                      <img
                        src={activeImage.src}
                        alt={activeImage.alt || titleText}
                        onLoad={() => setMainLoaded(true)}
                        loading="eager"
                        style={{ maxHeight: 540, maxWidth: "100%", objectFit: "contain", borderRadius: 8, position: "relative", zIndex: 1 }}
                      />
                    ) : (
                      <div className="text-6xl relative z-10">🍵</div>
                    )}
                  </div>
                </div>

                {/* видео внизу, свёрнуто */}
                {Array.isArray(product?.gallery?.videos) && product.gallery.videos.length > 0 && product.gallery.videos[0]?.url && (
                  <details className="mt-3">
                    <summary className="text-sm text-gray-600 cursor-pointer select-none">Видео</summary>
                    <video controls poster={product.gallery.videos[0]?.coverUrl || undefined} style={{ width: "100%", borderRadius: 8, marginTop: 8 }}>
                      <source src={product.gallery.videos[0]?.url} />
                    </video>
                  </details>
                )}
              </div>

              {/* кратко о товаре */}
              {summaryPairs.length > 0 && (
                <div className="card mt-4 p-4" style={{ boxShadow: "none", transform: "none" }}>
                  <div className="text-sm text-gray-600 mb-2">Кратко о товаре</div>
                  <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                    {summaryPairs.map((row) => (
                      <div key={`${row.label}:${row.value}`} className="bg-gray-50 rounded-lg p-3">
                        <div className="text-[11px] uppercase tracking-wide text-gray-500">{row.label}</div>
                        <div className="text-sm font-medium text-gray-900">{row.value}</div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>

            {/* правая колонка: шапка + вкладки */}
            <div className="lg:col-span-7">
              {/* шапка справа над вкладками */}
              <div className="relative bg-white rounded-xl border border-gray-200 p-4 mb-4">
                <div className="text-sm text-gray-500 mb-1">
                  SKU:{" "}
                  <button
                    onClick={() => { if (!product?.sku) return; copyToClipboard(product.sku); setCopied(true); setTimeout(() => setCopied(false), 1200); }}
                    className="p-0 m-0 bg-transparent border-0 text-gray-700 cursor-pointer underline decoration-dotted focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 rounded"
                    aria-label="Скопировать SKU" title="Скопировать SKU"
                  >
                    {product?.sku || "N/A"}
                  </button>
                  {copied && <span className="ml-2 text-xs text-green-600">Скопировано</span>}
                </div>

                <h2 className="font-bold text-gray-900 text-lg md:text-xl leading-snug line-clamp-2 max-w-3xl">
                  {product?.title || product?.name || `SKU ${product?.sku || ""}`}
                </h2>

                {(price || rating) && (
                  <div className="mt-2 flex items-center gap-3 text-sm text-gray-700">
                    {price && <span className="px-2 py-1 bg-gray-100 rounded-lg">{price}</span>}
                    {rating && <span className="px-2 py-1 bg-yellow-50 rounded-lg">⭐ {rating.rating}{rating.count ? ` (${rating.count})` : ""}</span>}
                  </div>
                )}

                <div className="absolute top-2 right-2 flex items-center gap-1">
                  {ozonUrl && (
                    <a href={ozonUrl} target="_blank" rel="noreferrer" className="w-9 h-9 flex items-center justify-center text-gray-500 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 rounded" title="Открыть на Ozon" aria-label="Открыть на Ozon" style={{ background: "transparent", border: "none" }}>
                      <IconExternalLink className="w-5 h-5" />
                    </a>
                  )}
                  <button onClick={onClose} aria-label="Закрыть" title="Закрыть" className="w-9 h-9 flex items-center justify-center text-gray-500 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 rounded" style={{ background: "transparent", border: "none" }}>
                    <IconClose className="w-5 h-5" />
                  </button>
                </div>
              </div>

              {/* вкладки */}
              <div className="flex gap-2 mb-4 sticky top-0 z-10" style={{ background: "white", paddingTop: 4, paddingBottom: 4 }}>
                {[{ id: "description", label: "Описание" }, { id: "characteristics", label: "Характеристики" }, { id: "variantsAll", label: "Варианты" }].map((t) => (
                  <button
                    key={t.id}
                    onClick={() => handleTabClick(t.id)}
                    className={`px-3 py-2 rounded-lg text-sm font-medium border ${activeTab === t.id ? "bg-blue-600 text-white border-blue-600" : "bg-white text-gray-700 border-gray-200"}`}
                    style={{ boxShadow: "none", transform: "none" }}
                    tabIndex={0}
                    onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); handleTabClick(t.id); } }}
                  >
                    {t.label}
                  </button>
                ))}
              </div>

              {/* контент вкладок */}
              <div className="card p-4" style={{ boxShadow: "none", transform: "none" }}>
                {activeTab === "description" && <DescriptionPane product={product} />}

                {activeTab === "characteristics" && (
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                    {(fullSpecs || []).length === 0 && <div className="text-gray-500">Нет данных</div>}
                    {(fullSpecs || []).map((s) => (
                      <div key={`${s.id}-${s.title}`} className="p-3 bg-gray-50 rounded-lg">
                        <div className="text-xs uppercase tracking-wide text-gray-500">{s.title}</div>
                        <div className="text-sm font-medium text-gray-900">{joinVals(s) || "—"}</div>
                      </div>
                    ))}
                  </div>
                )}

                {activeTab === "variantsAll" && (
                  <VariantsUnified groups={variantsGroups} onSelectSku={selectSku} />
                )}
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

ProductModal.propTypes = { isOpen: PropTypes.bool.isRequired, product: ProductPropType, onClose: PropTypes.func.isRequired, onSelectSku: PropTypes.func };
ProductModal.defaultProps = { product: {}, onSelectSku: () => {} };

/* ---------- Sub panes ---------- */

// Описание: перенос «Условия хранения» и «Состав» вниз + переносы строк из <br/>
function DescriptionPane({ product }) {
  const specs = Array.isArray(product?.description?.specs) ? product.description.specs : [];
  const bottomTitles = new Set(["Условия хранения", "Состав"]);
  const bottomSpecs = specs.filter((s) => bottomTitles.has(s.title));
  const otherSpecs = specs.filter((s) => !bottomTitles.has(s.title));

  return (
    <div className="space-y-6">
      {otherSpecs.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {otherSpecs.map((sp, i) => (
            <div key={`${sp.title}-${i}`} className="p-3 bg-gray-50 rounded-lg">
              <div className="text-xs uppercase tracking-wide text-gray-500">{sp.title}</div>
              <div className="text-sm text-gray-900"><TextWithBreaks text={sp.content} /></div>
            </div>
          ))}
        </div>
      )}

      {(product?.description?.content_blocks || []).map((b, i) => (
        <div key={i} className="space-y-2">
          {b?.img?.src && <img src={b.img.src} alt={b.img.alt || ""} loading="lazy" style={{ width: "100%", height: "auto", borderRadius: 10, display: "block" }} />}
          {b?.title && <div className="font-semibold"><TextWithBreaks text={b.title} /></div>}
          {b?.text && <div className="text-gray-700 whitespace-pre-wrap"><TextWithBreaks text={b.text} /></div>}
        </div>
      ))}

      {bottomSpecs.length > 0 && (
        <div className="space-y-3">
          <div className="text-sm text-gray-600">Дополнительно</div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {bottomSpecs.map((sp, i) => (
              <div key={`${sp.title}-${i}`} className="p-3 bg-gray-50 rounded-lg">
                <div className="text-xs uppercase tracking-wide text-gray-500">{sp.title}</div>
                <div className="text-sm text-gray-900"><TextWithBreaks text={sp.content} /></div>
              </div>
            ))}
          </div>
        </div>
      )}

      {otherSpecs.length === 0 &&
        (!product?.description?.content_blocks || product.description.content_blocks.length === 0) &&
        bottomSpecs.length === 0 && <div className="text-gray-500">Описание отсутствует</div>}
    </div>
  );
}
DescriptionPane.propTypes = { product: ProductPropType };

/* Вкладка «Варианты»: группы по аспектам + Коллекции + Предложения; заголовок + бейдж-счётчик */
function VariantsUnified({ groups, onSelectSku }) {
  if (!groups.length) return <div className="text-gray-500">Нет данных</div>;
  return (
    <div className="space-y-6">
      {groups.map((group) => (
        <div key={group.group}>
          <div className="text-sm font-semibold mb-2 flex items-center">
            {group.group}
            <CountBadge count={group.items.length} />
          </div>
          <div className="flex flex-wrap gap-2">
            {group.items.map((v) => (
              <button
                key={`${group.group}-${v.sku}`}
                className="px-3 py-2 bg-white border border-gray-200 rounded-lg focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500"
                onClick={() => onSelectSku(v.sku)}
                title={v.title}
              >
                <div className="flex items-center gap-2">
                  {v.coverImage ? (
                    <img src={v.coverImage} alt="" loading="lazy" style={{ width: 28, height: 28, borderRadius: 6, objectFit: "cover" }} />
                  ) : (
                    <div className="flex items-center justify-center" style={{ width: 28, height: 28, borderRadius: 6, background: "#f3f4f6" }} aria-hidden="true">
                      <IconImagePlaceholder className="w-4 h-4 text-gray-500" />
                    </div>
                  )}
                  <div className="text-sm text-gray-800 line-clamp-2" style={{ maxWidth: 220 }}>
                    {v.title || `SKU ${v.sku}`}
                  </div>
                </div>
              </button>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}
VariantsUnified.propTypes = { groups: PropTypes.array.isRequired, onSelectSku: PropTypes.func.isRequired };
