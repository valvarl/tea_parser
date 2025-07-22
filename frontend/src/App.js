import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './App.css';

const BACKEND_URL = process.env.REACT_APP_BACKEND_URL;
const API = `${BACKEND_URL}/api/v1`;

function App() {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [products, setProducts] = useState([]);
  const [tasks, setTasks] = useState([]);
  const [stats, setStats] = useState({});
  const [loading, setLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState('пуэр');
  const [scrapingTasks, setScrapingTasks] = useState([]);
  const [selectedTeaType, setSelectedTeaType] = useState('');
  const [categories, setCategories] = useState([]);

  useEffect(() => {
    fetchStats();
    fetchProducts();
    fetchTasks();
    fetchCategories();
  }, []);

  const fetchStats = async () => {
    try {
      const response = await axios.get(`${API}/stats`);
      setStats(response.data);
    } catch (error) {
      console.error('Error fetching stats:', error);
    }
  };

  const fetchProducts = async () => {
    try {
      const response = await axios.get(`${API}/products?limit=100${selectedTeaType ? `&tea_type=${selectedTeaType}` : ''}`);
      setProducts(response.data);
    } catch (error) {
      console.error('Error fetching products:', error);
    }
  };

  const fetchTasks = async () => {
    try {
      const response = await axios.get(`${API}/scrape/tasks`);
      setTasks(response.data);
    } catch (error) {
      console.error('Error fetching tasks:', error);
    }
  };

  const fetchCategories = async () => {
    try {
      const response = await axios.get(`${API}/categories`);
      setCategories(response.data.categories || []);
    } catch (error) {
      console.error('Error fetching categories:', error);
    }
  };

  const startScraping = async () => {
    if (!searchTerm.trim()) {
      alert('Please enter a search term');
      return;
    }

    setLoading(true);
    try {
      const response = await axios.post(`${API}/scrape/start?search_term=${encodeURIComponent(searchTerm)}`);
      alert(`Scraping started! Task ID: ${response.data.task_id}`);
      fetchTasks();
      fetchStats();
    } catch (error) {
      console.error('Error starting scraping:', error);
      alert('Error starting scraping. Please try again.');
    } finally {
      setLoading(false);
    }
  };

  const deleteProduct = async (productId) => {
    if (!window.confirm('Are you sure you want to delete this product?')) return;

    try {
      await axios.delete(`${API}/products/${productId}`);
      fetchProducts();
      fetchStats();
    } catch (error) {
      console.error('Error deleting product:', error);
    }
  };

  const exportCSV = async () => {
    try {
      const response = await axios.get(`${API}/export/csv`);
      const csvContent = "data:text/csv;charset=utf-8," + 
        "ID,Name,Price,Rating,Type,Region,Pressed,Scraped\n" +
        response.data.data.map(row => 
          `${row.id},${row.name},${row.price},${row.rating},${row.tea_type},${row.tea_region},${row.is_pressed},${row.scraped_at}`
        ).join("\n");
      
      const encodedUri = encodeURI(csvContent);
      const link = document.createElement("a");
      link.setAttribute("href", encodedUri);
      link.setAttribute("download", "tea_products.csv");
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    } catch (error) {
      console.error('Error exporting CSV:', error);
    }
  };

  const formatDate = (dateString) => {
    if (!dateString) return 'N/A';
    return new Date(dateString).toLocaleString('ru-RU');
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'completed': return 'text-green-600';
      case 'running': return 'text-blue-600';
      case 'failed': return 'text-red-600';
      default: return 'text-gray-600';
    }
  };

  const getStatusBadge = (status) => {
    const colors = {
      'completed': 'bg-green-100 text-green-800',
      'running': 'bg-blue-100 text-blue-800',
      'failed': 'bg-red-100 text-red-800',
      'pending': 'bg-yellow-100 text-yellow-800'
    };
    
    return (
      <span className={`px-2 py-1 rounded-full text-xs font-medium ${colors[status] || 'bg-gray-100 text-gray-800'}`}>
        {status}
      </span>
    );
  };

  const StatCard = ({ title, value, subtitle, color = 'blue' }) => (
    <div className="bg-white rounded-lg shadow-md p-6 border-l-4 border-blue-500">
      <div className="flex items-center justify-between">
        <div>
          <p className="text-sm font-medium text-gray-600">{title}</p>
          <p className="text-2xl font-bold text-gray-900">{value}</p>
          {subtitle && <p className="text-sm text-gray-500">{subtitle}</p>}
        </div>
        <div className={`text-3xl text-${color}-500`}>
          📊
        </div>
      </div>
    </div>
  );

  const TeaCard = ({ product }) => (
    <div className="bg-white rounded-lg shadow-md p-6 hover:shadow-lg transition-shadow">
      <div className="flex justify-between items-start mb-4">
        <h3 className="text-lg font-semibold text-gray-900 line-clamp-2">{product.name}</h3>
        <button 
          onClick={() => deleteProduct(product.id)}
          className="text-red-500 hover:text-red-700 text-sm"
        >
          ❌
        </button>
      </div>
      
      {product.images && product.images.length > 0 && (
        <img 
          src={product.images[0]} 
          alt={product.name}
          className="w-full h-48 object-cover rounded-lg mb-4"
        />
      )}
      
      <div className="space-y-2">
        {product.price && (
          <div className="flex justify-between">
            <span className="text-gray-600">Цена:</span>
            <span className="font-semibold text-green-600">{product.price} ₽</span>
          </div>
        )}
        
        {product.rating && (
          <div className="flex justify-between">
            <span className="text-gray-600">Рейтинг:</span>
            <span className="font-semibold">⭐ {product.rating}</span>
          </div>
        )}
        
        {product.tea_type && (
          <div className="flex justify-between">
            <span className="text-gray-600">Тип:</span>
            <span className="font-semibold text-blue-600">{product.tea_type}</span>
          </div>
        )}
        
        {product.tea_region && (
          <div className="flex justify-between">
            <span className="text-gray-600">Регион:</span>
            <span className="font-semibold">{product.tea_region}</span>
          </div>
        )}
        
        {product.is_pressed !== undefined && (
          <div className="flex justify-between">
            <span className="text-gray-600">Форма:</span>
            <span className="font-semibold">{product.is_pressed ? '🧱 Прессованный' : '🍃 Рассыпной'}</span>
          </div>
        )}
        
        <div className="flex justify-between text-sm">
          <span className="text-gray-500">Получено:</span>
          <span className="text-gray-500">{formatDate(product.scraped_at)}</span>
        </div>
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-4">
            <div className="flex items-center space-x-3">
              <div className="text-2xl">🍃</div>
              <div>
                <h1 className="text-xl font-bold text-gray-900">Парсер китайского чая</h1>
                <p className="text-sm text-gray-600">Ozon.ru • Система сбора данных</p>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <button 
                onClick={exportCSV}
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors"
              >
                📥 Экспорт CSV
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Navigation */}
      <nav className="bg-white border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex space-x-8">
            {[
              { id: 'dashboard', label: 'Панель управления', icon: '📊' },
              { id: 'scraping', label: 'Парсинг', icon: '🔍' },
              { id: 'products', label: 'Товары', icon: '🍃' },
              { id: 'tasks', label: 'Задачи', icon: '⚙️' }
            ].map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center space-x-2 py-4 px-2 border-b-2 font-medium text-sm ${
                  activeTab === tab.id 
                    ? 'border-blue-500 text-blue-600' 
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                <span>{tab.icon}</span>
                <span>{tab.label}</span>
              </button>
            ))}
          </div>
        </div>
      </nav>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {activeTab === 'dashboard' && (
          <div className="space-y-6">
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              <StatCard 
                title="Всего товаров" 
                value={stats.total_products || 0}
                subtitle="В базе данных"
              />
              <StatCard 
                title="Активных задач" 
                value={stats.running_tasks || 0}
                subtitle="В процессе выполнения"
              />
              <StatCard 
                title="Выполненных задач" 
                value={stats.completed_tasks || 0}
                subtitle="Успешно завершены"
              />
              <StatCard 
                title="Процент ошибок" 
                value={`${(stats.error_rate || 0).toFixed(1)}%`}
                subtitle="За все время"
                color="red"
              />
            </div>

            {/* Geo-blocking Alert */}
            {stats.total_products === 0 && stats.total_tasks > 0 && (
              <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-6">
                <div className="flex items-start">
                  <div className="text-yellow-600 text-2xl mr-4">⚠️</div>
                  <div>
                    <h3 className="text-lg font-semibold text-yellow-800 mb-2">
                      Возможная проблема с гео-блокировкой
                    </h3>
                    <p className="text-yellow-700 mb-4">
                      Все задачи парсинга завершаются с 0 товаров. Это может указывать на:
                    </p>
                    <ul className="text-yellow-700 space-y-1 mb-4">
                      <li>• 🌍 <strong>Гео-блокировка:</strong> Ozon.ru блокирует не-российские IP адреса</li>
                      <li>• 🔐 <strong>Отсутствие токенов:</strong> Нужны российские proxy и CSRF токены</li>
                      <li>• 🏷️ <strong>Отсутствие региона:</strong> Требуется cookie ozon_regions=213000000 (Москва)</li>
                    </ul>
                    <div className="bg-yellow-100 p-3 rounded-lg">
                      <p className="text-sm text-yellow-800">
                        <strong>Рекомендация:</strong> Для полноценной работы парсера требуются российские proxy-серверы 
                        и настройка региона. В текущем демо-режиме система обнаруживает и логирует проблемы доступа.
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            )}

            <div className="bg-white rounded-lg shadow-md p-6">
              <h3 className="text-lg font-semibold mb-4">Последние задачи</h3>
              <div className="space-y-3">
                {tasks.slice(0, 5).map(task => (
                  <div key={task.id} className="flex items-center justify-between p-3 bg-gray-50 rounded-lg">
                    <div>
                      <p className="font-medium">{task.search_term}</p>
                      <p className="text-sm text-gray-500">{formatDate(task.created_at)}</p>
                      {task.error_message && (
                        <p className="text-sm text-red-600 mt-1">{task.error_message}</p>
                      )}
                    </div>
                    <div className="flex items-center space-x-3">
                      <span className="text-sm text-gray-600">
                        {task.scraped_products || 0} товаров
                      </span>
                      {getStatusBadge(task.status)}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {activeTab === 'scraping' && (
          <div className="space-y-6">
            <div className="bg-white rounded-lg shadow-md p-6">
              <h3 className="text-lg font-semibold mb-4">Запустить парсинг</h3>
              <div className="flex items-center space-x-4">
                <input
                  type="text"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  placeholder="Введите поисковый запрос (например, 'пуэр')"
                  className="flex-1 px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
                <button
                  onClick={startScraping}
                  disabled={loading}
                  className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 transition-colors"
                >
                  {loading ? '🔄 Запускаем...' : '🚀 Запустить'}
                </button>
              </div>
              
              <div className="mt-4 p-4 bg-blue-50 rounded-lg">
                <h4 className="font-medium text-blue-900 mb-2">Рекомендуемые запросы:</h4>
                <div className="flex flex-wrap gap-2">
                  {['пуэр', 'шэн пуэр', 'шу пуэр', 'улун', 'китайский чай', 'зелёный чай'].map(term => (
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
              <h3 className="text-lg font-semibold mb-4">Статистика парсинга</h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="p-4 bg-green-50 rounded-lg">
                  <div className="text-green-600 text-sm font-medium">Решено капч</div>
                  <div className="text-2xl font-bold text-green-900">{stats.captcha_solves || 0}</div>
                </div>
                <div className="p-4 bg-blue-50 rounded-lg">
                  <div className="text-blue-600 text-sm font-medium">Всего задач</div>
                  <div className="text-2xl font-bold text-blue-900">{stats.total_tasks || 0}</div>
                </div>
                <div className="p-4 bg-purple-50 rounded-lg">
                  <div className="text-purple-600 text-sm font-medium">Успешность</div>
                  <div className="text-2xl font-bold text-purple-900">
                    {stats.total_tasks > 0 ? ((stats.completed_tasks / stats.total_tasks) * 100).toFixed(1) : 0}%
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'products' && (
          <div className="space-y-6">
            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold">Товары ({products.length})</h3>
                <div className="flex items-center space-x-4">
                  <select
                    value={selectedTeaType}
                    onChange={(e) => setSelectedTeaType(e.target.value)}
                    className="px-3 py-2 border border-gray-300 rounded-lg"
                  >
                    <option value="">Все типы</option>
                    {categories.map(category => (
                      <option key={category._id} value={category._id}>
                        {category._id} ({category.count})
                      </option>
                    ))}
                  </select>
                  <button
                    onClick={fetchProducts}
                    className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                  >
                    🔄 Обновить
                  </button>
                </div>
              </div>
              
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {products.map(product => (
                  <TeaCard key={product.id} product={product} />
                ))}
              </div>
              
              {products.length === 0 && (
                <div className="text-center py-12">
                  <div className="text-6xl mb-4">🍃</div>
                  <p className="text-gray-500">Товары не найдены</p>
                  <p className="text-sm text-gray-400 mt-2">Запустите парсинг, чтобы получить данные</p>
                </div>
              )}
            </div>
          </div>
        )}

        {activeTab === 'tasks' && (
          <div className="space-y-6">
            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-lg font-semibold">История задач</h3>
                <button
                  onClick={fetchTasks}
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
                >
                  🔄 Обновить
                </button>
              </div>
              
              <div className="overflow-x-auto">
                <table className="w-full table-auto">
                  <thead>
                    <tr className="bg-gray-50">
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Поисковый запрос
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Статус
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Товары
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Создана
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Завершена
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {tasks.map(task => (
                      <tr key={task.id} className="hover:bg-gray-50">
                        <td className="px-4 py-4 whitespace-nowrap">
                          <div className="font-medium text-gray-900">{task.search_term}</div>
                          {task.error_message && (
                            <div className="text-sm text-red-600">{task.error_message}</div>
                          )}
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap">
                          {getStatusBadge(task.status)}
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap">
                          <div className="text-sm text-gray-900">
                            {task.scraped_products || 0} / {task.total_products || 0}
                          </div>
                          {task.failed_products > 0 && (
                            <div className="text-sm text-red-600">
                              {task.failed_products} ошибок
                            </div>
                          )}
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">
                          {formatDate(task.created_at)}
                        </td>
                        <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">
                          {formatDate(task.completed_at)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              
              {tasks.length === 0 && (
                <div className="text-center py-12">
                  <div className="text-6xl mb-4">⚙️</div>
                  <p className="text-gray-500">Задачи не найдены</p>
                </div>
              )}
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;