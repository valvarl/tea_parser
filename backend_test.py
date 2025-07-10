#!/usr/bin/env python3
"""
Comprehensive Backend Testing for Chinese Tea Scraping System
Tests all API endpoints, database operations, and core functionality
"""

import requests
import json
import time
import sys
from datetime import datetime
from typing import Dict, List, Any

# Backend URL from frontend/.env
BACKEND_URL = "https://55dcae2e-605d-4eee-8e34-b0c44591c936.preview.emergentagent.com/api"

class TeaScrapingAPITester:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        self.test_results = []
        self.task_id = None
        
    def log_test(self, test_name: str, success: bool, details: str = "", response_data: Any = None):
        """Log test results"""
        result = {
            'test': test_name,
            'success': success,
            'details': details,
            'timestamp': datetime.now().isoformat(),
            'response_data': response_data
        }
        self.test_results.append(result)
        
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} {test_name}")
        if details:
            print(f"    Details: {details}")
        if not success and response_data:
            print(f"    Response: {response_data}")
        print()
    
    def test_root_endpoint(self):
        """Test GET /api/ - Root endpoint"""
        try:
            response = self.session.get(f"{self.base_url}/")
            
            if response.status_code == 200:
                data = response.json()
                if "message" in data and "Chinese Tea Scraper API" in data["message"]:
                    self.log_test("Root Endpoint", True, f"API responding correctly: {data['message']}")
                    return True
                else:
                    self.log_test("Root Endpoint", False, "Unexpected response format", data)
                    return False
            else:
                self.log_test("Root Endpoint", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Root Endpoint", False, f"Connection error: {str(e)}")
            return False
    
    def test_stats_endpoint(self):
        """Test GET /api/stats - Scraping statistics"""
        try:
            response = self.session.get(f"{self.base_url}/stats")
            
            if response.status_code == 200:
                data = response.json()
                required_fields = ['total_tasks', 'running_tasks', 'completed_tasks', 'failed_tasks', 'total_products']
                
                if all(field in data for field in required_fields):
                    self.log_test("Stats Endpoint", True, f"Stats retrieved: {data['total_tasks']} tasks, {data['total_products']} products")
                    return True
                else:
                    missing = [f for f in required_fields if f not in data]
                    self.log_test("Stats Endpoint", False, f"Missing fields: {missing}", data)
                    return False
            else:
                self.log_test("Stats Endpoint", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Stats Endpoint", False, f"Error: {str(e)}")
            return False
    
    def test_products_endpoint(self):
        """Test GET /api/products - List tea products"""
        try:
            response = self.session.get(f"{self.base_url}/products")
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    self.log_test("Products Endpoint", True, f"Retrieved {len(data)} products")
                    
                    # Test with filtering
                    filter_response = self.session.get(f"{self.base_url}/products?tea_type=пуэр&limit=10")
                    if filter_response.status_code == 200:
                        filter_data = filter_response.json()
                        self.log_test("Products Filtering", True, f"Filtered products: {len(filter_data)} items")
                    else:
                        self.log_test("Products Filtering", False, f"Filter failed: HTTP {filter_response.status_code}")
                    
                    return True
                else:
                    self.log_test("Products Endpoint", False, "Response is not a list", data)
                    return False
            else:
                self.log_test("Products Endpoint", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Products Endpoint", False, f"Error: {str(e)}")
            return False
    
    def test_categories_endpoint(self):
        """Test GET /api/categories - Tea categories"""
        try:
            response = self.session.get(f"{self.base_url}/categories")
            
            if response.status_code == 200:
                data = response.json()
                if "categories" in data and "total_types" in data:
                    self.log_test("Categories Endpoint", True, f"Found {data['total_types']} tea categories")
                    return True
                else:
                    self.log_test("Categories Endpoint", False, "Missing required fields", data)
                    return False
            else:
                self.log_test("Categories Endpoint", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Categories Endpoint", False, f"Error: {str(e)}")
            return False
    
    def test_search_queries_endpoint(self):
        """Test GET /api/search/queries - Generate search queries"""
        try:
            response = self.session.get(f"{self.base_url}/search/queries?limit=20")
            
            if response.status_code == 200:
                data = response.json()
                if "queries" in data and isinstance(data["queries"], list):
                    queries = data["queries"]
                    if len(queries) > 0 and "пуэр" in str(queries):
                        self.log_test("Search Queries Endpoint", True, f"Generated {len(queries)} search queries")
                        return True
                    else:
                        self.log_test("Search Queries Endpoint", False, "No valid tea queries found", data)
                        return False
                else:
                    self.log_test("Search Queries Endpoint", False, "Invalid response format", data)
                    return False
            else:
                self.log_test("Search Queries Endpoint", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Search Queries Endpoint", False, f"Error: {str(e)}")
            return False
    
    def test_start_scraping(self):
        """Test POST /api/scrape/start - Start scraping task"""
        try:
            # Start scraping with search term "пуэр"
            response = self.session.post(f"{self.base_url}/scrape/start?search_term=пуэр")
            
            if response.status_code == 200:
                data = response.json()
                if "task_id" in data and "status" in data:
                    self.task_id = data["task_id"]
                    self.log_test("Start Scraping", True, f"Task started: {self.task_id}, Status: {data['status']}")
                    return True
                else:
                    self.log_test("Start Scraping", False, "Missing task_id or status", data)
                    return False
            else:
                self.log_test("Start Scraping", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Start Scraping", False, f"Error: {str(e)}")
            return False
    
    def test_task_status(self):
        """Test GET /api/scrape/status/{task_id} - Check task status"""
        if not self.task_id:
            self.log_test("Task Status", False, "No task_id available from previous test")
            return False
            
        try:
            response = self.session.get(f"{self.base_url}/scrape/status/{self.task_id}")
            
            if response.status_code == 200:
                data = response.json()
                required_fields = ['id', 'search_term', 'status', 'created_at']
                
                if all(field in data for field in required_fields):
                    status = data['status']
                    search_term = data['search_term']
                    self.log_test("Task Status", True, f"Task {self.task_id}: {status}, Search: {search_term}")
                    
                    # Wait a bit and check again to see if status changes
                    time.sleep(3)
                    response2 = self.session.get(f"{self.base_url}/scrape/status/{self.task_id}")
                    if response2.status_code == 200:
                        data2 = response2.json()
                        new_status = data2['status']
                        self.log_test("Task Status Update", True, f"Status after 3s: {new_status}")
                    
                    return True
                else:
                    missing = [f for f in required_fields if f not in data]
                    self.log_test("Task Status", False, f"Missing fields: {missing}", data)
                    return False
            else:
                self.log_test("Task Status", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Task Status", False, f"Error: {str(e)}")
            return False
    
    def test_list_tasks(self):
        """Test GET /api/scrape/tasks - List all tasks"""
        try:
            response = self.session.get(f"{self.base_url}/scrape/tasks")
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    self.log_test("List Tasks", True, f"Retrieved {len(data)} scraping tasks")
                    
                    # Verify our task is in the list
                    if self.task_id:
                        task_found = any(task.get('id') == self.task_id for task in data)
                        if task_found:
                            self.log_test("Task in List", True, f"Created task {self.task_id} found in task list")
                        else:
                            self.log_test("Task in List", False, f"Created task {self.task_id} not found in list")
                    
                    return True
                else:
                    self.log_test("List Tasks", False, "Response is not a list", data)
                    return False
            else:
                self.log_test("List Tasks", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("List Tasks", False, f"Error: {str(e)}")
            return False
    
    def test_export_csv(self):
        """Test GET /api/export/csv - Export products to CSV"""
        try:
            response = self.session.get(f"{self.base_url}/export/csv")
            
            if response.status_code == 200:
                data = response.json()
                if "data" in data and "count" in data:
                    count = data["count"]
                    csv_data = data["data"]
                    
                    if isinstance(csv_data, list):
                        self.log_test("Export CSV", True, f"Exported {count} products to CSV format")
                        
                        # Verify CSV structure if data exists
                        if count > 0 and len(csv_data) > 0:
                            sample = csv_data[0]
                            expected_fields = ['id', 'name', 'price', 'tea_type']
                            has_fields = any(field in sample for field in expected_fields)
                            if has_fields:
                                self.log_test("CSV Structure", True, "CSV data has expected fields")
                            else:
                                self.log_test("CSV Structure", False, f"CSV missing expected fields", sample)
                        
                        return True
                    else:
                        self.log_test("Export CSV", False, "CSV data is not a list", data)
                        return False
                else:
                    self.log_test("Export CSV", False, "Missing data or count fields", data)
                    return False
            else:
                self.log_test("Export CSV", False, f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Export CSV", False, f"Error: {str(e)}")
            return False
    
    def test_error_handling(self):
        """Test error handling scenarios"""
        # Test invalid task ID
        try:
            response = self.session.get(f"{self.base_url}/scrape/status/invalid-task-id")
            if response.status_code == 404:
                self.log_test("Error Handling - Invalid Task ID", True, "Correctly returns 404 for invalid task ID")
            else:
                self.log_test("Error Handling - Invalid Task ID", False, f"Expected 404, got {response.status_code}")
        except Exception as e:
            self.log_test("Error Handling - Invalid Task ID", False, f"Error: {str(e)}")
        
        # Test invalid product ID
        try:
            response = self.session.get(f"{self.base_url}/products/invalid-product-id")
            if response.status_code == 404:
                self.log_test("Error Handling - Invalid Product ID", True, "Correctly returns 404 for invalid product ID")
            else:
                self.log_test("Error Handling - Invalid Product ID", False, f"Expected 404, got {response.status_code}")
        except Exception as e:
            self.log_test("Error Handling - Invalid Product ID", False, f"Error: {str(e)}")
    
    def test_database_integration(self):
        """Test database operations and data persistence"""
        # This is tested implicitly through other endpoints
        # Check if stats show database activity
        try:
            response = self.session.get(f"{self.base_url}/stats")
            if response.status_code == 200:
                data = response.json()
                total_tasks = data.get('total_tasks', 0)
                total_products = data.get('total_products', 0)
                
                if total_tasks >= 0 and total_products >= 0:
                    self.log_test("Database Integration", True, f"Database accessible: {total_tasks} tasks, {total_products} products stored")
                    return True
                else:
                    self.log_test("Database Integration", False, "Invalid database counts", data)
                    return False
            else:
                self.log_test("Database Integration", False, f"Cannot access stats: HTTP {response.status_code}")
                return False
        except Exception as e:
            self.log_test("Database Integration", False, f"Database error: {str(e)}")
            return False
    
    def run_all_tests(self):
        """Run all backend tests"""
        print("=" * 60)
        print("CHINESE TEA SCRAPING SYSTEM - BACKEND API TESTS")
        print("=" * 60)
        print(f"Testing backend at: {self.base_url}")
        print(f"Started at: {datetime.now().isoformat()}")
        print()
        
        # Core API endpoints
        self.test_root_endpoint()
        self.test_stats_endpoint()
        self.test_products_endpoint()
        self.test_categories_endpoint()
        self.test_search_queries_endpoint()
        
        # Scraping functionality
        self.test_start_scraping()
        self.test_task_status()
        self.test_list_tasks()
        
        # Export functionality
        self.test_export_csv()
        
        # Error handling
        self.test_error_handling()
        
        # Database integration
        self.test_database_integration()
        
        # Summary
        self.print_summary()
    
    def print_summary(self):
        """Print test summary"""
        print("=" * 60)
        print("TEST SUMMARY")
        print("=" * 60)
        
        passed = sum(1 for result in self.test_results if result['success'])
        failed = len(self.test_results) - passed
        
        print(f"Total Tests: {len(self.test_results)}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Success Rate: {(passed/len(self.test_results)*100):.1f}%")
        print()
        
        if failed > 0:
            print("FAILED TESTS:")
            for result in self.test_results:
                if not result['success']:
                    print(f"❌ {result['test']}: {result['details']}")
            print()
        
        print("CRITICAL FUNCTIONALITY STATUS:")
        critical_tests = [
            "Root Endpoint",
            "Start Scraping", 
            "Task Status",
            "Products Endpoint",
            "Database Integration"
        ]
        
        for test_name in critical_tests:
            result = next((r for r in self.test_results if r['test'] == test_name), None)
            if result:
                status = "✅ WORKING" if result['success'] else "❌ FAILED"
                print(f"{status} {test_name}")
        
        print()
        print(f"Completed at: {datetime.now().isoformat()}")

if __name__ == "__main__":
    tester = TeaScrapingAPITester(BACKEND_URL)
    tester.run_all_tests()