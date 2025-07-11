#!/usr/bin/env python3
"""
Quick test of scraping functionality
"""

import requests
import time
import json

BACKEND_URL = "http://localhost:8000/api"

def test_scraping_workflow():
    print("Testing complete scraping workflow...")
    
    # Start scraping
    response = requests.post(f"{BACKEND_URL}/scrape/start?search_term=пуэр")
    if response.status_code == 200:
        data = response.json()
        task_id = data["task_id"]
        print(f"✅ Started scraping task: {task_id}")
        
        # Monitor task for 30 seconds
        for i in range(6):  # Check every 5 seconds for 30 seconds
            time.sleep(5)
            status_response = requests.get(f"{BACKEND_URL}/scrape/status/{task_id}")
            if status_response.status_code == 200:
                status_data = status_response.json()
                status = status_data["status"]
                scraped = status_data.get("scraped_products", 0)
                total = status_data.get("total_products", 0)
                
                print(f"Status check {i+1}: {status} - {scraped}/{total} products")
                
                if status == "completed":
                    print(f"✅ Scraping completed successfully! Found {total} products")
                    break
                elif status == "failed":
                    error = status_data.get("error_message", "Unknown error")
                    print(f"❌ Scraping failed: {error}")
                    break
            else:
                print(f"❌ Failed to get status: {status_response.status_code}")
                break
        
        # Check final stats
        stats_response = requests.get(f"{BACKEND_URL}/stats")
        if stats_response.status_code == 200:
            stats = stats_response.json()
            print(f"\nFinal stats: {stats['total_tasks']} tasks, {stats['total_products']} products")
        
        # Check if any products were scraped
        products_response = requests.get(f"{BACKEND_URL}/products?limit=5")
        if products_response.status_code == 200:
            products = products_response.json()
            print(f"Products in database: {len(products)}")
            if products:
                print("Sample product:", products[0].get("name", "No name"))
    else:
        print(f"❌ Failed to start scraping: {response.status_code}")

if __name__ == "__main__":
    test_scraping_workflow()