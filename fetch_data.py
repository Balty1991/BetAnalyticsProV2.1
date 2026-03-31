#!/usr/bin/env python3
"""
BetAnalytics Pro V2 - GitHub Actions Data Fetcher
Rulează automat la fiecare 30 minute și salvează datele API ca JSON static.
Aplicația HTML citește aceste fișiere fără probleme CORS.
"""

import os
import json
import requests
from datetime import datetime, timezone

TOKEN = os.environ.get('BSD_TOKEN', '')
API_BASE = 'https://sports.bzzoiro.com'
HEADERS = {'Authorization': f'Token {TOKEN}'}
TZ = 'Europe/Bucharest'

# Fallback: citește datele din V1 (BetAnalyticsPro) dacă tokenul nu e setat
V1_BASE = 'https://balty1991.github.io/BetAnalyticsPro/data'

def fetch_url(url):
    """Fetch date de la un URL complet cu retry."""
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  Attempt {attempt+1} failed for {url}: {e}")
            if attempt == 2:
                return None
    return None

def fetch_all_pages(endpoint):
    """Fetch toate paginile pentru un endpoint dat."""
    all_results = []
    next_url = f"{API_BASE}{endpoint}"
    
    print(f"Fetching all pages for {endpoint}...")
    
    page_count = 0
    while next_url:
        page_count += 1
        print(f"  Page {page_count}...")
        data = fetch_url(next_url)
        if not data:
            break
            
        # Dacă datele sunt o listă (unele endpoint-uri pot returna direct lista)
        if isinstance(data, list):
            all_results.extend(data)
            break
            
        # Dacă datele sunt paginate (au 'results' și 'next')
        results = data.get('results', [])
        all_results.extend(results)
        
        next_url = data.get('next')
        # Asigură-te că next_url folosește HTTPS dacă API-ul returnează HTTP din greșeală
        if next_url and next_url.startswith('http://'):
            next_url = next_url.replace('http://', 'https://', 1)
            
    return all_results

def fetch_from_v1(filename):
    """Fallback: citeste datele din V1 GitHub Pages."""
    url = f"{V1_BASE}/{filename}?t={int(datetime.now().timestamp())}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        print(f"  Fallback V1 OK: {url}")
        return data
    except Exception as e:
        print(f"  Fallback V1 FAIL: {e}")
        return None

def save_json(data, filename):
    """Salvează datele ca JSON în directorul data/."""
    os.makedirs('data', exist_ok=True)
    path = f'data/{filename}'
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    size = os.path.getsize(path)
    print(f"  Salvat: {path} ({size} bytes)")

def main():
    print(f"=== BetAnalytics V2 Fetch [{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}] ===")

    all_predictions = []
    all_live = []

    if TOKEN:
        # Fetch Predictions cu paginare
        all_predictions = fetch_all_pages(f'/api/predictions/?tz={TZ}')
        print(f"  Total predictions: {len(all_predictions)}")

        # Fetch Live cu paginare
        all_live = fetch_all_pages('/api/live/')
        print(f"  Total live: {len(all_live)}")
    else:
        print("WARN: BSD_TOKEN nu este setat - folosim fallback V1")

    # Fallback la V1 dacă API-ul nu a returnat nimic
    if not all_predictions:
        print("Fallback: citire predictions din V1...")
        v1_data = fetch_from_v1('predictions.json')
        if v1_data:
            all_predictions = v1_data.get('results', v1_data) if isinstance(v1_data, (dict, list)) else []

    if not all_live:
        print("Fallback: citire live din V1...")
        v1_live = fetch_from_v1('live.json')
        if v1_live:
            all_live = v1_live.get('results', v1_live) if isinstance(v1_live, (dict, list)) else []

    # Salvare date (păstrăm formatul listă pentru a fi compatibil cu index.html)
    save_json(all_predictions, 'predictions.json')
    save_json(all_live, 'live.json')

    # Metadata
    meta = {
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'predictions_count': len(all_predictions),
        'live_count': len(all_live),
        'status': 'ok'
    }
    save_json(meta, 'meta.json')
    print(f"Meta salvat: {meta}")
    print("=== Done ===")

if __name__ == '__main__':
    main()
