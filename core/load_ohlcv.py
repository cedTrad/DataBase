import pandas as pd

import time
from datetime import datetime

import requests
import sqlite3

from concurrent.futures import ThreadPoolExecutor
import threading

class FastBinanceDownloader:
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.base_url = "https://fapi.binance.com/fapi/v1/klines"
        self.session = requests.Session()
        self.lock = threading.Lock()
        self._init_db()
    
    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.close()
    
    def _create_table(self, symbol, interval):
        table_name = f"{symbol}_{interval}"
        conn = sqlite3.connect(self.db_path)
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS [{table_name}] (
                timestamp INTEGER PRIMARY KEY,
                open REAL, high REAL, low REAL, close REAL, volume REAL
            )
        """)
        conn.commit()
        conn.close()
    
    def _get_last_timestamp(self, symbol, interval):
        table_name = f"{symbol}_{interval}"
        conn = sqlite3.connect(self.db_path)
        try:
            result = conn.execute(f"SELECT MAX(timestamp) FROM [{table_name}]").fetchone()
            return result[0] if result[0] else None
        except:
            return None
        finally:
            conn.close()
    
    def _fetch_batch(self, symbol, interval, start_time, end_time, retries=10):
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'endTime': end_time,
            'limit': 1500
        }
        
        for attempt in range(retries):
            try:
                response = self.session.get(self.base_url, params=params, timeout=10)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limit
                    time.sleep(1 * (2 ** attempt))  # Exponential backoff
                    continue
            except Exception as e:
                if attempt == retries - 1:
                    print(f"Failed to fetch {symbol} after {retries} attempts: {e}")
                time.sleep(0.5 * (2 ** attempt))
        
        return []
    
    def _save_batch(self, symbol, interval, klines):
        if not klines:
            return
        
        table_name = f"{symbol}_{interval}"
        data = [(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[7])) for k in klines]
        
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.executemany(f"INSERT OR REPLACE INTO [{table_name}] VALUES (?,?,?,?,?,?)", data)
            conn.commit()
            conn.close()
    
    def _get_interval_ms(self, interval):
        intervals = {'1m': 60000, '5m': 300000, '15m': 900000, '1h': 3600000, '4h': 14400000, '1d': 86400000}
        return intervals.get(interval, 60000)
    
    def _download_chunk(self, symbol, interval, start_time, end_time):
        interval_ms = self._get_interval_ms(interval)
        current = start_time
        
        while current < end_time:
            chunk_end = min(current + (1500 * interval_ms), end_time)
            klines = self._fetch_batch(symbol, interval, current, chunk_end)
            
            if klines:
                self._save_batch(symbol, interval, klines)
                current = int(klines[-1][6]) + 1
                #print(f"{symbol} - {len(klines)} klines saved")
            else:
                break
            
            time.sleep(0.02)  # Minimal delay
    
    def download(self, symbol, interval, start_date, end_date=None, threads=8, fill_gaps=False):
        print(f"Downloading {symbol} {interval}...")
        
        self._create_table(symbol, interval)
        
        # Convert dates
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
        end_ts = int(datetime.now().timestamp() * 1000) if not end_date else int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
        
        # Check for existing data
        if not fill_gaps:
            last_ts = self._get_last_timestamp(symbol, interval)
            if last_ts:
                start_ts = last_ts + 1
                print(f"Resuming from {datetime.fromtimestamp(start_ts/1000)}")
            
        if start_ts >= end_ts:
            print("Already up to date")
            return
        
        # Split work across threads
        total_duration = end_ts - start_ts
        chunk_duration = total_duration // threads
        
        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = []
            for i in range(threads):
                chunk_start = start_ts + (i * chunk_duration)
                chunk_end = start_ts + ((i + 1) * chunk_duration) if i < threads - 1 else end_ts
                
                future = executor.submit(self._download_chunk, symbol, interval, chunk_start, chunk_end)
                futures.append(future)
            
            # Wait for completion
            for future in futures:
                future.result()
        
        print(f"✓ {symbol} {interval} completed")
   
    
    def get_data(self, symbol, interval, start_date=None, end_date=None, limit=None):
        """Récupère les données d'une table comme DataFrame"""
        table_name = f"{symbol}_{interval}"
        
        query = f"SELECT timestamp, open, high, low, close, volume FROM [{table_name}]"
        conditions = []
        
        if start_date:
            start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
            conditions.append(f"timestamp >= {start_ts}")
        
        if end_date:
            end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp() * 1000)
            conditions.append(f"timestamp <= {end_ts}")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY timestamp"
        
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql(query, conn)
            conn.close()
            
            if not df.empty:
                df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            
            return df
            
        except Exception as e:
            print(f"Error reading {table_name}: {e}")
            return pd.DataFrame()

    def download_multiple(self, symbols, interval, start_date, end_date=None):
        """Download multiple symbols sequentially"""
        for symbol in symbols:
            self.download(symbol, interval, start_date, end_date)
    
    def delete_table(self, symbol, interval):
        """Supprime la table correspondante au symbole et intervalle donnés."""
        table_name = f"{symbol}_{interval}"
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(f"DROP TABLE IF EXISTS [{table_name}]")
            conn.commit()
            print(f"Table {table_name} supprimée.")
        except Exception as e:
            print(f"Erreur lors de la suppression de la table {table_name}: {e}")
        finally:
            conn.close()
            
    def detect_gaps(self, symbol, interval):
        """Détecte les gaps dans la table symbol_interval. Retourne une liste de tuples (gap_start, gap_end) en datetime."""
        table_name = f"{symbol}_{interval}"
        interval_ms = self._get_interval_ms(interval)
        conn = sqlite3.connect(self.db_path)
        try:
            df = pd.read_sql(f"SELECT timestamp FROM [{table_name}] ORDER BY timestamp", conn)
            if len(df) < 2:
                return []
            diffs = df['timestamp'].diff().dropna()
            gap_indices = diffs[diffs > interval_ms].index
            gaps = []
            for idx in gap_indices:
                gap_start = df.loc[idx - 1, 'timestamp'] + interval_ms
                gap_end = df.loc[idx, 'timestamp'] - interval_ms
                gaps.append((datetime.fromtimestamp(gap_start/1000), datetime.fromtimestamp(gap_end/1000)))
            return gaps
        except Exception as e:
            print(f"Error detecting gaps in {table_name}: {e}")
            return []
        finally:
            conn.close()
    
    def fill_gaps(self, symbol, interval, threads=1):
        """Télécharge et comble automatiquement les gaps détectés pour une table donnée."""
        gaps = self.detect_gaps(symbol, interval)
        if not gaps:
            print(f"Aucun gap à combler pour {symbol}_{interval}.")
            return
        print(f"Comblement de {len(gaps)} gap(s) pour {symbol}_{interval}...")
        for gap_start, gap_end in gaps:
            # Ajoute une marge de sécurité d'un intervalle de chaque côté
            start_str = gap_start.strftime('%Y-%m-%d')
            end_str = gap_end.strftime('%Y-%m-%d')
            self.download(symbol, interval, start_str, end_str, threads=threads, fill_gaps=True)
        print(f"Comblement terminé pour {symbol}_{interval}.")
    
    
    def info_tables(self):
        """
        Retourne des infos sur chaque table : taille, first_date, last_date, gaps éventuels (liste).
        """
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            infos = []
            for table in tables:
                cursor.execute(f"SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM [{table}]")
                count, min_ts, max_ts = cursor.fetchone()
                if min_ts is not None and max_ts is not None:
                    first_date = datetime.fromtimestamp(min_ts/1000)
                    last_date = datetime.fromtimestamp(max_ts/1000)
                    # Vérification des gaps
                    symbol, interval = table.rsplit('_', 1)
                    gaps = self.detect_gaps(symbol, interval)
                    gap_ok = len(gaps) == 0
                else:
                    first_date = last_date = None
                    gap_ok = None
                    gaps = []
                infos.append({
                    'table': table,
                    'rows': count,
                    'first_date': first_date,
                    'last_date': last_date,
                    'no_gap': gap_ok,
                    'gaps': gaps
                })
            return pd.DataFrame(infos)
        finally:
            conn.close()



