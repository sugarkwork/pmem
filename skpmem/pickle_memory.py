import os
import pickle
import threading
import queue
import time
import logging
from typing import Any, Dict, Optional
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

@dataclass
class WriteOperation:
    """書き込み操作を表すデータクラス"""
    data: Dict[str, Any]
    timestamp: datetime

class CachedStore:
    def __init__(self, file_path: str = 'cached_store.pickle', 
                 write_interval: float = 0.1):
        self.file_path = file_path
        self.write_interval = write_interval
        self._cache: Dict[str, Any] = {}
        self._cache_lock = threading.RLock()
        self._write_queue: queue.Queue[WriteOperation] = queue.Queue()
        self._shutdown_flag = threading.Event()
        
        # ロガーの設定
        self.logger = logging.getLogger('CachedStore')
        self.logger.setLevel(logging.DEBUG)
        
        # 初期データの読み込み
        self._load_initial_data()
        
        # 書き込みデーモンの開始
        self._writer_thread = threading.Thread(
            target=self._writer_daemon, 
            daemon=True,
            name="StoreWriter"
        )
        self._writer_thread.start()

    def _load_initial_data(self):
        """初期データをファイルから読み込む"""
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'rb') as f:
                    self._cache = pickle.load(f)
                self.logger.info(f"Loaded {len(self._cache)} items from {self.file_path}")
            except (EOFError, pickle.PickleError) as e:
                self.logger.error(f"Error loading data: {e}")
                self._cache = {}
        else:
            self._cache = {}
            self._save_to_file(self._cache)  # 空のファイルを作成

    def _writer_daemon(self):
        """非同期で書き込みを行うデーモンスレッド"""
        last_write = None
        
        while not self._shutdown_flag.is_set():
            try:
                # キューから最新の書き込み操作を取得
                operation = self._write_queue.get(timeout=self.write_interval)
                
                # 待機中の全ての書き込み操作を取得
                while not self._write_queue.empty():
                    operation = self._write_queue.get_nowait()
                
                # ファイルに書き込み
                self._save_to_file(operation.data)
                last_write = operation.timestamp
                
                self.logger.debug(f"Wrote data to file at {last_write}")
                
            except queue.Empty:
                # タイムアウト - 特に何もしない
                pass
            except Exception as e:
                self.logger.error(f"Error in writer daemon: {e}")
            
            # 書き込み間隔を設ける
            time.sleep(self.write_interval)

    def _save_to_file(self, data: Dict[str, Any]):
        """ファイルへの実際の書き込みを行う"""
        temp_file = f"{self.file_path}.tmp"
        try:
            with open(temp_file, 'wb') as f:
                pickle.dump(data, f)
            os.replace(temp_file, self.file_path)
        except Exception as e:
            self.logger.error(f"Error saving to file: {e}")
            if os.path.exists(temp_file):
                os.remove(temp_file)
            raise

    def __setitem__(self, key: str, value: Any):
        """キャッシュを更新し、書き込みキューに追加"""
        with self._cache_lock:
            self._cache[key] = value
            # 現在のキャッシュ全体のコピーを書き込みキューに追加
            self._write_queue.put(WriteOperation(
                data=self._cache.copy(),
                timestamp=datetime.now()
            ))

    def __getitem__(self, key: str) -> Any:
        """キャッシュから値を取得"""
        with self._cache_lock:
            if key not in self._cache:
                raise KeyError(key)
            return self._cache[key]

    def get(self, key: str, default: Any = None) -> Any:
        """キャッシュから値を取得（デフォルト値あり）"""
        with self._cache_lock:
            return self._cache.get(key, default)

    def __contains__(self, key: str) -> bool:
        """キーの存在確認"""
        with self._cache_lock:
            return key in self._cache

    def delete(self, key: str) -> None:
        """キーの削除"""
        with self._cache_lock:
            if key in self._cache:
                del self._cache[key]
                self._write_queue.put(WriteOperation(
                    data=self._cache.copy(),
                    timestamp=datetime.now()
                ))

    def clear(self) -> None:
        """全データの削除"""
        with self._cache_lock:
            self._cache.clear()
            self._write_queue.put(WriteOperation(
                data={},
                timestamp=datetime.now()
            ))

    def close(self):
        """ストアを適切にクローズ"""
        self._shutdown_flag.set()
        self._writer_thread.join(timeout=5.0)
        
        # 最後の書き込みを確実に行う
        if not self._write_queue.empty():
            last_operation = None
            while not self._write_queue.empty():
                last_operation = self._write_queue.get_nowait()
            if last_operation:
                self._save_to_file(last_operation.data)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

# 使用例
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    
    with CachedStore("test_cached_store.pickle") as store:
        # データの保存
        store['key1'] = 'value1'
        store['key2'] = [1, 2, 3]
        store['key3'] = {'name': 'John', 'age': 30}
        
        # 少し待機して書き込みが行われるのを確認
        time.sleep(0.2)
        
        # データの読み込み
        print(f"key1: {store['key1']}")
        print(f"key2: {store['key2']}")
        print(f"key3: {store['key3']}")
        
        # 存在確認
        print(f"'key1' exists: {'key1' in store}")
        print(f"'unknown' exists: {'unknown' in store}")
        
        # デフォルト値付きの取得
        print(f"Default value: {store.get('unknown', 'default')}")
        
        # データの削除
        store.delete('key1')
        print(f"After delete - 'key1' exists: {'key1' in store}")
        
        # 全データの削除
        store.clear()
        print(f"After clear - store empty: {store.get('key2') is None}")