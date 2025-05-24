import aiosqlite
import sqlite3
import hashlib
import pickle
import asyncio
import threading
import queue
import time
from typing import Any, Optional
import logging


class PersistentMemory:
    def __init__(self, database_file: str = 'persistent_memory.db', key_error: bool = False):
        self.logger = logging.getLogger('skpmem')
        self.database_file = database_file
        self.memory_store: dict = {}
        self.write_queue: asyncio.Queue = asyncio.Queue()
        self.initialized: bool = False
        self._init_task: Optional[asyncio.Task] = None
        
        # 同期操作用のバックグラウンドスレッド関連
        self.sync_write_queue: queue.Queue = queue.Queue()
        self.sync_read_queue: queue.Queue = queue.Queue()
        self.sync_response_queue: queue.Queue = queue.Queue()
        self.daemon_thread: Optional[threading.Thread] = None
        self.daemon_running: bool = False
        self.sync_initialized: bool = False
        self._lock = threading.Lock()
        self.deleted_keys: set = set()  # 削除されたキーを追跡

        # KeyErrorの発生を抑制するかどうか
        self.key_error = key_error

    def __getitem__(self, key: str) -> Any:
        """同期版の load_sync を呼び出す（存在しない場合はKeyErrorを発生）"""
        # 特別なセンチネル値を使用してキーの存在を確認
        _sentinel = object()
        result = self.load_sync(key, _sentinel)
        if result is _sentinel:
            if self.key_error:
                raise KeyError(key)
            else:
                return None
        return result

    def __setitem__(self, key: str, value: Any):
        """同期版の save_sync を呼び出す"""
        self.save_sync(key, value)

    def __delitem__(self, key: str):
        """同期版の delete_sync を呼び出す"""
        self.delete_sync(key)

    def delete_sync(self, key: str):
        """同期版の削除メソッド - バックグラウンドスレッドで削除"""
        hash_key = self._name_hash(key)
        
        # メモリキャッシュから即座に削除し、削除済みマークを付ける
        with self._lock:
            if hash_key in self.memory_store:
                del self.memory_store[hash_key]
            self.deleted_keys.add(hash_key)  # 削除済みマークを追加
        
        # バックグラウンドスレッドに削除を依頼
        try:
            self.sync_write_queue.put(('delete', hash_key), timeout=1.0)
            self.logger.debug(f"Queued {key} for background deletion")
        except queue.Full:
            self.logger.warning(f"Write queue full, falling back to direct delete for {key}")
            # フォールバック：直接削除
            try:
                with sqlite3.connect(self.database_file) as db:
                    db.execute('DELETE FROM memory WHERE key = ?', (hash_key,))
                    db.commit()
                    self.logger.debug(f"Direct deleted {key} from DB")
            except Exception as e:
                self.logger.error(f"Error in direct delete fallback: {e}")

    async def _delete_from_db(self, hash_key: str):
        if not self.initialized:
            await self.initialize()
        try:
            async with aiosqlite.connect(self.database_file) as db:
                await db.execute('DELETE FROM memory WHERE key = ?', (hash_key,))
                await db.commit()
                self.logger.debug(f"Deleted {hash_key} from DB")
        except Exception as e:
            self.logger.error(f"Error deleting from DB: {e}")

    async def initialize(self):
        if self.initialized:
            return

        # データベースの初期化
        async with aiosqlite.connect(self.database_file) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS memory (
                    key TEXT PRIMARY KEY,
                    value BLOB
                )
            ''')
            await db.commit()

        # データベースへの非同期書き込みタスク開始
        self._init_task = asyncio.create_task(self._async_db_writer())
        self.initialized = True
        self._start_daemon_thread()

    def _start_daemon_thread(self):
        """バックグラウンドデーモンスレッドを開始"""
        if not self.daemon_running:
            self.daemon_running = True
            self.daemon_thread = threading.Thread(target=self._daemon_worker, daemon=True)
            self.daemon_thread.start()
            self.logger.debug("Daemon thread started")

    def _daemon_worker(self):
        """バックグラウンドでデータベース操作を処理するデーモンワーカー"""
        try:
            with sqlite3.connect(self.database_file) as db:
                self.logger.debug("Daemon worker connected to database")
                
                while self.daemon_running:
                    try:
                        # 書き込み操作をチェック
                        try:
                            operation, data = self.sync_write_queue.get(timeout=0.1)
                            if operation == 'save':
                                hash_key, value = data
                                db.execute(
                                    'REPLACE INTO memory (key, value) VALUES (?, ?)',
                                    (hash_key, pickle.dumps(value))
                                )
                                db.commit()
                                self.logger.debug(f"Daemon saved {hash_key} to DB")
                            elif operation == 'delete':
                                hash_key = data
                                db.execute('DELETE FROM memory WHERE key = ?', (hash_key,))
                                db.commit()
                                self.logger.debug(f"Daemon deleted {hash_key} from DB")
                            self.sync_write_queue.task_done()
                        except queue.Empty:
                            pass
                        
                        # 読み込み操作をチェック
                        try:
                            request_id, hash_key = self.sync_read_queue.get(timeout=0.1)
                            cursor = db.execute(
                                'SELECT value FROM memory WHERE key = ?',
                                (hash_key,)
                            )
                            row = cursor.fetchone()
                            result = pickle.loads(row[0]) if row is not None else None
                            self.sync_response_queue.put((request_id, result))
                            self.sync_read_queue.task_done()
                            self.logger.debug(f"Daemon loaded {hash_key} from DB")
                        except queue.Empty:
                            pass
                            
                    except Exception as e:
                        self.logger.error(f"Error in daemon worker: {e}")
                        time.sleep(0.1)
                        
        except Exception as e:
            self.logger.error(f"Fatal error in daemon worker: {e}")
        finally:
            self.logger.debug("Daemon worker stopped")

    def initialize_sync(self):
        """同期版の初期化メソッド"""
        if self.sync_initialized:
            return

        # データベースの初期化（同期版）
        with sqlite3.connect(self.database_file) as db:
            db.execute('''
                CREATE TABLE IF NOT EXISTS memory (
                    key TEXT PRIMARY KEY,
                    value BLOB
                )
            ''')
            db.commit()

        self.sync_initialized = True
        self._start_daemon_thread()

    def _name_hash(self, name: str) -> str:
        return hashlib.sha512(name.encode()).hexdigest()

    async def save(self, key: str, val: Any):
        if not self.initialized:
            await self.initialize()
        
        self.logger.debug(f"Saving {key} to memory")

        hash_key = self._name_hash(key)
        self.memory_store[hash_key] = val
        await self.write_queue.put((hash_key, val))

    def save_sync(self, key: str, val: Any):
        """同期版の save メソッド - バックグラウンドスレッドで永続化"""
        if not self.sync_initialized:
            self.initialize_sync()
        
        self.logger.debug(f"Saving {key} to memory (sync)")

        hash_key = self._name_hash(key)
        
        # メモリキャッシュに即座に保存（高速応答）
        with self._lock:
            self.memory_store[hash_key] = val
            # 保存時に削除済みマークを削除
            self.deleted_keys.discard(hash_key)
        
        # バックグラウンドスレッドに永続化を依頼（非ブロッキング）
        try:
            self.sync_write_queue.put(('save', (hash_key, val)), timeout=1.0)
            self.logger.debug(f"Queued {key} for background save")
        except queue.Full:
            self.logger.warning(f"Write queue full, falling back to direct save for {key}")
            # キューが満杯の場合は直接保存にフォールバック
            try:
                with sqlite3.connect(self.database_file) as db:
                    db.execute(
                        'REPLACE INTO memory (key, value) VALUES (?, ?)',
                        (hash_key, pickle.dumps(val))
                    )
                    db.commit()
                    self.logger.debug(f"Direct saved {key} to DB (sync)")
            except Exception as e:
                self.logger.error(f"Error in direct save fallback: {e}")

    async def _async_db_writer(self):
        if not self.initialized:
            await self.initialize()
        
        async with aiosqlite.connect(self.database_file) as db:
            while True:
                await asyncio.sleep(0.1)
                hash_key, val = await self.write_queue.get()
                try:
                    self.logger.debug(f"Writing {hash_key} to DB")
                    await db.execute(
                        'REPLACE INTO memory (key, value) VALUES (?, ?)',
                        (hash_key, pickle.dumps(val))
                    )
                    await db.commit()
                    self.logger.debug(f"Write complete for {hash_key}")
                except Exception as e:
                    self.logger.error(f"Error writing to DB: {e}")
                finally:
                    self.write_queue.task_done()

    async def load(self, key: str, defval: Any = None) -> Any:
        if not self.initialized:
            await self.initialize()
        
        hash_key = self._name_hash(key)
        if hash_key in self.memory_store:
            self.logger.debug(f"Loaded {key} from memory")
            return self.memory_store[hash_key]

        try:
            async with aiosqlite.connect(self.database_file) as db:
                async with db.execute(
                    'SELECT value FROM memory WHERE key = ?',
                    (hash_key,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if row is not None:
                        value = pickle.loads(row[0])
                        self.memory_store[hash_key] = value
                        self.logger.debug(f"Loaded {key} from DB")
                        return value
        except Exception as e:
            self.logger.error(f"Error reading from DB: {e}")
        
        return defval

    def load_sync(self, key: str, defval: Any = None) -> Any:
        """同期版の load メソッド - バックグラウンドスレッドで読み込み"""
        if not self.sync_initialized:
            self.initialize_sync()
        
        hash_key = self._name_hash(key)
        
        # 削除済みキーのチェック
        with self._lock:
            if hash_key in self.deleted_keys:
                self.logger.debug(f"Key {key} was deleted")
                return defval
            
            # メモリキャッシュから高速読み込み
            if hash_key in self.memory_store:
                self.logger.debug(f"Loaded {key} from memory (sync)")
                return self.memory_store[hash_key]

        # 直接データベースから読み込み
        try:
            with sqlite3.connect(self.database_file) as db:
                cursor = db.execute(
                    'SELECT value FROM memory WHERE key = ?',
                    (hash_key,)
                )
                row = cursor.fetchone()
                if row is not None:
                    value = pickle.loads(row[0])
                    with self._lock:
                        self.memory_store[hash_key] = value
                    self.logger.debug(f"Direct loaded {key} from DB (sync)")
                    return value
        except Exception as e:
            self.logger.error(f"Error in direct read fallback: {e}")
        
        return defval

    async def close(self):
        """残りのキュー項目を処理し、リソースをクリーンアップします"""
        if self._init_task:
            await self.write_queue.join()
            self._init_task.cancel()
        
        # 同期操作のクリーンアップ
        self.close_sync()

    def close_sync(self):
        """同期版のクリーンアップ"""
        if self.daemon_running:
            self.logger.debug("Stopping daemon thread...")
            self.daemon_running = False
            
            # 残りの書き込み操作を完了させる
            if self.daemon_thread and self.daemon_thread.is_alive():
                try:
                    # 最大5秒待機
                    self.daemon_thread.join(timeout=5.0)
                    if self.daemon_thread.is_alive():
                        self.logger.warning("Daemon thread did not stop gracefully")
                    else:
                        self.logger.debug("Daemon thread stopped successfully")
                except Exception as e:
                    self.logger.error(f"Error stopping daemon thread: {e}")

    def flush_sync(self):
        """同期版のフラッシュ - 全ての保留中の書き込み操作を完了"""
        if not self.daemon_running:
            return
            
        # 書き込みキューが空になるまで待機
        start_time = time.time()
        initial_size = self.sync_write_queue.qsize()
        
        while not self.sync_write_queue.empty() and time.time() - start_time < 5.0:
            time.sleep(0.05)  # より短い間隔でチェック
        
        final_size = self.sync_write_queue.qsize()
        
        if not self.sync_write_queue.empty():
            self.logger.warning(f"Flush timeout: {final_size} writes may not be completed (started with {initial_size})")
        else:
            self.logger.debug(f"All {initial_size} pending writes completed")
    
    async def flush(self):
        await self.write_queue.join()
    
    async def __aenter__(self):
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


# 使用例
async def main():

    logging.basicConfig(level=logging.INFO)

    # インスタンス作成
    async with PersistentMemory('my_custom_database.db') as mem:
        # メモリに保存
        await mem.save("test_key", "test_value")
        # メモリから読み込み
        value = await mem.load("test_key")
        print(f"Loaded value1: {value}")

        counter = await mem.load("counter", 0)
        print(f"Counter: {counter}")
        counter += 1
        await mem.save("counter", counter)
        
        # 非同期版では辞書インターフェースは使用しない（同期版で使用）
        await mem.save("test_key 3", "test_value 3")
        value = await mem.load("test_key 3")
        print(f"test_key 3: {value}")

        
    # インスタンス作成
    async with PersistentMemory('my_custom_database2.db') as mem:
        # メモリに保存
        await mem.save("test_key2", "test_value2")
        # メモリから読み込み
        value = await mem.load("test_key2")
        print(f"Loaded value3: {value}")

        value = await mem.load("test_key3", {})
        print(f"Loaded value2: {value}")
        value["count"] = value.get("count", 0) + 1
        await mem.save("test_key3", value)

    # 同期版の使用例（辞書インターフェース使用）
    print("\n--- 同期版の使用例（辞書インターフェース） ---")
    mem_sync = PersistentMemory('sync_test.db')
    
    try:
        # 辞書のようにデータを保存
        mem_sync["sync_key"] = "sync_value"
        mem_sync["sync_counter"] = 100
        mem_sync["user_data"] = {"name": "Bob", "age": 30}
        
        # 辞書のようにデータを読み込み
        sync_value = mem_sync["sync_key"]
        print(f"Sync loaded value: {sync_value}")
        
        sync_counter = mem_sync["sync_counter"]
        print(f"Sync counter: {sync_counter}")
        
        user_data = mem_sync["user_data"]
        print(f"User data: {user_data}")
        
        # カウンターを増加
        mem_sync["sync_counter"] = sync_counter + 1
        print(f"Updated counter: {mem_sync['sync_counter']}")
        
        # 存在しないキーのデフォルト値テスト
        default_value = mem_sync.load_sync("nonexistent_key", "default")
        print(f"Default value: {default_value}")
        
        # 削除のテスト
        print(f"Before deletion: {mem_sync['sync_key']}")
        del mem_sync["sync_key"]
        
        try:
            deleted_value = mem_sync["sync_key"]
            if deleted_value is not None:
                print(f"⚠️ 削除されたキーから値が取得されました: {deleted_value}")
            else:
                print("✓ 削除されたキーから値が取得されませんでした (None)")
        except KeyError:
            print("✓ 削除されたキーでKeyErrorが発生（期待通り）")
        
        # 全ての書き込み操作が完了するまで待機
        print("Flushing pending operations...")
        mem_sync.flush_sync()
        print("Flush completed")
        
    finally:
        # クリーンアップ
        print("Cleaning up...")
        mem_sync.close_sync()
        print("Cleanup completed")


if __name__ == "__main__":
    asyncio.run(main())