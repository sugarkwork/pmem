import sqlite3
import hashlib
import pickle
import logging
from typing import Any, Optional


class PersistentMemory:
    def __init__(self, database_file: str = 'persistent_memory.db'):
        self.logger = logging.getLogger('pmem')
        self.database_file = database_file
        self.memory_store: dict = {}
        self.initialized: bool = False

    def initialize(self):
        if self.initialized:
            return

        # データベースの初期化
        with sqlite3.connect(self.database_file) as db:
            db.execute('''
                CREATE TABLE IF NOT EXISTS memory (
                    key TEXT PRIMARY KEY,
                    value BLOB
                )
            ''')
            db.commit()

        self.initialized = True

    def _name_hash(self, name: str) -> str:
        return hashlib.sha512(name.encode()).hexdigest()

    def save(self, key: str, val: Any):
        if not self.initialized:
            self.initialize()
        
        self.logger.debug(f"Saving {key} to memory")

        hash_key = self._name_hash(key)
        self.memory_store[hash_key] = val
        
        try:
            with sqlite3.connect(self.database_file) as db:
                self.logger.debug(f"Writing {hash_key} to DB")
                db.execute(
                    'REPLACE INTO memory (key, value) VALUES (?, ?)',
                    (hash_key, pickle.dumps(val))
                )
                db.commit()
                self.logger.debug(f"Write complete for {hash_key}")
        except Exception as e:
            self.logger.error(f"Error writing to DB: {e}")

    def load(self, key: str, defval: Any = None) -> Any:
        if not self.initialized:
            self.initialize()
        
        hash_key = self._name_hash(key)
        if hash_key in self.memory_store:
            self.logger.debug(f"Loaded {key} from memory")
            return self.memory_store[hash_key]

        try:
            with sqlite3.connect(self.database_file) as db:
                cursor = db.execute(
                    'SELECT value FROM memory WHERE key = ?',
                    (hash_key,)
                )
                row = cursor.fetchone()
                if row is not None:
                    value = pickle.loads(row[0])
                    self.memory_store[hash_key] = value
                    self.logger.debug(f"Loaded {key} from DB")
                    return value
        except Exception as e:
            self.logger.error(f"Error reading from DB: {e}")
        
        return defval

    def close(self):
        """リソースをクリーンアップします"""
        pass  # 同期版では特に何もする必要がありません

# 使用例
def main():
    # インスタンス作成
    mem = PersistentMemory('my_custom_database.db')
    # メモリに保存
    mem.save("test_key", "test_value")
    # メモリから読み込み
    value = mem.load("test_key")
    print(f"Loaded value: {value}")

    count = mem.load("countup", 0)
    mem.save("countup", count + 1)
    print(f"Count: {mem.load('countup')}")
    # クリーンアップ
    mem.close()

    # インスタンス作成
    mem = PersistentMemory('my_custom_database2.db')
    # メモリに保存
    mem.save("test_key2", "test_value2")
    # メモリから読み込み
    value = mem.load("test_key")
    print(f"Loaded value: {value}")
    value = mem.load("test_key2")
    print(f"Loaded value: {value}")
    # クリーンアップ
    mem.close()

if __name__ == "__main__":
    main()