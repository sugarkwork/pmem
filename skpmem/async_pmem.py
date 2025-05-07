import aiosqlite
import hashlib
import pickle
import asyncio
from typing import Any, Optional
import logging


class PersistentMemory:
    def __init__(self, database_file: str = 'persistent_memory.db'):
        self.logger = logging.getLogger('skpmem')
        self.database_file = database_file
        self.memory_store: dict = {}
        self.write_queue: asyncio.Queue = asyncio.Queue()
        self.initialized: bool = False
        self._init_task: Optional[asyncio.Task] = None

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
        await asyncio.sleep(0.1)

    def _name_hash(self, name: str) -> str:
        return hashlib.sha512(name.encode()).hexdigest()

    async def save(self, key: str, val: Any):
        if not self.initialized:
            await self.initialize()
        
        self.logger.debug(f"Saving {key} to memory")

        hash_key = self._name_hash(key)
        self.memory_store[hash_key] = val
        await self.write_queue.put((hash_key, val))

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

    async def close(self):
        """残りのキュー項目を処理し、リソースをクリーンアップします"""
        if self._init_task:
            await self.write_queue.join()
            self._init_task.cancel()
    
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
        

if __name__ == "__main__":
    asyncio.run(main())