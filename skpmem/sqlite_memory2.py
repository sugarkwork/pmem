import sqlite3
import hashlib
import pickle
import threading
from queue import Queue
from datetime import datetime

class PersistentMemory:
    def __init__(self, database_file, history=False):
        self.database_file = database_file
        self.memory_store = {}
        self.write_queue = Queue()
        self.initialized = False
        self.history = history
        self.initialize()

    def initialize(self):
        if self.initialized:
            return

        conn = sqlite3.connect(self.database_file)
        c = conn.cursor()
        c.execute('''
            CREATE TABLE IF NOT EXISTS memory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key_hash TEXT,
                date TIMESTAMP,
                value_hash TEXT,
                value BLOB
            )
        ''')
        #c.execute('CREATE INDEX IF NOT EXISTS idx_key_hash ON memory (key_hash)')
        #c.execute('CREATE INDEX IF NOT EXISTS idx_date ON memory (date)')
        conn.commit()
        conn.close()

        db_writer_thread = threading.Thread(target=self.async_db_writer, daemon=True)
        db_writer_thread.start()

        self.initialized = True

    def name_hash(self, name):
        return hashlib.sha256(str(name).encode()).hexdigest()

    def value_hash(self, value):
        return hashlib.sha256(pickle.dumps(value)).hexdigest()

    def __setitem__(self, key, val):
        if isinstance(key, tuple):
            if len(key) != 2:
                raise KeyError("For datetime access, use (key, datetime)")
            self.save_memory(key[0], val, date=key[1])
        else:
            self.save_memory(key, val)

    def __getitem__(self, key):
        if isinstance(key, tuple):
            if len(key) != 2:
                raise KeyError("For datetime access, use (key, datetime)")
            return self.load_memory(key[0], date=key[1])
        return self.load_memory(key)

    def save_memory(self, key, val, date: datetime = None):
        key_hash = self.name_hash(key)
        val_hash = self.value_hash(val)
        
        if isinstance(date, str):
            # parse datetime
            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f").isoformat()

        if isinstance(date, datetime):
            date = date.isoformat()

        if date is None:
            date = datetime.now().isoformat()
        
        conn = sqlite3.connect(self.database_file)
        c = conn.cursor()
        c.execute('SELECT value_hash FROM memory WHERE key_hash = ? ORDER BY date DESC LIMIT 1', (key_hash,))
        last_val_hash = c.fetchone()
        conn.close()

        if not last_val_hash or last_val_hash[0] != val_hash:
            self.memory_store[key_hash] = val
            self.write_queue.put((key, key_hash, val, val_hash, date, self.history))
        else:
            print(f"Value already saved for {key_hash}")

    def async_db_writer(self):
        conn = sqlite3.connect(self.database_file)
        c = conn.cursor()
        while True:
            key, key_hash, val, val_hash, date, history = self.write_queue.get()
            try:
                if not history:
                    c.execute('DElETE FROM memory WHERE key_hash = ?', (key_hash,))

                c.execute('INSERT INTO memory (key_hash, date, value_hash, value) VALUES (?, ?, ?, ?)',
                          (key_hash, date, val_hash, pickle.dumps(val)))
                conn.commit()
                print(f"@ Saved {key_hash} at {date}, {key} = {val}")
            except Exception as e:
                print(f"Error writing to DB: {e}")
            self.write_queue.task_done()
    
    def flush(self):
        import time
        while not self.write_queue.empty():
            time.sleep(0.1)

    def load_memory(self, key, defval=None, date=None):
        key_hash = self.name_hash(key)
        
        if date is None and key_hash in self.memory_store:
            return self.memory_store[key_hash]

        conn = sqlite3.connect(self.database_file)
        c = conn.cursor()
        try:
            if date:
                c.execute('SELECT value FROM memory WHERE key_hash = ? AND date <= ? ORDER BY date DESC LIMIT 1',
                          (key_hash, date.isoformat()))
            else:
                c.execute('SELECT value FROM memory WHERE key_hash = ? ORDER BY date DESC LIMIT 1', (key_hash,))
            
            row = c.fetchone()
            if row is not None:
                value = pickle.loads(row[0])
                if date is None:
                    self.memory_store[key_hash] = value
                return value
        except Exception as e:
            print(f"Error reading from DB: {e}")
        finally:
            conn.close()
        
        return defval
    
    def load_all(self, key, defval=None) -> list:
        self.flush()

        key_hash = self.name_hash(key)
        
        conn = sqlite3.connect(self.database_file)
        c = conn.cursor()
        try:
            c.execute('SELECT value FROM memory WHERE key_hash = ? ORDER BY date DESC', (key_hash,))
            
            rows = c.fetchall()
            if rows:
                values = [pickle.loads(row[0]) for row in rows]
                return values
        except Exception as e:
            print(f"Error reading from DB: {e}")
        finally:
            conn.close()
        
        return defval


def main():
    import random
    import sys

    mem = PersistentMemory('my_custom_database.db', history=True)
    # test
    print("Save test")
    mem['test_key'] = 'test_value_' + str(random.randint(0, sys.maxsize))
    print("Load test")
    print("    ", mem['test_key'])

    print("---")

    print("Save test2") 
    mem['test_key', datetime(2023, 1, 1)] = 'test_value2_' + str(random.randint(0, sys.maxsize))
    print("Load test2")
    print("    ", mem['test_key', datetime(2023, 1, 1)])

    print("---")
    print(mem.load_all('test_key'))


if __name__ == "__main__":
    main()