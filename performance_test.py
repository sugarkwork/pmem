#!/usr/bin/env python3
"""
PersistentMemory のパフォーマンステスト
同期メソッドの高速化効果を測定
"""

import time
import logging
from skpmem.async_pmem import PersistentMemory

def test_sync_performance():
    """同期メソッドのパフォーマンステスト"""
    print("=== 同期メソッドパフォーマンステスト ===")
    
    # ログレベルを WARNING に設定してデバッグメッセージを抑制
    logging.basicConfig(level=logging.WARNING)
    
    mem = PersistentMemory('performance_test.db')
    
    try:
        # 書き込みパフォーマンステスト
        print("\n1. 書き込みパフォーマンステスト")
        num_operations = 1000
        
        start_time = time.time()
        for i in range(num_operations):
            mem.save_sync(f"test_key_{i}", f"test_value_{i}")
        write_time = time.time() - start_time
        
        print(f"書き込み操作 {num_operations} 回: {write_time:.3f}秒")
        print(f"1回あたりの平均時間: {write_time/num_operations*1000:.3f}ms")
        
        # フラッシュして全ての書き込みを完了
        print("\n2. フラッシュ操作")
        flush_start = time.time()
        mem.flush_sync()
        flush_time = time.time() - flush_start
        print(f"フラッシュ時間: {flush_time:.3f}秒")
        
        # 読み込みパフォーマンステスト
        print("\n3. 読み込みパフォーマンステスト")
        
        # メモリキャッシュからの読み込み
        start_time = time.time()
        for i in range(num_operations):
            value = mem.load_sync(f"test_key_{i}")
        cache_read_time = time.time() - start_time
        
        print(f"キャッシュからの読み込み {num_operations} 回: {cache_read_time:.3f}秒")
        print(f"1回あたりの平均時間: {cache_read_time/num_operations*1000:.3f}ms")
        
        # 新しいインスタンスでDBからの読み込みテスト
        mem2 = PersistentMemory('performance_test.db')
        start_time = time.time()
        for i in range(100):  # 少ない回数でテスト
            value = mem2.load_sync(f"test_key_{i}")
        db_read_time = time.time() - start_time
        
        print(f"DBからの読み込み 100 回: {db_read_time:.3f}秒")
        print(f"1回あたりの平均時間: {db_read_time/100*1000:.3f}ms")
        
        # 削除パフォーマンステスト
        print("\n4. 削除パフォーマンステスト")
        start_time = time.time()
        for i in range(100):
            mem.delete_sync(f"test_key_{i}")
        delete_time = time.time() - start_time
        
        print(f"削除操作 100 回: {delete_time:.3f}秒")
        if delete_time > 0:
            print(f"1回あたりの平均時間: {delete_time/100*1000:.3f}ms")
        else:
            print("1回あたりの平均時間: < 0.001ms (非常に高速)")
        
        # 最終フラッシュ
        mem.flush_sync()
        mem2.close_sync()
        
        print("\n=== パフォーマンステスト完了 ===")
        print(f"総書き込み時間: {write_time:.3f}秒")
        print(f"キャッシュ読み込み速度: {num_operations/cache_read_time:.0f} ops/sec")
        print(f"DB読み込み速度: {100/db_read_time:.0f} ops/sec")
        if delete_time > 0:
            print(f"削除速度: {100/delete_time:.0f} ops/sec")
        else:
            print("削除速度: > 100,000 ops/sec (非常に高速)")
        
    finally:
        mem.close_sync()

def test_concurrent_operations():
    """並行操作のテスト"""
    print("\n=== 並行操作テスト ===")
    
    mem = PersistentMemory('concurrent_test.db')
    
    try:
        # 複数の書き込み操作を高速で実行
        start_time = time.time()
        for i in range(500):
            mem.save_sync(f"concurrent_key_{i}", {"data": i, "timestamp": time.time()})
            if i % 2 == 0:
                # 読み込み操作を混在
                value = mem.load_sync(f"concurrent_key_{i//2}", None)
        
        concurrent_time = time.time() - start_time
        print(f"並行操作 (書き込み500回 + 読み込み250回): {concurrent_time:.3f}秒")
        
        # フラッシュして完了を確認
        mem.flush_sync()
        print("全ての操作が正常に完了しました")
        
    finally:
        mem.close_sync()

if __name__ == "__main__":
    test_sync_performance()
    test_concurrent_operations()