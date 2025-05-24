#!/usr/bin/env python3
"""
PersistentMemory の辞書インターフェース（__getitem__, __setitem__, __delitem__）のテスト
"""

import time
import logging
from skpmem.async_pmem import PersistentMemory

def test_dict_interface():
    """辞書インターフェースのテスト"""
    print("=== 辞書インターフェーステスト ===")
    
    # ログレベルを INFO に設定
    logging.basicConfig(level=logging.INFO)
    
    mem = PersistentMemory('dict_interface_test.db')
    
    try:
        print("\n1. __setitem__ テスト（mem[key] = value）")
        
        # 辞書のように値を設定
        mem["user_name"] = "Alice"
        mem["user_age"] = 25
        mem["user_data"] = {"id": 123, "email": "alice@example.com"}
        mem["numbers"] = [1, 2, 3, 4, 5]
        
        print("✓ 複数の値を辞書形式で設定完了")
        
        print("\n2. __getitem__ テスト（value = mem[key]）")
        
        # 辞書のように値を取得
        name = mem["user_name"]
        age = mem["user_age"]
        data = mem["user_data"]
        numbers = mem["numbers"]
        
        print(f"✓ user_name: {name}")
        print(f"✓ user_age: {age}")
        print(f"✓ user_data: {data}")
        print(f"✓ numbers: {numbers}")
        
        print("\n3. 値の更新テスト")
        
        # 既存の値を更新
        mem["user_age"] = 26
        mem["user_data"]["status"] = "active"
        
        updated_age = mem["user_age"]
        updated_data = mem["user_data"]
        
        print(f"✓ 更新後の user_age: {updated_age}")
        print(f"✓ 更新後の user_data: {updated_data}")
        
        print("\n4. __delitem__ テスト（del mem[key]）")
        
        # 削除前の確認
        print(f"削除前の user_name: {mem['user_name']}")
        
        # 辞書のように削除
        del mem["user_name"]
        
        # 削除後の確認（存在しないキーにアクセス）
        try:
            deleted_value = mem["user_name"]
            print(f"⚠️ 削除されたはずの値が取得できました: {deleted_value}")
        except KeyError:
            print("✓ 削除されたキーにアクセスするとKeyErrorが発生（期待通り）")
        
        # load_sync でデフォルト値を使って確認
        deleted_value = mem.load_sync("user_name", "NOT_FOUND")
        print(f"✓ load_sync での確認: {deleted_value}")
        
        print("\n5. 大量データのテスト")
        
        # 大量のデータを辞書形式で設定
        start_time = time.time()
        for i in range(100):
            mem[f"bulk_data_{i}"] = {"index": i, "value": f"data_{i}"}
        
        bulk_time = time.time() - start_time
        print(f"✓ 100個のデータ設定: {bulk_time:.3f}秒")
        
        # 大量のデータを辞書形式で取得
        start_time = time.time()
        retrieved_data = []
        for i in range(100):
            data = mem[f"bulk_data_{i}"]
            retrieved_data.append(data)
        
        retrieve_time = time.time() - start_time
        print(f"✓ 100個のデータ取得: {retrieve_time:.3f}秒")
        print(f"✓ 最初のデータ: {retrieved_data[0]}")
        print(f"✓ 最後のデータ: {retrieved_data[-1]}")
        
        print("\n6. 永続性テスト")
        
        # フラッシュして全ての書き込みを完了
        mem.flush_sync()
        print("✓ フラッシュ完了")
        
    finally:
        mem.close_sync()
        print("✓ クリーンアップ完了")

def test_persistence():
    """永続性のテスト（新しいインスタンスでデータが読み込めるか）"""
    print("\n=== 永続性テスト ===")
    
    # 最初のインスタンスでデータを保存
    mem1 = PersistentMemory('persistence_test.db')
    try:
        mem1["persistent_key"] = "persistent_value"
        mem1["persistent_number"] = 42
        mem1.flush_sync()
        print("✓ 最初のインスタンスでデータを保存")
    finally:
        mem1.close_sync()
    
    # 新しいインスタンスでデータを読み込み
    mem2 = PersistentMemory('persistence_test.db')
    try:
        value = mem2["persistent_key"]
        number = mem2["persistent_number"]
        
        print(f"✓ 新しいインスタンスで読み込み成功:")
        print(f"  persistent_key: {value}")
        print(f"  persistent_number: {number}")
        
        if value == "persistent_value" and number == 42:
            print("✓ 永続性テスト成功！")
        else:
            print("⚠️ 永続性テスト失敗")
            
    finally:
        mem2.close_sync()

def test_error_handling():
    """エラーハンドリングのテスト"""
    print("\n=== エラーハンドリングテスト ===")
    
    mem = PersistentMemory('error_test.db')
    
    try:
        # 存在しないキーへのアクセス
        print("1. 存在しないキーへのアクセステスト")
        try:
            value = mem["nonexistent_key"]
            print(f"⚠️ 存在しないキーから値が取得されました: {value}")
        except KeyError:
            print("✓ 存在しないキーでKeyErrorが発生（期待通り）")
        
        # 存在しないキーの削除
        print("\n2. 存在しないキーの削除テスト")
        try:
            del mem["nonexistent_key"]
            print("✓ 存在しないキーの削除は正常に処理されました")
        except KeyError:
            print("✓ 存在しないキーの削除でKeyErrorが発生")
        except Exception as e:
            print(f"⚠️ 予期しないエラー: {e}")
            
    finally:
        mem.close_sync()

if __name__ == "__main__":
    test_dict_interface()
    test_persistence()
    test_error_handling()
    print("\n=== 全テスト完了 ===")