# install

    pip install skpmem

## skpmem/async_pmem.py の特徴

- 非同期対応の永続メモリクラス `PersistentMemory` を提供
- SQLite データベースを用いてデータを永続化
- 非同期での保存・読み込みが可能
- 非同期コンテキストマネージャ対応（`async with` で利用可能）
- 自動的にテーブル作成・初期化を実施
- 内部で書き込みキューを持ち、効率的にデータベースへ反映

## 使用例

```python
import asyncio
from skpmem.async_pmem import PersistentMemory

async def main():
    # ログ出力の設定（任意）
    import logging
    logging.basicConfig(level=logging.INFO)

    # 永続メモリのインスタンス作成
    async with PersistentMemory('my_custom_database.db') as mem:
        # データの保存
        await mem.save("test_key", "test_value")
        # データの読み込み
        value = await mem.load("test_key")
        print(f"Loaded value1: {value}")

        # デフォルト値付きでカウンタを取得・更新
        counter = await mem.load("counter", 0)
        print(f"Counter: {counter}")
        counter += 1
        await mem.save("counter", counter)

asyncio.run(main())
```

## dictライクなアクセス

`PersistentMemory` は `dict` のようなアクセスも一部サポートしています。

- `mem["key"] = value` で値を保存（非同期でDBにも保存されます）
- `value = mem["key"]` でキャッシュから値を取得（キャッシュにない場合は `KeyError`。初回は `await mem.load("key")` でロードしてください）
- `del mem["key"]` でキャッシュとDBから削除（DB削除は非同期）

```python
async with PersistentMemory('my_custom_database.db') as mem:
    await mem.save("foo", 123)      # 通常の保存
    mem["bar"] = 456               # dict風の保存（非同期でDBにも保存）

    await mem.load("bar")          # DBからロードしキャッシュ
    print(mem["bar"])              # キャッシュから取得

    del mem["bar"]                 # キャッシュとDBから削除
```

- `save(key, value)` でデータを保存
- `load(key, defval=None)` でデータを取得（存在しない場合は defval を返す）
- 非同期で複数の保存・取得操作が可能
- データは SQLite データベースに永続化される
