package main

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v3"
)

func main() {
	//  打开DB
	db, err := badger.Open(badger.DefaultOptions("                                             /tmp/badger"))
	defer db.Close()
	//v1 test
	// 读写事务
	err = db.Update(func(txn *badger.Txn) error {
		// Your code here…
		txn.Set([]byte("answer"), []byte("42"))
		txn.Get([]byte("answer"))
		return nil
	})
	// 只读事务
	err = db.View(func(txn *badger.Txn) error {
		// Your code here…
		txn.Get([]byte("answer_v1"))
		return nil
	})
	// 遍历keys
	err = db.View(func(txn *badger.Txn) error {
		// 创建一个默认的迭代器对象
		opts := badger.DefaultIteratorOptions
		// 配置预读kv条数为10
		opts.PrefetchSize = 10
		// 创建一个事务层迭代器对象
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			//9. 检查itme是否合法，如果合法就是说item不为空，则调用next方法再次执行上述步骤

			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				fmt.Printf("key=%s, value=%s\n", k, v)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	// vlog 的GC
	err = db.RunValueLogGC(0.7)
	_ = err
}
