package db

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/dgraph-io/badger"
	cmn "github.com/tendermint/tendermint/libs/common"
)

func init() {
	dbCreator := func(name string, dir string) (DB, error) {
		return NewBadgerDB(name, dir)
	}
	registerDBCreator(BadgerDBBackend, dbCreator, false)
}

var _ DB = (*BadgerDB)(nil)

type BadgerDB struct {
	mtx sync.Mutex
	db  *badger.DB
}

type Options badger.Options

func badgerDBCreator(dbName, dir string) (DB, error) {
	return NewBadgerDB(dbName, dir)
}

var (
	_KB = int64(1024)
	_MB = 1024 * _KB
	_GB = 1024 * _MB
)

// NewBadgerDB creates a Badger key-value store backed to the
// directory dir supplied. If dir does not exist, it is created
func NewBadgerDB(dbName, dir string) (*BadgerDB, error) {

	dbPath := filepath.Join(dir, dbName+".db")
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, err
	}
	opts := badger.DefaultOptions
	// Support for Large KeyValue stores.
	opts.ValueLogFileSize = 1000 * _MB
	//Pros and Cons - 1)sync writes with higher write latency and ensure persistence, or 2)async writes with lower write latency and give up immediate persistence
	//Currently 2 is chosen
	//Things to consider -  Even with sync writes, writes to SSDs can be pretty fast for set SyncWrites=true
	opts.SyncWrites = false
	//Provide a way to truncate a corrupted Badger DB
	//it's a normal scenario to have a (relatively small) portion of the DB corrupted after a crash.
	opts.Truncate = true
	opts.Dir = dbPath
	opts.ValueDir = dbPath

	return NewBadgerDBWithOptions(opts, dbPath)
}

// NewBadgerDBWithOptions creates a BadgerDB key value store
// gives the flexibility of initializing a database with the
// respective options.
func NewBadgerDBWithOptions(opts badger.Options, dir string) (*BadgerDB, error) {
	db, err := badger.Open(opts)
	if err != nil {
		if strings.Contains(err.Error(), "lock") {
			log.Println("database locked, probably due to improper shutdown")
			if db, err := retry(opts); err == nil {
				log.Println("database unlocked, value log truncated")
				return db, nil
			}
			log.Println("could not unlock database:", err)

		}
		return nil, err
	}
	return &BadgerDB{db: db}, nil
}

//Retry Logic - open a database,call retry if it fails from presence of a lock file
func retry(originalOpts badger.Options) (*BadgerDB, error) {
	lockPath := filepath.Join(originalOpts.Dir, "LOCK")
	if err := os.Remove(lockPath); err != nil {
		return nil, fmt.Errorf(`removing "lock": %s`, err)
	}
	retryOpts := originalOpts
	retryOpts.Truncate = true
	db, err := badger.Open(retryOpts)
	if err != nil {
		return nil, err
	}

	return &BadgerDB{db: db}, nil
}

func (b *BadgerDB) DB() *badger.DB {
	return b.db
}

//Badger DB operations implementation

//read-only transaction
func (b *BadgerDB) Get(key []byte) []byte {
	var val []byte
	key = nonNilBytes(key)
	if len(key) > 0 {
		err := b.db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err == badger.ErrKeyNotFound {
				return nil
			}
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			val, err = item.ValueCopy(nil)

			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			cmn.PanicCrisis(err)
		}

	}
	return val
}

//read-only transaction
func (b *BadgerDB) Has(key []byte) bool {
	var found bool
	key = nonNilBytes(key)
	if len(key) > 0 {
		err := b.db.View(func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			if err != nil && err != badger.ErrKeyNotFound {
				return err
			}
			found = (err != badger.ErrKeyNotFound)
			return nil
		})
		if err != nil {
			cmn.PanicCrisis(err)
		}

	}
	return found
}

//start a read-write transaction
func (b *BadgerDB) Set(key, value []byte) {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	if len(key) > 0 && len(value) > 0 {
		err := b.db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		if err != nil {
			cmn.PanicCrisis(err)
		}
	}
}

func (b *BadgerDB) SetSync(key, value []byte) {
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	if len(key) > 0 && len(value) > 0 {
		err := b.db.Update(func(txn *badger.Txn) error {
			return txn.Set(key, value)
		})
		if err != nil {
			cmn.PanicCrisis(err)
		}
	}
}

func (b *BadgerDB) Delete(key []byte) {
	key = nonNilBytes(key)
	if len(key) > 0 {
		err := b.db.Update(func(txn *badger.Txn) error {
			return txn.Delete(key)
		})
		if err != nil {
			cmn.PanicCrisis(err)
		}
	}
}

func (b *BadgerDB) DeleteSync(key []byte) {
	key = nonNilBytes(key)
	if len(key) > 0 {
		err := b.db.Update(func(txn *badger.Txn) error {
			return txn.Delete(key)
		})
		if err != nil {
			cmn.PanicCrisis(err)
		}
	}
}

func (b *BadgerDB) Close() {
	if err := b.db.Close(); err != nil {
		cmn.PanicCrisis(err)
	}
}

func (b *BadgerDB) Fprint(w io.Writer) {
}

func (b *BadgerDB) Print() {
	bw := bufio.NewWriter(os.Stdout)
	b.Fprint(bw)
}

func (b *BadgerDB) Iterator(start, end []byte) Iterator {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	var itr *badger.Iterator

	err := b.db.View(func(txn *badger.Txn) error {
		itr = txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   10,
		})

		defer itr.Close()
		return nil
	})

	if err != nil {
		cmn.PanicCrisis(err)
	}

	itr.Rewind()
	return newBadgerDBIterator(*itr, start, end, false)

}

func (b *BadgerDB) ReverseIterator(start, end []byte) Iterator {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	var itr *badger.Iterator

	err := b.db.View(func(txn *badger.Txn) error {
		itr = txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   10,
			//Reverse = true set in iterator options so that rewind works accordingly
			Reverse: true,
		})

		defer itr.Close()
		return nil
	})

	if err != nil {
		cmn.PanicCrisis(err)
	}
	// Ensure that iterator is at the zeroth item
	itr.Rewind()
	return newBadgerDBIterator(*itr, start, end, true)
}

func (b *BadgerDB) IteratorPrefix(prefix []byte) Iterator {
	return b.Iterator(prefix, nil)
}

func (b *BadgerDB) Stats() map[string]string {
	return nil
}

func (b *BadgerDB) NewBatch() Batch {
	return &badgerDBBatch{db: b}
}

var _ Batch = (*badgerDBBatch)(nil)

type badgerDBBatch struct {
	entriesMu sync.Mutex
	entries   []*badger.Entry

	db *BadgerDB
}

func (bb *badgerDBBatch) Set(key, value []byte) {
	bb.entriesMu.Lock()
	key = nonNilBytes(key)
	value = nonNilBytes(value)
	if len(key) > 0 && len(value) > 0 {
		bb.entries = append(bb.entries, &badger.Entry{
			Key:   key,
			Value: value,
		})
	}
	bb.entriesMu.Unlock()
}

func (bb *badgerDBBatch) Delete(key []byte) {
	bb.db.Delete(key)
}

// Write commits all batch sets to the DB
func (bb *badgerDBBatch) Write() {
	bb.entriesMu.Lock()
	entries := bb.entries
	bb.entries = nil
	bb.entriesMu.Unlock()
	if len(entries) == 0 {
		return
	}
	err := bb.db.db.Update(func(txn *badger.Txn) error {
		for _, e := range entries {
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func (bb *badgerDBBatch) WriteSync() {
	bb.entriesMu.Lock()
	entries := bb.entries
	bb.entries = nil
	bb.entriesMu.Unlock()
	if len(entries) == 0 {
		return
	}

	err := bb.db.db.Update(func(txn *badger.Txn) error {
		for _, e := range entries {
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

type badgerDBIterator struct {
	mu        sync.RWMutex
	source    badger.Iterator
	start     []byte
	end       []byte
	isInvalid bool
	isReverse bool
}

var _ Iterator = (*badgerDBIterator)(nil)

func newBadgerDBIterator(source badger.Iterator, start, end []byte, isReverse bool) *badgerDBIterator {
	if isReverse {
		if end == nil {
			source.Rewind()
		} else {
			source.Seek(end)
		}
	} else {
		if start == nil {
			source.Rewind()
		} else {
			source.Seek(start)
		}

	}
	return &badgerDBIterator{
		source:    source,
		start:     start,
		end:       end,
		isReverse: isReverse,
		isInvalid: false,
	}
}

func (itr *badgerDBIterator) Domain() ([]byte, []byte) {
	return itr.start, itr.end
}

func (itr *badgerDBIterator) Valid() bool {
	return itr.source.Valid()
}

func (itr *badgerDBIterator) Key() []byte {
	return itr.source.Item().Key()
}

func (itr *badgerDBIterator) Value() []byte {
	var value []byte
	value,err := itr.source.Item().ValueCopy(value)
	if err != nil {
		cmn.PanicCrisis(err)
	}
	return value
}

func (itr *badgerDBIterator) kv() (key, value []byte) {
	var valueobtained []byte
	bItem := itr.source.Item()
	if bItem == nil {
		return nil, nil
	}
	valueobtained,err := itr.source.Item().ValueCopy(valueobtained)
	if err != nil {
		cmn.PanicCrisis(err)
	}
	return bItem.Key(), valueobtained
}

func (itr *badgerDBIterator) Next() {
	itr.source.Next()

}

func (itr *badgerDBIterator) rewind() {
	itr.source.Rewind()
}

func (itr *badgerDBIterator) Close() {
	itr.source.Close()
}
