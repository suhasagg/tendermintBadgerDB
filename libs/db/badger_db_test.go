package db

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
	"github.com/tendermint/tendermint/libs/common"
)

func setupBadgerDB(t *testing.T) (d *BadgerDB, shutDown func()) {
	testDir, err := ioutil.TempDir("", "badgerdbTests")
	require.Nil(t, err, "expecting a nil error")
	ddb, err := NewBadgerDB("test_db", testDir)
	shutDown = func() {
		ddb.Close()
		os.RemoveAll(testDir)
	}
	return ddb, shutDown
}

func setupBadgerDBforBenchmark(b *testing.B) (d *BadgerDB, shutDown func()) {
	testDir, err := ioutil.TempDir("", "badgerdbbenchTests")
	require.Nil(b, err, "expecting a nil error")
	ddb, err := NewBadgerDB("testbench_db", testDir)
	shutDown = func() {
		ddb.Close()
		os.RemoveAll(testDir)
	}
	return ddb, shutDown
}

func TestBadgerDB(t *testing.T) {
	t.Parallel()
	_, shutDown := setupBadgerDB(t)
	defer shutDown()

}

func TestBadgerDBBackend(t *testing.T) {
	name := fmt.Sprintf("test_%x", common.RandStr(12))
	dir := os.TempDir()
	db := NewDB(name, BadgerDBBackend, dir)
	defer cleanupDBDir(dir, name)
	_, ok := db.(*BadgerDB)
	assert.True(t, ok)

}

func TestBadgerDBSetGet(t *testing.T) {
	t.Parallel()
	ddb, shutDown := setupBadgerDB(t)
	defer shutDown()

	tests := []struct {
		key, value []byte
	}{
		{key: []byte("foo"), value: []byte("bar")},
		{key: []byte(""), value: []byte("")},
		{key: []byte(""), value: []byte("")},
		{key: []byte(" "), value: []byte("a     2")},
		{key: common.RandBytes(1024), value: []byte("right here ")},
	}

	for _, kv := range tests {
		ddb.SetSync(kv.key, kv.value)
	}

	for i, kv := range tests {
		if got, want := ddb.Get(kv.key), kv.value; !bytes.Equal(got, want) {
			t.Errorf("#%d: Get mismatch\ngot: %x\nwant:%x", i, got, want)
		}
	}

	// Purge them all
	for _, kv := range tests {
		ddb.DeleteSync(kv.key)
	}

	for i, kv := range tests {
		if got := ddb.Get(kv.key); !bytes.Equal(got, nil) {
			t.Errorf("#%d: Get mismatch\ngot: %x\nwant:nil", i, got)
		}
	}
}

func TestBadgerDBBatchSet(t *testing.T) {
	t.Parallel()
	ddb, shutDown := setupBadgerDB(t)
	defer shutDown()

	tests := []struct {
		key, value []byte
	}{
		{key: []byte("foo"), value: []byte("bar")},
		{key: common.RandBytes(1024), value: []byte("right here ")},
		{key: []byte("p"), value: []byte("     ")},
		{key: common.RandBytes(511), value: nil},
	}

	batch := ddb.NewBatch()

	// Test concurrent batch sets and
	// ensure there are no concurrent race conditions and operation is thread safe
	var wg sync.WaitGroup
	for _, kv := range tests {
		wg.Add(1)
		go func(key, value []byte) {
			defer wg.Done()
			batch.Set(key, value)
		}(kv.key, kv.value)
	}
	wg.Wait()

	// Verify that no keys have been committed yet.
	for i, kv := range tests {
		if got := ddb.Get(kv.key); !bytes.Equal(got, nil) {
			t.Errorf("#%d: Get mismatch\ngot: %x\nwant:nil", i, got)
		}
	}

	batch.Write()

	// Now ensure that wrote the data
	for i, kv := range tests {
		if got, want := ddb.Get(kv.key), kv.value; !bytes.Equal(got, want) {
			t.Errorf("#%d: Get mismatch\ngot: %x\nwant:%x", i, got, want)
		}
	}
}

var benchmarkData = []struct {
	key, value []byte
}{
	{common.RandBytes(1000), []byte("foo")},
	{[]byte("foo"), common.RandBytes(1000)},
	{common.RandBytes(10000), common.RandBytes(1000)},
}

func BenchmarkBadgerDBSet(b *testing.B) {
	ddb, shutDown := setupBadgerDBforBenchmark(b)
	defer shutDown()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, kv := range benchmarkData {
			ddb.Set(kv.key, kv.value)
		}
	}
	b.ReportAllocs()
}

func BenchmarkBadgerDBSetSync(b *testing.B) {
	ddb, shutDown := setupBadgerDBforBenchmark(b)
	defer shutDown()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, kv := range benchmarkData {
			ddb.SetSync(kv.key, kv.value)
		}
	}
	b.ReportAllocs()
}

//Batch set should be faster as compared to normal set operation
func BenchmarkBadgerDBBatchSet(b *testing.B) {
	ddb, shutDown := setupBadgerDBforBenchmark(b)
	defer shutDown()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := ddb.NewBatch()
		for _, kv := range benchmarkData {
			batch.Set(kv.key, kv.value)
		}
		batch.Write()
	}
	b.ReportAllocs()
}

func BenchmarkBadgerDBSetDelete(b *testing.B) {
	ddb, shutDown := setupBadgerDBforBenchmark(b)
	defer shutDown()

	for i := 0; i < b.N; i++ {
		for _, kv := range benchmarkData {
			ddb.Set(kv.key, kv.value)
		}
		for _, kv := range benchmarkData {
			ddb.Delete(kv.key)
		}
	}
	b.ReportAllocs()
}
