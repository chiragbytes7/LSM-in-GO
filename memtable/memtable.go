package memtable

import (
	"os"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/B1NARY-GR0UP/originium/pkg/logger"
	"github.com/B1NARY-GR0UP/originium/pkg/skiplist"
	"github.com/B1NARY-GR0UP/originium/types"
	"github.com/B1NARY-GR0UP/originium/utils"
	"github.com/B1NARY-GR0UP/originium/wal"
)

type memtable struct {
	mu       sync.RWMutex
	logger   logger.Logger
	skiplist *skiplist.SkipList
	wal      *wal.WAL
	dir      string
	readOnly bool
}

func newMemtable(dir string, maxLevel int, p float64) *memtable {
	l, err := wal.Create(dir)
	if err != nil {
		panic(err)
	}
	return &memtable{
		logger:   logger.GetLogger(),
		skiplist: skiplist.New(maxLevel, p),
		wal:      l,
		dir:      dir,
		readOnly: false,
	}
}

func (mt *memtable) recover() int64 {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	defer utils.Elapsed(time.Now(), mt.logger, "memtable recover")

	files, err := os.ReadDir(mt.dir)
	if err != nil {
		mt.logger.Panicf("read dir %v failed: %v", mt.dir, err)
	}

	var walFiles []string
	for _, file := range files {
		if !file.IsDir() && path.Ext(file.Name()) == ".log" && wal.CompareVersion(wal.ParseVersion(file.Name()), mt.wal.Version()) < 0 {
			walFiles = append(walFiles, path.Join(mt.dir, file.Name()))
		}
	}

	if len(walFiles) == 0 {
		return 0
	}

	slices.Sort(walFiles)

	var maxVersion int64

	mt.logger.Infof("found %d wal file, recovery start", len(walFiles))
	// merge wal files
	for _, file := range walFiles {
		l, err := wal.Open(file)
		if err != nil {
			mt.logger.Panicf("open wal %v failed: %v", file, err)
		}

		entries, err := l.Read()
		if err != nil {
			mt.logger.Panicf("read wal %v failed: %v", file, err)
		}

		for _, entry := range entries {
			// record max version
			maxVersion = max(maxVersion, entry.Version)

			mt.skiplist.Set(entry)
			if err = mt.wal.Write(entry); err != nil {
				mt.logger.Panicf("write wal failed: %v", err)
			}
		}

		if err = l.Delete(); err != nil {
			mt.logger.Panicf("delete wal %v failed: %v", file, err)
		}
	}
	mt.logger.Infof("recovery finished")

	return maxVersion
}

func (mt *memtable) set(entry types.Entry) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.readOnly {
		mt.logger.Panicf("write readonly memtable")
	}

	mt.skiplist.Set(entry)
	if err := mt.wal.Write(entry); err != nil {
		mt.logger.Panicf("write wal failed: %v", err)
	}
	mt.logger.Infof("memtable set [key: %v] [value: %v] [tombstone: %v] [version: %v]", entry.Key, string(entry.Value), entry.Tombstone, entry.Version)
}

func (mt *memtable) get(key types.Key) (types.Entry, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.Get(key)
}

// first entry greater or equal than key
func (mt *memtable) lowerBound(key types.Key) (types.Entry, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.LowerBound(key)
}

func (mt *memtable) scan(start, end types.Key) []types.Entry {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.Scan(start, end)
}

func (mt *memtable) all() []types.Entry {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.All()
}

func (mt *memtable) size() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.Size()
}

func (mt *memtable) freeze() {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if err := mt.wal.Close(); err != nil {
		mt.logger.Panicf("wal close failed: %v", err)
	}
	mt.readOnly = true
}

func (mt *memtable) reset() *memtable {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	l, err := mt.wal.Reset()
	if err != nil {
		mt.logger.Panicf("wal reset failed: %v", err)
	}
	return &memtable{
		logger:   logger.GetLogger(),
		skiplist: mt.skiplist.Reset(),
		wal:      l,
		dir:      mt.dir,
		readOnly: false,
	}
}
