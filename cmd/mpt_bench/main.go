package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/leveldb"
	"github.com/ethereum/go-ethereum/triedb"
	"github.com/ethereum/go-ethereum/triedb/pathdb"
	"github.com/holiman/uint256"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func main() {
	var (
		nAccounts = flag.Int("n", 100, "Number of accounts to create")
		nSlots    = flag.Int("slots", 1000, "Number of slots per account")
		mModify   = flag.Int("m", 10, "Number of accounts to modify after creation")
		kCommit   = flag.Int("k", 50, "Number of accounts per commit/flush")
		dbPath    = flag.String("db", "mpt_bench_db", "Path to LevelDB")
		clearDB   = flag.Bool("clear", true, "Clear database before starting")
	)
	flag.Parse()

	if *clearDB {
		fmt.Printf("Cleaning up old database at %s...\n", *dbPath)
		os.RemoveAll(*dbPath)
	}

	// 1. Initialize LevelDB
	fmt.Printf("Initializing LevelDB at %s (Compression: Off)...\n", *dbPath)
	ldb, err := leveldb.NewCustom(*dbPath, "eth/db/chaindata/", func(options *opt.Options) {
		options.Compression = opt.NoCompression
		options.OpenFilesCacheCapacity = 1024
		options.BlockCacheCapacity = 256 * opt.MiB
		options.WriteBuffer = 64 * opt.MiB
	})
	if err != nil {
		fmt.Printf("Failed to open LevelDB: %v\n", err)
		return
	}
	diskdb := rawdb.NewDatabase(ldb)
	defer diskdb.Close()

	// 2. Initialize TrieDB (PathDB for Pruning) and StateDB
	fmt.Println("Initializing TrieDB with PathDB (Pruning: On)...")
	trieDB := triedb.NewDatabase(diskdb, &triedb.Config{
		PathDB: pathdb.Defaults,
	})
	sdb := state.NewDatabase(trieDB, nil)
	statedb, _ := state.New(common.Hash{}, sdb)

	// 3. Phase 1: Creation
	fmt.Printf("Phase 1: Creating %d accounts with variable slots (avg %d, k=%d)...\n", *nAccounts, *nSlots, *kCommit)
	start := time.Now()

	addrs := make([]common.Address, *nAccounts)
	batchSize := *kCommit
	var currentRoot common.Hash

	// Use a fixed seed for deterministic benchmarking (borrowed from C# version)
	r := rand.New(rand.NewSource(42))

	for i := 0; i < *nAccounts; i++ {
		addr := common.BytesToAddress(crypto.Keccak256([]byte(fmt.Sprintf("account-%d", i)))[:20])
		addrs[i] = addr

		statedb.SetBalance(addr, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
		statedb.SetNonce(addr, uint64(i), tracing.NonceChangeUnspecified)

		// Borrowed from C#: Variable slots to simulate real world distribution (avg nSlots)
		vSlots := r.Intn(*nSlots * 2)
		for j := 0; j < vSlots; j++ {
			// Include account index i to ensure slots are unique across different accounts
			slotKey := common.BytesToHash(crypto.Keccak256([]byte(fmt.Sprintf("account-%d-slot-%d", i, j))))

			// Borrowed from C#: 30% probability for zero or small values to test RLP compression
			var slotVal common.Hash
			dice := r.Intn(100)
			if dice < 20 {
				// Keep zero
			} else if dice < 30 {
				slotVal[31] = 1 // Small value
			} else {
				r.Read(slotVal[:]) // Random 32 bytes
			}
			statedb.SetState(addr, slotKey, slotVal)
		}

		if (i+1)%10 == 0 || i+1 == *nAccounts {
			fmt.Printf("...processed %d/%d accounts (%.1f%%)\r", i+1, *nAccounts, float64(i+1)/float64(*nAccounts)*100)
		}

		// Periodic commit to keep memory usage low
		if (i+1)%batchSize == 0 || i+1 == *nAccounts {
			root, err := statedb.Commit(uint64(i/batchSize), false, false)
			if err != nil {
				fmt.Printf("\nFailed to commit StateDB: %v\n", err)
				return
			}
			err = trieDB.Commit(root, false)
			if err != nil {
				fmt.Printf("\nFailed to commit TrieDB: %v\n", err)
				return
			}
			currentRoot = root

			// Borrowed from C#: Memory monitoring
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			fmt.Printf("\n[Batch %d] Root: %.8s | Disk: %.2f MB | MemAlloc: %.2f MB\n",
				(i/batchSize)+1, currentRoot.String(), float64(getDirSize(*dbPath))/1024/1024, float64(mem.Alloc)/1024/1024)

			// Re-create statedb from the new root to release memory of dirty objects
			statedb, _ = state.New(currentRoot, sdb)
			runtime.GC() // Suggest GC to clean up
		}
	}
	fmt.Println()
	fmt.Printf("Creation finished in %v. Final Root: %x\n", time.Since(start), currentRoot)

	// 4. Phase 2: Modification
	if *mModify > *nAccounts {
		*mModify = *nAccounts
	}
	fmt.Printf("Phase 2: Randomly modifying slots in %d accounts (k=%d)...\n", *mModify, *kCommit)
	start = time.Now()

	// statedb is already updated to currentRoot from phase 1
	rMod := rand.New(rand.NewSource(time.Now().UnixNano()))
	perm := rMod.Perm(*nAccounts)
	for i := 0; i < *mModify; i++ {
		accountIdx := perm[i]
		addr := addrs[accountIdx]

		// Modify some slots randomly
		for j := 0; j < 500; j++ { // modify 500 random slots per account
			slotIdx := rMod.Intn(*nSlots)
			// Use the same unique key pattern as in Phase 1
			slotKey := common.BytesToHash(crypto.Keccak256([]byte(fmt.Sprintf("account-%d-slot-%d", accountIdx, slotIdx))))
			var newVal common.Hash
			rMod.Read(newVal[:])
			statedb.SetState(addr, slotKey, newVal)
		}

		if (i+1)%10 == 0 || i+1 == *mModify {
			fmt.Printf("...modified %d/%d accounts (%.1f%%)\r", i+1, *mModify, float64(i+1)/float64(*mModify)*100)
		}

		// Modification periodic commit
		if (i+1)%batchSize == 0 || i+1 == *mModify {
			root, err := statedb.Commit(uint64(i/batchSize)+1000000, false, false) // different block space
			if err != nil {
				fmt.Printf("\nFailed to commit modifications: %v\n", err)
				return
			}
			err = trieDB.Commit(root, false)
			if err != nil {
				fmt.Printf("\nFailed to commit TrieDB (mod): %v\n", err)
				return
			}
			currentRoot = root

			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			fmt.Printf("\n[Mod Batch] Disk: %.2f MB | MemAlloc: %.2f MB\n",
				float64(getDirSize(*dbPath))/1024/1024, float64(mem.Alloc)/1024/1024)

			statedb, _ = state.New(currentRoot, sdb)
			runtime.GC()
		}
	}
	fmt.Println()
	fmt.Printf("Modification finished in %v. Final New Root: %x\n", time.Since(start), currentRoot)

	// 5. Final Report
	size := getDirSize(*dbPath)
	fmt.Printf("\n--- Final Report ---\n")
	fmt.Printf("Database Path: %s\n", *dbPath)
	fmt.Printf("Disk Usage:    %.2f MB\n", float64(size)/(1024*1024))
}

func getDirSize(path string) int64 {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0
	}
	return size
}

