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
	"github.com/holiman/uint256"
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
	fmt.Printf("Initializing LevelDB at %s...\n", *dbPath)
	ldb, err := leveldb.New(*dbPath, 256, 1024, "eth/db/chaindata/", false)
	if err != nil {
		fmt.Printf("Failed to open LevelDB: %v\n", err)
		return
	}
	diskdb := rawdb.NewDatabase(ldb)
	defer diskdb.Close()

	// 2. Initialize TrieDB and StateDB
	trieDB := triedb.NewDatabase(diskdb, triedb.HashDefaults)
	sdb := state.NewDatabase(trieDB, nil)
	statedb, _ := state.New(common.Hash{}, sdb)

	// 3. Phase 1: Creation
	fmt.Printf("Phase 1: Creating %d accounts with %d slots each (k=%d)...\n", *nAccounts, *nSlots, *kCommit)
	start := time.Now()

	addrs := make([]common.Address, *nAccounts)
	batchSize := *kCommit
	var currentRoot common.Hash

	for i := 0; i < *nAccounts; i++ {
		addr := common.BytesToAddress(crypto.Keccak256([]byte(fmt.Sprintf("account-%d", i)))[:20])
		addrs[i] = addr

		statedb.SetBalance(addr, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
		statedb.SetNonce(addr, uint64(i), tracing.NonceChangeUnspecified)

		for j := 0; j < *nSlots; j++ {
			slotKey := common.BytesToHash(crypto.Keccak256([]byte(fmt.Sprintf("slot-%d", j))))
			slotVal := common.BytesToHash(crypto.Keccak256([]byte(fmt.Sprintf("value-%d", j))))
			statedb.SetState(addr, slotKey, slotVal)
		}

		if (i+1)%10 == 0 || i+1 == *nAccounts {
			fmt.Printf("...processed %d/%d accounts (%.1f%%)\r", i+1, *nAccounts, float64(i+1)/float64(*nAccounts)*100)
		}

		// Periodic commit to keep memory usage low
		if (i+1)%batchSize == 0 || i+1 == *nAccounts {
			fmt.Printf("\n[Batch %d] Committing to disk...\n", (i/batchSize)+1)
			root, err := statedb.Commit(uint64(i/batchSize), false, false)
			if err != nil {
				fmt.Printf("Failed to commit StateDB: %v\n", err)
				return
			}
			err = trieDB.Commit(root, false)
			if err != nil {
				fmt.Printf("Failed to commit TrieDB: %v\n", err)
				return
			}
			currentRoot = root
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
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	perm := r.Perm(*nAccounts)
	for i := 0; i < *mModify; i++ {
		addr := addrs[perm[i]]

		// Modify some slots randomly
		for j := 0; j < 500; j++ { // modify 500 random slots per account
			slotIdx := r.Intn(*nSlots)
			slotKey := common.BytesToHash(crypto.Keccak256([]byte(fmt.Sprintf("slot-%d", slotIdx))))
			newVal := common.BytesToHash(crypto.Keccak256([]byte(fmt.Sprintf("new-value-%d-%d", i, j))))
			statedb.SetState(addr, slotKey, newVal)
		}

		if (i+1)%10 == 0 || i+1 == *mModify {
			fmt.Printf("...modified %d/%d accounts (%.1f%%)\r", i+1, *mModify, float64(i+1)/float64(*mModify)*100)
		}

		// Modification periodic commit
		if (i+1)%batchSize == 0 || i+1 == *mModify {
			fmt.Printf("\n[Mod Batch] Committing to disk...\n")
			root, err := statedb.Commit(uint64(i/batchSize)+1000000, false, false) // different block space
			if err != nil {
				fmt.Printf("Failed to commit modifications: %v\n", err)
				return
			}
			err = trieDB.Commit(root, false)
			if err != nil {
				fmt.Printf("Failed to commit TrieDB (mod): %v\n", err)
				return
			}
			currentRoot = root
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

