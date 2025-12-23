/usr/bin/time -l go run cmd/mpt_bench/main.go -n 150000 -slots 1000 -m 150000 -k 100 -db temp_bench_db -clear |tee mpt_bench.log

