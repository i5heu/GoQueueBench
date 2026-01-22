go run cmd/bench/main.go -json -iter 1 && go run cmd/buildGraph/main.go && python3 analyze.py

#go run cmd/bench/main.go -json -high-concurrency -iter 3 && go run cmd/buildGraph/main.go && python3 analyze.py
