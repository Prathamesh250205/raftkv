package kv

import "hash/fnv"

// ShardFor maps key to one of n shards. Clients and servers must agree on it.
func ShardFor(key string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}
