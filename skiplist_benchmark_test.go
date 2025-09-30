package rindb

import "testing"

func BenchmarkSkipListPut(b *testing.B) {
	cfg := testConfig(b)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		b.Fatalf("init skiplist: %v", err)
	}

	const ringSize = 1024
	keys := make([]int, ringSize)
	for i := range keys {
		keys[i] = i
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keys[i%ringSize]
		list.Put(key, i)
	}
}

func BenchmarkSkipListRemove(b *testing.B) {
	cfg := testConfig(b)
	list, err := InitSkipList[int, int](cfg)
	if err != nil {
		b.Fatalf("init skiplist: %v", err)
	}

	const ringSize = 1024
	for i := 0; i < ringSize; i++ {
		list.Put(i, i)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := i % ringSize
		if err := list.Remove(key); err != nil {
			b.Fatalf("remove %d: %v", key, err)
		}
		list.Put(key, i)
	}
}
