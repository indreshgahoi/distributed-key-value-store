```text
+-----------------------------------------------------------------------+
| Layer 2: MVCC Protocol & Snapshot Engine                              |
| (Point Get, Versioned Put, Tombstone Delete, Snapshot Scan, GC)       |
+-----------------------------------------------------------------------+
                                   ▲
                                   │ calls
+-----------------------------------------------------------------------+
| Layer 1: Binary Encoding & Serialization (Memcmpable Layer)           |
| (Key Escaping, Inverted Timestamp Big-Endian Packing, Value Headers)  |
+-----------------------------------------------------------------------+
                                   ▲
                                   │ calls
+-----------------------------------------------------------------------+
| Layer 0: Raw Ordered Byte Storage Engine                              |
| (SkipList, In-Memory B-Tree, or LSM-Tree / RocksDB / Pebble)          |
+-----------------------------------------------------------------------+
```

### Layer 0: Raw Ordered Byte Storage Engine  
1. What Layer 0 Does 
This layer has zero concept of transactions, timestamps, or SQL. It is a pure ordered key-value dictionary that stores arbitrary byte slices ([]byte -> []byte). It guarantees that all stored keys are kept in strict lexicographical order (bytes.Compare).

In memory, this is typically implemented using a Concurrent SkipList or B-Tree. On disk, it is an LSM-Tree (like Pebble or RocksDB).







