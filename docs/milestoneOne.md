# Milestone 1: Storage Layer Design Notes

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

## Layer 0: Raw Ordered Byte Storage Engine

#### What Layer 0 Does

This layer has zero concept of transactions, timestamps, or SQL. It is a pure ordered key-value dictionary that stores arbitrary byte slices (`[]byte -> []byte`). It guarantees that all stored keys are kept in strict lexicographical order (`bytes.Compare`).

In memory, this is typically implemented using a Concurrent SkipList or B-Tree. On disk, it is an LSM-Tree (like Pebble or RocksDB).

## Layer 1: Memcmpable Binary Codec

#### 1. Why Do We Need Layer 1? (The Core Intuition)

Layer 0 stores raw bytes in strict dictionary (`bytes.Compare`) order.
However, an MVCC engine doesn't store plain user keys; it stores **versioned keys**:
- **User Key** (e.g. `"balance_Ram"`)
- **Timestamp** (uint64 integer, e.g. `100`, `200`)

If we concatenate them naively:
$$\text{Physical Key} = \text{UserKey} + \text{Timestamp}$$

Two severe flaws break the database engine:

##### Flaw A: Delimiter Collision
Suppose you use a separator byte like `0x00`:
- User A writes key: `"user"` at TS `100` $\implies$ `"user" + 0x00 + 100`
- User B writes key: `"user\x00"` at TS `0` $\implies$ `"user" + 0x00 + 0`

Because the user key itself can contain arbitrary binary data (including `0x00`), the engine cannot deterministically distinguish where the user key ends and where internal metadata begins.

##### Flaw B: Timestamp Sort Inversion
Standard integers sort ascending: `100 < 200`.
In binary big-endian representation, timestamp `100` sorts **before** timestamp `200`.

In an MVCC database, **reads want to find the latest version first**. If you store timestamps normally, a seek for `"Ram"` lands on the oldest version from years ago, forcing an $O(N)$ sequential walk through all intermediate versions to find current state.

---

#### 2. The Solutions: Memcmpable Escaping & Bit-Inversion

##### A. Inverted Timestamp Packing (Bitwise NOT: `^ts`)
Instead of storing the raw timestamp, we invert every bit:
$$\text{Stored TS} = \sim\text{Original TS} \quad (\text{in Go: } \text{\textasciicircum ts})$$

Let us compare two timestamps:
- $T_1 = 100 \implies \text{\textasciicircum}100 = 18,446,744,073,709,551,515$
- $T_2 = 200 \implies \text{\textasciicircum}200 = 18,446,744,073,709,551,415$

Because $\text{\textasciicircum}200 < \text{\textasciicircum}100$, when serialized as Big-Endian bytes:
1. $T=200$ (the newest version) **physically appears first** in Layer 0.
2. $T=100$ (the older version) **appears second**.

A seek for the newest version lands directly on the latest write in $O(1)$ iterator steps.

##### B. Zero-Collision Key Escaping Protocol
To allow arbitrary binary user keys without delimiter collisions, we use **byte stuffing**:
- Escape byte: `0x00`
- If user key contains `0x00`: escape it as `0x00 0xFF`
- End of User Key Sentinel: `0x00 0x01`

Because `0x01 < 0xFF`, any key prefix ending with terminator `0x00 0x01` will sort **strictly before** any key that legitimately has an escaped byte `0x00 0xFF`:

```text
Original Key "user":       "user" + [0x00, 0x01] + [^TS]
Original Key "user\x00a":  "user" + [0x00, 0xFF] + "a" + [0x00, 0x01] + [^TS]
```

`bytes.Compare` gives the exact same result on the encoded bytes as it would on the raw user keys, with no delimiters breaking boundaries.

#### 3. Physical Layout Diagrams

Encoded Physical Key:
```text
+-------------------------+------------------+-----------------------------+
|    Escaped User Key     | Terminator (2B)  | Bit-Inverted Timestamp (8B) |
| (0x00 -> [0x00, 0xFF])  |   [0x00, 0x01]   |  BigEndian(^uint64(ts))     |
+-------------------------+------------------+-----------------------------+
```

Encoded Physical Value:
```text
+---------------+-----------------------------------------------+
|  OpType (1B)  |           Raw User Value Bytes                |
| 1=Put, 2=Del  |   (Zero-copy subslice returned to reader)     |
+---------------+-----------------------------------------------+
```

#### 4. Layer 1 Guarantees Provided to Layer 2

1. **Memcmpable Ordering Guarantee**
   $$\forall k_1, k_2: k_1 < k_2 \iff \text{EncodeKey}(k_1, t) < \text{EncodeKey}(k_2, t)$$
2. **Reverse Temporal Ordering Guarantee**
   $$\forall t_1 < t_2: \text{EncodeKey}(k, t_2) < \text{EncodeKey}(k, t_1)$$
3. **Zero-Allocation Hot Path**
   All encoding and decoding APIs accept pre-allocated destination buffers (`dst []byte`), allowing callers in Layer 2 to eliminate heap allocations via stack buffers or `sync.Pool`.
