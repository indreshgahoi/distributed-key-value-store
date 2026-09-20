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

## Layer 2: MVCC Protocol & Snapshot Engine

At Layer 2, we enforce a strict rule: data is never modified or erased in place. Every write or delete creates an immutable, timestamped record.

- A read at timestamp $T_{\text{read}}$ sees the database frozen in time: it returns the newest version whose timestamp $T_{\text{version}} \le T_{\text{read}}$.
- Versions written with $T_{\text{version}} > T_{\text{read}}$ are completely invisible to that reader.

How the components fit together:
```text
+-------------------------------------------------------------------------+
|                              Layer 2: MVCC                              |
|   Get(key, readTS)    Put(key, val, commitTS)    Delete(key, commitTS)   |
|   Scan(start, end, readTS)                       GC(safeWatermarkTS)    |
+-------------------------------------------------------------------------+
       │                             │                             │
       │ Encodes physical key        │ Checks OpType               │ Traverses
       ▼                             ▼                             ▼
+---------------------+       +---------------------+       +-------------+
|   Layer 1: Codec    |       |   OpTypePut (1)     |       | Layer 0:    |
|   EncodeKeyAppend   |       |   OpTypeDelete (2)  |       | ByteEngine  |
|   DecodeKey         |       |   (Tombstone)       |       | (SkipList)  |
+---------------------+       +---------------------+       +-------------+
```

#### Step-by-Step Point Read Execution: `Get(key, readTS)`

Suppose our storage engine holds three versions of key `"Ram"`:
- `"Ram"` @ TS 300 $\to$ Value: ₹200
- `"Ram"` @ TS 200 $\to$ Value: Tombstone (`OpTypeDelete`)
- `"Ram"` @ TS 100 $\to$ Value: ₹100

Because Layer 1 bit-inverts timestamps (`^ts`), Layer 0 physically orders them:

```text
[Ram, ^300] < [Ram, ^200] < [Ram, ^100]
```

If a client runs `Get("Ram", readTS = 150)`:

1. Layer 2 constructs the seek key: `seekKey = EncodeKeyAppend(nil, "Ram", 150)` (which contains `^150`).
2. Because `300 > 150`, `^300 < ^150` — so `[Ram, ^300]` sorts before our seek target in the skip list.
3. Because `200 > 150`, `^200 < ^150` — so `[Ram, ^200]` also sorts before our seek target.
4. Calling `iter.Seek(seekKey)` lands directly on `[Ram, ^100]` in $O(\log N)$ time.
5. We inspect the record:
   - Is the user key still `"Ram"`? Yes.
   - Is the timestamp $\le 150$? Yes ($100 \le 150$).
   - Is it a tombstone? No (`OpTypePut`).
6. Return ₹100. Versions 200 and 300 were skipped automatically.

### Watermark-Based Compaction & Garbage Collection (GC)

#### 1. Why Do We Need Compaction? (The Storage Leak Problem)

Because MVCC guarantees that writes never overwrite or delete data in place, physical memory and disk usage only ever grow.

Without a cleanup mechanism:
1. **Unbounded Space Growth:** Updating an account balance 1,000 times writes 1,000 separate version records to storage.
2. **Degraded Read Performance:** Range scans have to physically step over thousands of dead historical versions and tombstone markers just to find the current active values.

However, we **cannot simply delete older versions as soon as a new version arrives**. A background analytical audit, a backup job, or a distributed transaction might currently be running an isolated read at an older snapshot timestamp.

Compaction must be mathematically safe: **reclaim space without violating snapshot isolation for any running transaction**.

---

#### 2. The Core Compaction Invariant: The "Safe Watermark"

Let $T_{\text{watermark}}$ be the **Safe Watermark Timestamp** — the oldest snapshot timestamp among all active, open read transactions across the cluster.

$$\text{Active Snapshot Window} = [T_{\text{watermark}}, +\infty)$$

Any read that ever executes in the system now or in the future will read at some timestamp $T_{\text{read}} \ge T_{\text{watermark}}$.

```text
Historical Timeline for User Key "account:Ram":
====================================================================================>
Version:       V1(T=50)        V2(T=100)      |      V3(T=200)        V4(T=300)
Payload:       Put ₹50         Put ₹100       |      Put ₹200         Delete (Tombstone)
                                              |
                                     Safe Watermark = 150
```

For every user key, whose versions sort newest-to-oldest, compaction evaluates versions against three strict rules:

##### Rule 1: Versions Above Watermark ($T > T_{\text{watermark}}$) $\to$ RETAIN
- **Action:** Keep all versions completely untouched.
- **Why:** An active transaction may currently be reading at snapshot $T_{\text{read}} = 250$. It must be able to see $V_3$ (TS 200) while remaining isolated from $V_4$ (TS 300).

##### Rule 2: First Version Encountered Below Watermark ($T \le T_{\text{watermark}}$) $\to$ RETAIN AS BASELINE
- **Action:** Retain this single version.
- **Why:** Any transaction reading at or after $T_{\text{watermark}}$ that has not found a newer version above the watermark will fall back to this record. It is the definitive "baseline" state of the key at the watermark horizon.
- **Tombstone case:** If this baseline version is an `OpTypeDelete` (tombstone) and no transactions exist behind it, the key is logically dead for all time. The tombstone itself is scheduled for eviction, same as Rule 3.

##### Rule 3: Subsequent Versions Below Watermark ($T < T_{\text{baseline}}$) $\to$ PURGE
- **Action:** Overwrite the physical entry so it is treated as gone.
- **Why:** Because $V_2$ ($T=100$) satisfies all queries at or above $T_{\text{watermark}} = 150$, older version $V_1$ ($T=50$) is permanently shadowed. No active or future transaction can ever legally observe it.

#### 3. Execution Mechanics: How Compaction Traverses Storage

Compaction runs as an offline or background worker that coordinates across all three layers:

```text
[ Layer 2: Compactor ]
        │
        ├── 1. Seek to start of range: Seek([startKey, ^MaxUint64])
        ├── 2. Iterate using Layer 0 Iterator
        │
        ├── 3. For each physical key:
        │         Decodes (userKey, versionTS) via Layer 1 Codec
        │
        ├── 4. Tracks:
        │         - Current userKey boundary
        │         - Encountered baseline <= T_watermark (Boolean flag)
        │
        └── 5. Purges shadowed versions:
                  raw.Put(physicalKey, EncodeValueAppend(nil, OpTypeDelete, nil))
```

Purging overwrites the shadowed entry with a valid, self-describing tombstone rather than calling `raw.Delete` directly. Layer 0 has no true physical removal — its `Delete` just writes a 0-byte value — so a later `Get`/`Scan` landing on that exact physical key would fail to decode it. Writing an explicit `OpTypeDelete` keeps the slot decodable and correctly invisible to readers, at the cost of not actually reclaiming Layer 0's arena space (a known Layer 0 limitation, independent of this compaction pass).

#### 4. Worked Example: Compaction Decisions for `account:Ram`

```text
+---------------------------------------------------------------------------------+
| Physical Entry             | Decision       | Reason                            |
+---------------------------------------------------------------------------------+
| [Ram, ^300, OpDelete]      | KEEP           | TS 300 > Watermark (150)          |
| [Ram, ^200, OpPut(₹200)]   | KEEP           | TS 200 > Watermark (150)          |
| [Ram, ^100, OpPut(₹100)]   | KEEP (Baseline)| First version <= Watermark (150)  |
| [Ram, ^50,  OpPut(₹50)]    | PURGE          | Shadowed by TS 100 below watermark|
+---------------------------------------------------------------------------------+
```
