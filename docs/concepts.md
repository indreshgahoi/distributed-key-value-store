 ## Distributed System

Distributed systems are quite difficult to understand under the hood. We will try to build a distributed key-value store from basic understanding.
So we will break down the system into layers, along with each layer's invariants and the guarantees it provides.

Distributed database systems can be broken down into the following layers:
 1. SQL/API Interface
 2. Distributed Transaction Engine
 3. Concurrency Control & Replication
 4. MVCC (Multi-Version Concurrency Control) & Lease Holder
 5. Local Storage Engine

Now let's try to understand why we need this.

Imagine we have a simple key-value store on a single machine.
 1. Ram sets balance to ₹100. SET balance = ₹100
 2. Shyam gets balance. GET balance, Shyam sees balance = ₹100

On the local machine, the SET balance command writes to a file, balance = ₹100, to make it durable. When Shyam runs GET balance, you read the value from the file and return balance = ₹100 to Shyam.

### What Could Go Wrong While Interacting With a Local Machine (Failure Mode)
 1. If the machine dies or the disk crashes after setting the balance, Shyam won't be able to see the balance. (**Data loss**)
 2. If millions of set/get operations happen on the single machine, the CPU and disk choke. (**Hard limit on CPU/Disk/Memory**)

To prevent data loss and serve high traffic, we need multiple machines. Let's call each one a Node.

### What Challenges Are Posed by Multiple Nodes
Suppose we have three computers: Node 1, Node 2, Node 3, connected via a network.
#### Replication
First, Ram sets balance to ₹100 on Node 1.
Second, Shyam gets balance from Node 2. Node 2 says, "I don't know the balance."

To address this, we need **replication** across machines: the balance should be available on all the nodes. From here onwards, we will replace Ram and Shyam with "client."

#### Consensus
Now client 1 writes to Node 1, set balance = ₹100, and at the same time client 2 writes to Node 2, set balance = ₹150. Now clients 3 and 4 may see two different values depending on timing and network delay.

How do multiple nodes agree on the **exact same sequence of events**? Does Node 1's write happen first, or does Node 2's write happen first? In order to replicate the data on all the machines, they need to agree on the order in which these events happened, so that each node ends up with the same value.

### The Solution to the Above Problem: Raft Protocol (Majority Rule)
1. Node 1, Node 2, and Node 3 hold an election, and Node 1 wins. Node 1 is the Leader. Now all writes will go to the Leader.
2. Client 1 issues a command, set balance = ₹100. Node 1 writes an entry in the append-only log, Entry #1: "Set balance = ₹100" as an instruction; it does not overwrite the balance immediately.
3. Replication: Node 1 sends a copy of Entry #1 to Node 2 and Node 3.
4. Majority Quorum ($Q = \lfloor N/2 \rfloor + 1$):
    - Node 2 receives and saves the entry, then replies to Node 1 that it's okay.
    - Node 3 is dead, no response.
    - Node 1 counts its own write plus Node 2's write, so 2 successful writes out of 3 total. That's a majority among 3 nodes.
    - Quorum is reached. Node 1 marks Entry #1 as Committed, sets balance = ₹100, then replies success to the client.

Even if Node 2 wakes up after one hour, it simply copies the entries from Node 1 — the data is safe. Let's call Node 1, Node 2, Node 3 **a Raft Group, responsible for replicating and reaching consensus on a set of keys**.

## Data Is Too Big for 3 Nodes (Partitions and Ranges)
Let's say the database grows to 50 terabytes. One node can't hold that much data. We must devise a strategy to spread the data across nodes.

### The Solution Is Splitting the Data
A simple strategy we can consider is dividing the keys into sorted ranges.
1. Range 1: Keys from [A-G)
2. Range 2: Keys from [G-P)
3. Range 3: Keys from [P-Z)

Now introduce physical servers X, Y, Z. X, Y, Z can replicate only Range 1 and Range 2; similarly, other servers are responsible for the rest of the data.
So Range 1 is replicated to Servers X, Y, Z, called **Raft Group 1**.
Similarly, Range 2 is replicated to Servers X, Y, Z, called **Raft Group 2**.

Now Anurag updates his profile; Raft Group 1 can handle it, and any other clients see the updated profile.
Similarly, Gopal updates his profile; Raft Group 2 can handle it, and other clients see the updated profile.

This is how the system can scale horizontally, by adding more servers and splitting ranges once they grow beyond a threshold (~64 MB).

## Reading and Writing at the Same Time
Let's consider the scenario where Ram has ₹100.

1. A background audit wants to sum up all the balances in the database (takes 10 seconds).
2. While the audit is going on, Ram wants to deposit ₹50.

In a traditional database, the audit takes a lock on Ram's row. Ram can't deposit the money until the audit finishes.
At scale, millions of transactions halt because of one long-running transaction.

### The Solution: Multi-Version Concurrency Control (MVCC)
Rule: Never update data in place. Always write a new version with a timestamp.

Instead of storing:
Ram balance = ₹150.
The disk instead stores an ordered timeline of values:
- Ram balance = ₹100 @ 10:00:00
- Ram balance = ₹150 @ 10:05:00

So when the audit starts at 10:01:00, it asks, "Give me all the values that existed at exactly 10:01:00."
- When it looks at Ram, it finds the latest version <= 10:01:00, which is ₹100.
- Even if Ram writes ₹150 at 10:05:00, the audit is completely unaffected.

Result: Reads never block writes, and writes never block reads.

## The Physical Problem of Time (Clock Skew)
For MVCC to work, every write operation needs an accurate timestamp.
On a single node we use time.now(), but across three data centers in New York, London, and India, clocks are never in sync.
- Motherboard quartz crystals vibrate at slightly different speeds depending on temperature and hardware age.
- Server A might think the time is 10:00:00.000
- Server B might think the time is 09:59:59.985, 15ms behind Server A

### Why This Breaks the Database
1. Anurag transfers money in London via Server A. Server A thinks the time is 10:00:00.000.
2. Ram checks his balance in India via Server B. Server B thinks the time is 09:59:59.985.
3. For Server B, Anurag's **transfer happened in the future**. Ram sees stale or missing data — it violates causality.

### The Solution: Two Engineering Approaches to Solve This
1. Google Spanner's way (TrueTime)
2. CockroachDB's way (Hybrid Logical Clock)

We will discuss each one.
#### Google Spanner's Way (TrueTime)
It is hardware-based.
"I don't know the exact time, but I guarantee it is between [12:00:00.001 and 12:00:00.007]."
Error window ε (epsilon) = 3ms.

#### CockroachDB (Hybrid Logical Clock, HLC)
Software-based.
Combines the normal computer clock with a logical counter (12:00:00, counter=1).
Whenever nodes exchange messages, they bump their clocks forward to the highest time seen.

### The TrueTime "Commit Wait" Trick
How does Google guarantee that if Event 2 happens after Event 1, Event 2 gets a higher timestamp?
- Server A wants to commit a write. TrueTime reports the current time with an uncertainty window ($\epsilon = 2\text{ ms}$).
- Server A picks the highest possible time: 10:00:05.
- Server A intentionally sleeps for $2\epsilon$ (4 ms) before telling the client "Success."
- By the time Server A responds, the real physical time across the entire universe is guaranteed to be past 10:00:05. Anyone who reads after this will see a timestamp higher than 10:00:05.

### Transactions Across Multiple Ranges (Two-Phase Commit / 2PC)
What happens when Anurag (in Range [A-G)) wants to send $50 to Priya (in Range [P-Z))?

- Anurag is managed by Raft Group 1.
- Priya is managed by Raft Group 3.

You cannot use a single Raft agreement because they are two independent committees on different machines. If Raft Group 1 debits Anurag, but Raft Group 3 crashes before crediting Priya, $50 disappears into thin air.

```text
                [Transaction Coordinator]
                      /             \
             Phase 1: "Prepare"      Phase 1: "Prepare"
                    /                 \
                   v                   v
     [Raft Group 1: Anurag]       [Raft Group 3: Priya]
     - Lock Anurag's $50          - Check Priya's account
     - Replicate via Raft        - Replicate via Raft
     - Reply: "Prepared"         - Reply: "Prepared"
                   \                   /
                    \                 /
            Phase 2: Coordinator writes "COMMITTED"
                      /             \
             Phase 2: "Commit"       Phase 2: "Commit"
                    /                 \
                   v                   v
     Apply debit permanently     Apply credit permanently
```

#### The Traditional 2PC Flaw vs. Distributed Databases
- Traditional 2PC Flaw: If the Coordinator is a single server and crashes after telling Group 1 to prepare, the whole database hangs waiting forever.
- How Spanner/CockroachDB solves it: The Coordinator is itself a replicated Raft group. The coordinator's status log (PREPARING, COMMITTED) is replicated across multiple machines. If the physical machine running the coordinator dies, another machine in its Raft group takes over immediately and finishes the transaction.

### When Cables Get Cut (Partitions & Split-Brain)
Imagine 5 database nodes: 3 in Oregon and 2 in Virginia. A fiber-optic line is severed across the country. Oregon and Virginia can no longer talk to each other.
```text
       [Oregon: 3 Nodes]      <--- X (Severed Network) X --->      [Virginia: 2 Nodes]
```
- What does Virginia do?
    - Virginia has 2 nodes. Total cluster size is 5.
    - To commit any change, Raft requires a majority: $\lfloor 5/2 \rfloor + 1 = 3\text{ votes}$.
    - Virginia can only get 2 votes (itself). It cannot reach a majority.
    - Virginia halts all writes. It refuses to accept updates.
- What does Oregon do?
    - Oregon has 3 nodes. 3 out of 5 is a majority!
    - Oregon continues accepting reads and writes seamlessly.

Why do we force Virginia to stop?
If Virginia also accepted writes, users in New York would write to Virginia, while users in California would write to Oregon. When the network cable is repaired, you would have two completely different, conflicting versions of reality.
This is called **Split-Brain**.
In distributed database architecture, we **sacrifice temporary write availability in the minority region** to protect data correctness.

## Summary: How It All Fits Together in One Request
When a user executes:
**TRANSFER $50 FROM Anurag TO Priya:**

1. **Routing:** The request identifies that Anurag is in Range [A-G) and Priya is in Range [P-Z).
2. **Coordination:** A Coordinator starts a Two-Phase Commit (2PC).
3. **Write Intents & Consensus**:
    - A provisional write is sent to Anurag's range and Priya's range.
    - Inside each range, the write is agreed upon by a Raft majority.
4. **Time Assignment:** A timestamp is assigned using TrueTime or HLC so that future reads see this transfer in the correct chronological order.
5. **Commit:** Once both Raft groups confirm they are prepared, the coordinator marks the transaction COMMITTED.
6. **MVCC Storage:** The changes are appended to the local storage engine as new timestamped versions. Old versions remain untouched so historical reads are never blocked.

```text
+------------------------------------------------------------------+
| SQL / Relational / KV API Layer                                 |
+------------------------------------------------------------------+
| Distributed Transaction Engine (2PC Coordinator & Participant)   |
+------------------------------------------------------------------+
| Concurrency Control & Replication (Multi-Raft / Multi-Paxos)      |
+------------------------------------------------------------------+
| MVCC & Range Lease Management (Leaseholders)                     |
+------------------------------------------------------------------+
| Local Storage Engine (LSM-Tree / RocksDB / Pebble / Custom WAL)  |
+------------------------------------------------------------------+
```
