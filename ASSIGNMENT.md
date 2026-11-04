# Blue Ocean Backend Assignment v2: Settlement Stream

**Track:** Backend / distributed systems / data
**Time box:** **6 hours, hard limit.** See [§10](#10-how-to-spend-the-6-hours).
**Version:** v2

---

## 0. Read this first

You will build the core of a payments settlement pipeline:
- consume a **multiplexed, sequenced session protocol** from an acquirer gateway;
- fan events out through a **message broker** to parallel workers, without losing per-merchant order;
- apply them **exactly once** to a **PostgreSQL ledger**;
- move money through a **partner bank API** that will happily pay twice if you let it.

You will also review a previous team's version of the same system, which caused a production incident.

**Use AI tools. We assume you will.** Our engineers use them every day.

This assignment is designed so that the tools only get you as far as the questions you know how to ask. Every component here has a well-known naive implementation that looks right, passes a happy-path test, and is wrong. The harness is built to find those implementations. So are our reviewers.

What we are testing:
- whether you can see the failure modes **before** they happen;
- whether you can tell your assistant precisely what to build;
- whether you can tell when what it built is wrong.

**How it is graded:**

| Part | What | Weight |
|---|---|---|
| **A** | Working system, run by us under fault injection (§5) | 40 |
| **B** | `DESIGN.md`: production-scale design, with numbers (§6) | 20 |
| **C** | `REVIEW.md`: defects in the incumbent code, with reproducers (§7) | 20 |
| — | Code quality and tests | 10 |
| — | Observability and operability | 10 |

**The bar:**
- **Minimum to pass:** a total ≥ 70, **and** Part A correctness ≥ 20/30, **and** Part C ≥ 12/20.
- **Double payments.** A single double payment in our standard run caps Part A correctness at 10/30.

**After you submit.** If you pass, a 45-minute technical debrief follows, and you will be asked to change your system live. Build only what you can explain.

> The harness numbers in this brief are the defaults. **We run your submission with a different seed, more data, harsher chaos, and the fault-injection schedule in §5.4.** Making `verify.py` pass on your laptop is necessary, not sufficient.

---

## 1. The scenario, in plain terms

Blue Ocean runs payment-processing and settlement systems for property-management and hospitality clients. Card payments flow through an **acquirer**, the company that processes card transactions for a merchant. The acquirer streams every money-moving event for every merchant to us. We keep each merchant's **ledger**, and when a merchant asks for its money we pay it out through a **settlement bank**.

You do not need a payments background. Here is the whole domain:

| Term | Meaning here |
|---|---|
| **Merchant** | A business we settle for, such as a hotel. One merchant is one stream of events. |
| **AUTH** (authorisation) | A card payment is approved and the amount is put **on hold**. The merchant cannot spend it yet. |
| **CAPTURE** | The held payment is taken. The hold is released, and the captured amount minus our fee becomes **settled** money. |
| **AUTH_EXPIRY** | An authorisation lapses without being captured. Its hold is released. |
| **REFUND** | The merchant gives money back to a customer from a capture. Settled money goes down. |
| **CHARGEBACK** | A customer disputes a capture and the bank claws it back, plus a fee. This is forced: it can push a merchant's balance **negative**. |
| **Reserve** | A per-merchant floor that must stay in the account. |
| **PAYOUT_REQUEST** | The merchant asks for `amount` to be sent to its bank account. We approve it only if the money is there. |
| **Platform fee** | Our revenue, taken on every capture and chargeback. |

That is all the finance. The engineering problem:
- an **ordered, lossy, duplicating stream** with **strict per-key ordering**;
- **exactly-once effects** in a database at high throughput;
- **exactly-once side effects** against an external API that cannot give you exactly-once itself.

---

## 2. Production scale (Part B is graded against these numbers)

| Dimension | Number |
|---|---|
| Merchants | **60,000**, Zipf-skewed. The busiest 1% of merchants generate ~40% of events. |
| Event volume | ~**450 M ledger events / month**. Steady **4,000 events/s**, peak **30,000 events/s** (sale days, for hours). |
| Ledger retention | **7 years online**, about **3 billion rows** at steady state. Regulators can ask for any merchant's balance at any instant within 7 years: it must be answered **exactly**, in **< 200 ms**. |
| Gateway | One live session per `client_id`, at most **25,000 frames/s per session**. You may hold up to **16 `client_id`s**; in production a `LOGON` may subscribe to a chosen subset of channels. The gateway keeps replay history for **72 hours**. |
| Settlement bank | **50 requests/s** per client, **2 in-flight payouts per destination account**, idempotency keys remembered for **120 s**, a single live OAuth token per client, maintenance windows of up to **20 minutes**. |
| Payouts | Usually ~40 k/day, but **month-end brings ~1.5 M payout requests within 6 hours**. |
| Availability | Losing an event the gateway has delivered and we have credited is **not acceptable**. Reader failover must complete within **30 s**. Paying the same payout twice is a **P1 incident**. |

---

## 3. What we give you

```
harness/                 - run it, read it, do NOT modify it (we run our own copy)
  gateway.py             BOSP/1 acquirer gateway (TCP 7100, status on 7101)
  PROTOCOL.md            the session protocol: read it twice
  bank.py                settlement bank simulator (HTTP 7200)
  BANK-API.md            the bank's contract and failure behaviour
  ledger_events.py       deterministic event model + reference ledger rules (used by the verifier)
  verify.py              checks your system against ground truth
  docker-compose.harness.yml, Dockerfile, README.md
incumbent/               - the previous team's code, for Part C
  README.md, schema.sql, session_reader.py, leader.py, ledger_consumer.py, payouts.py
```

**Harness facts:**
- Everything in `harness/` is plain Python 3.11+ standard library, with no dependencies.
- The default run is 200 merchants × 2,500 frames (500 k frames) at 1,000 frames/s with 4× bursts. That takes ~7 minutes to release.
- Our review run is ≥ **2 M frames at 3,000 frames/s**.
- Your pipeline must get events **only over BOSP**. Importing `ledger_events` to generate, look up or "fix" events in your pipeline is disqualifying. Using it in tests is fine.
- Your pipeline must never call the bank's `/_admin` endpoint.

---

## 4. The ledger rules (exact)

Events arrive as `DATA` frames. Every event has `event_id` (`"<merchant_id>:<seq>"`, globally unique), `merchant_id`, `type`, `event_time` (ISO-8601 UTC, strictly increasing within a channel) and `currency`. Amounts are **integers in minor units** (cents).

Per merchant you keep:
- `settled`
- `held`
- `reserve`
- `bank_account`
- `currency` (fixed by the merchant's first `MERCHANT_CONFIG`)

`available = settled − reserve`.

Events are applied **in channel sequence order**:

| `type` | Fields | Effect |
|---|---|---|
| `MERCHANT_CONFIG` | `reserve_minor`, `bank_account` | Set `reserve` and `bank_account`. The first one also fixes `currency`. |
| `AUTH` | `amount_minor` | `held += amount`. The authorisation is now **open**. |
| `AUTH_EXPIRY` | `ref` (an AUTH event_id) | If that auth is open: close it, `held -= its amount`. Otherwise **reject** (no effect). |
| `CAPTURE` | `ref` (AUTH), `amount_minor`, `fee_minor` | If the auth is open **and** `amount ≤ auth amount`: close the auth; `held -= auth amount` (the whole hold is released, even on a partial capture); `settled += amount − fee`; `platform[currency] += fee`. Otherwise **reject**. |
| `REFUND` | `ref` (CAPTURE), `amount_minor` | If the capture exists **and** `refunded_so_far + amount ≤ captured amount`: `settled -= amount`. Otherwise **reject**. Fees are not refunded. |
| `CHARGEBACK` | `ref` (CAPTURE), `amount_minor`, `fee_minor` | If the capture exists: `settled -= amount + fee`; `platform[currency] += fee`. It may go negative. Otherwise reject. |
| `PAYOUT_REQUEST` | `amount_minor` | If `amount > 0` **and** `available ≥ amount` at this point in the sequence: `settled -= amount` and the payout is **accepted**. Otherwise it is **rejected** (`insufficient_funds`). |
| anything else | — | **Ignore** it, for forward compatibility, but it still consumes its sequence number. |

Rejected events still consume their sequence number. `NOTICE` and `GAP_FILL` frames have no ledger effect.

**Payout execution.** An accepted payout must be executed at the bank **exactly once**, using:

| Bank field | Value |
|---|---|
| `reference` | the `event_id` |
| `account` | the merchant's `bank_account` at that point in the sequence |
| `currency` | the merchant's currency |
| `amount_minor` | the requested amount |

A payout to a closed account (`422 account_closed`) is **failed**. Its money stays deducted (operations handles it manually), and it must never be retried.

`harness/ledger_events.py → apply_event()` is the reference implementation of this table. Where the prose and the code disagree, the code wins.

---

## 5. Part A: build it

### 5.1 Required stack

- **Language:** Python 3.11+ for your services (any framework: asyncio, FastAPI, Django, …).
- **Database:** PostgreSQL 15+ is the system of record.
- **Redis:** required for cross-process coordination. **Your correctness must not depend on Redis being durable.** We will wipe it mid-run.
- **Broker:** one of Kafka / Redpanda, RabbitMQ (quorum queues or streams), NATS JetStream, or Redis Streams. Justify the choice.
- **Run command:** `docker compose up` must bring up everything, including the harness services from `harness/docker-compose.harness.yml`, with no manual steps. Document how to scale each service.

### 5.2 Required behaviour

**A1. Session ownership and the protocol.**
- **Replicas.** Run **≥ 2 session-reader replicas**. Exactly one may hold the gateway session at a time.
- **Failover.** If the leader dies (`kill -9`), a standby must hold the session within **15 s**.
- **A paused leader.** If the leader is **paused** (`SIGSTOP` for 45 s) and then resumes, nothing it does afterwards may corrupt the ledger or the stream. Explain what fences it.
- **The protocol.** Implement BOSP/1 correctly:
  - framing;
  - heartbeats and `TEST_REQUEST`;
  - gap detection and resend, **including lost resends**;
  - `GAP_FILL`;
  - duplicates, whatever their `poss_dup` flag;
  - resume from a checkpoint;
  - credit.
- **Credit and durability.** How your credit grants relate to durability downstream is part of the design: state it.

**A2. Parallel processing with ordering.**
- **Consumers.** Run **≥ 4 ledger-consumer processes** (processes, not threads) that apply events in parallel while preserving **per-merchant order**.
- **What it must survive:** a consumer `kill -9` mid-batch, a broker restart, and a rebalance or scale-out, all without double-applying, skipping or reordering anything.
- **Delivery semantics.** Write down the delivery semantics at every hop.

**A3. The ledger, in PostgreSQL.**
- **Rules and effects.** Apply the rules in §4 with **exactly-once effect**, in integer minor units.
- **Throughput.** Sustain **≥ 3,000 events/s** end to end on our review machine (8 cores, 32 GB) at 2 M events with chaos on, and **catch up within 60 s** of the gateway finishing.
- **Platform fees.** Every capture across every merchant touches them. Do not let that become one hot row everything serialises on, or, if you choose to, show us the numbers that make it acceptable.

**A4. Payouts.**
- **Executors.** Run **≥ 2 payout-executor processes**.
- **What they must achieve:**
  - every accepted payout executed **exactly once**;
  - closed-account payouts failed, never retried;
  - nothing lost, nothing paid twice.
- **What they must survive:** the bank's failure modes, scheduled maintenance windows longer than the idempotency-key lifetime, and executor crashes at any point.
- **Shared limits.** The single-token OAuth session, the global rate limit and the per-account in-flight limit are shared by all executors.

**A5. Read API.** The contract `verify.py` uses:

```
GET /v1/merchants/{merchant_id}/balance
  → 200 {"merchant_id", "currency", "settled_minor", "held_minor", "available_minor", "last_seq"}
     last_seq = the highest channel sequence number fully processed (NOTICE / GAP_FILL included)

GET /v1/merchants/{merchant_id}/balance?as_of=2026-11-03T14:05:09.123Z
  → 200 {"merchant_id", "settled_minor", "held_minor", "available_minor"}
     the balance after every event of that merchant with event_time ≤ as_of; all zeros before the first event

GET /v1/platform/balances            → 200 {"USD": 123, "EUR": 45, ...}

GET /v1/payouts/{payout_id}          → 200 {"payout_id", "status": "pending|paid|failed|rejected", "bank_payout_id", "reason"}
                                        404 if that PAYOUT_REQUEST has not been applied yet
```

`as_of` must have a p99 latency **< 50 ms** over HTTP at 2 M events, and Part B must explain how that holds at 3 B rows.

**A6. Observability.**
- **Questions an operator must be able to answer:**
  - which channels are behind and by how much;
  - who holds the session, and since when (epoch);
  - resend and gap rates;
  - broker lag per partition or queue;
  - payouts by state, including **ambiguous / unknown outcome**;
  - bank error classes.
- **Endpoints:** `/healthz` and `/readyz` with real checks.
- **Logs:** structured, carrying `event_id` / `payout_id` / `session_epoch`.

**A7. Tests.**
- **The risky logic, not getters:**
  - the per-channel sequencing state machine (unit, deterministic: gaps, lost resend, `GAP_FILL`, duplicates, reconnect);
  - exactly-once apply under concurrent redelivery, against **real PostgreSQL**;
  - the payout outcome matrix (every row of the table in `BANK-API.md`);
  - the cross-process limiter.
- **Fault injection.** Include a script that reproduces at least **two** of the faults in §5.4 against your running stack.

### 5.3 How we check Part A

We run `harness/verify.py check` after each scenario. It must print `RESULT: PASS`. It checks:
- every channel drained;
- every balance exact;
- point-in-time balances;
- platform fees;
- every payout's status;
- the **bank's own record of money moved**: exactly one execution per paid payout, none otherwise.

### 5.4 Our fault-injection schedule (published, so it is fair)

During a single 2 M-frame run, in an order and at times we choose, we will:

1. `kill -9` the session-reader leader.
2. `SIGSTOP` the (new) leader for 45 s, then `SIGCONT` it.
3. `kill -9` one ledger consumer in the middle of a batch; add two more consumers later.
4. Restart the broker.
5. Restart Redis **without persistence**, as if after a `FLUSHALL`.
6. Restart PostgreSQL (fast shutdown).
7. `kill -9` a payout executor while a payout is in flight, and again during a bank maintenance window.
8. Raise gateway chaos: 5× the drop rate, disconnects every 20–40 s.
9. Shrink the bank's windows: idempotency TTL 20 s, maintenance 30 s every 90 s, 10% after-commit failures.

---

## 6. Part B: `DESIGN.md`

Answer with **numbers and mechanisms**, against §2. Generic prose ("we'd add more workers", "Kafka scales horizontally") scores zero. Diagrams are welcome; walls of generated text are not. Keep it under ~8 pages.

1. **Delivery semantics, hop by hop.** For each hop:
   - gateway → reader
   - reader → broker
   - broker → consumer
   - consumer → PostgreSQL
   - PostgreSQL → payout executor → bank

   Say whether it is at-most-once, at-least-once or effectively-once, and **the exact mechanism**: key, constraint, offset or checkpoint, and **when** it is written relative to the effect.
2. **Fencing.** The leader stalls for 45 s while holding received-but-unpublished frames and an open producer. A standby takes over and resumes. The old leader wakes up.
   - Walk through, step by step, what each process does and what stops corruption, at which hop.
   - What is your fencing token, and who checks it?
   - Why is a lock with a TTL not sufficient on its own?
3. **Backpressure.** At a 30 k/s peak your broker slows to 10 k/s for 10 minutes.
   - Where does the backlog live, how big does it get, and what bounds memory in each process?
   - How does credit flow control participate?
4. **Partitioning and skew.**
   - Partition count, and the key, for 60 k Zipf-skewed merchants.
   - What happens when one merchant alone exceeds a partition's or worker's throughput?
   - How do you go beyond one 25 k frames/s session (16 `client_id`s)? How are channels assigned, and how is that reassigned on failure without two sessions owning a channel?
5. **PostgreSQL at 3 B rows:**
   - schema, partitioning strategy and keys;
   - the index that serves an exact `as_of` in < 200 ms at 3 B rows, and the write cost it adds;
   - update-heavy rows (running balances, platform fees): HOT updates, vacuum, bloat, contention at 30 k events/s;
   - estimated WAL volume per day;
   - `synchronous_commit` and fsync choices, and why;
   - your connection-pooling mode, and what it forbids you from using.
6. **Isolation levels.** State the isolation level of every transaction, and why. Give **one concrete anomaly** your design would suffer at READ COMMITTED if you removed one specific lock or constraint you rely on: name it, and show the interleaving.
7. **Payout maths and the ambiguity protocol.**
   - Month-end: 1.5 M payouts in 6 h against 50 req/s and 2-in-flight per account. What is the drain time, what is the backlog, and which merchants wait longest?
   - Your ambiguous-outcome protocol as a **state machine**.
   - Where, under your design, could a double payment still occur, and what would detect it?
8. **Failure modes.** For each, what happens, what self-heals, and what pages a human:
   - Redis lost;
   - PostgreSQL failover to an **async** replica (what is lost; how do you detect and repair the ledger and the session checkpoint?);
   - a broker partition-leader loss;
   - a gateway checkpoint older than the 72 h replay window;
   - a poison event.
9. **Three tradeoffs** you made: what you gave up, and what you would change with more time.

---

## 7. Part C: `REVIEW.md`, the incumbent

`incumbent/` is the previous team's version of this system. They used Kafka.

It ran in production for two weeks and was rolled back after an incident:
- **14 payouts were executed twice**;
- **3 merchants' balances drifted** from the acquirer's statements;
- **a further 9 accepted payouts were never sent**.

The post-mortem never found the root causes. `incumbent/README.md` has the context.

**Deliver `REVIEW.md`:**

1. **Every defect you find, ranked by severity.** For each:
   - `file:line`;
   - the **concrete trigger**: the interleaving, failure or input that makes it go wrong;
   - the **impact**: money, ordering, loss, availability or performance;
   - the fix.
2. **Root causes.** Which defects, in combination, explain each of the three incident symptoms? Be specific: name the defects and the sequence of events.
3. **Reproducers for your top 5.** An **executable reproducer** for each of your five most severe defects: a test or script that **fails against the incumbent logic and passes against your fix**.
   - You may extract the incumbent logic into testable units.
   - You may run against the PostgreSQL/Redis in your compose.
   - A description of a reproducer is not a reproducer.

We know of more than 20 defects. Some are the kind any linter or assistant flags in seconds; those are worth little. The ones that caused the incident are not.

---

## 8. Deliverables

```
your-submission/
├── README.md            # how to run (one command), how to scale each service, what's built vs deferred,
│                        #   known issues, and TIME SPENT PER AREA (honestly)
├── DESIGN.md            # Part B
├── REVIEW.md            # Part C
├── AI-USAGE.md          # which tools you used, for what, and three places where the AI was wrong
│                        #   and how you noticed
├── docker-compose.yml   # everything, including the harness services
├── services/ ...        # your code
├── tests/ ...           # including Part C reproducers
└── scripts/faults.*     # your fault-injection script (A7)
```

---

## 9. Submission

- **Format.** A Git repository link (preferred) or a zip without virtualenvs, `node_modules`, data dumps or database volumes. A commit history that shows your progression is valuable to us.
- **Runtime.** It must start with the documented command in **under 10 minutes** on a clean machine with Docker.
- **The debrief.** In the debrief you will be asked to make a change live, for example "payouts must now also respect a per-merchant daily limit", or "the gateway now sends `CHANNEL_RESET`". Expect it.

---

## 10. How to spend the 6 hours

This is more than 6 hours of work if you have to look things up. That is deliberate: we want to see what you prioritise. A suggested order:

| Time | Focus |
|---|---|
| 0:00–0:45 | Read `PROTOCOL.md`, `BANK-API.md`, §4 and the incumbent. Decide the architecture and delivery semantics **before** prompting anything. |
| 0:45–2:30 | Session reader (protocol state machine plus leadership), broker, ledger consumers. Get `verify.py` passing on balances with chaos on. |
| 2:30–3:45 | Payout executor: token, limits, the ambiguity protocol. Get the bank checks passing. |
| 3:45–4:45 | Part C `REVIEW.md` plus the top-5 reproducers. |
| 4:45–5:30 | Part B `DESIGN.md`. |
| 5:30–6:00 | Fault-injection run, README, `AI-USAGE.md`. |

**If you run out of time:**
- correctness beats throughput;
- the payout path beats observability;
- a written, specific account of what you did not build beats a half-built feature.

**Tell us what you deferred and why.**

---

## 11. Ground rules

- **AI tools.** Use any tools, libraries and AI assistants you like. You must be able to explain every line and every decision; unexplained code counts against you.
- **The harness.** Do not modify it. We run our own copy.
- **Assumptions.** Write them down. Where this brief is ambiguous, choose, state the choice, and move on.
- **Questions.** You may email us questions about the brief. We answer clarifications, not design questions.
