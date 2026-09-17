

## Table of Contents
- [Table of Contents](#table-of-contents)
- [ML Model](#ml-model)
  - [Rolling Window Features](#rolling-window-features)
  - [Transaction Features](#transaction-features)
  - [Identity / Device Features](#identity--device-features)
  - [User Reference Features (broadcast state)](#user-reference-features-broadcast-state)
  - [Merchant Reference Features (broadcast state)](#merchant-reference-features-broadcast-state)
  - [ONNX Runtime](#onnx-runtime)


## ML Model
We didn't focus on building a good model because the goal of this project is to demonstrate engineering guarantees and focus only on data engineering scope, so the model is simple (XGBoost baseline, no tuning).

### Rolling Window Features

**What they are:** Rolling features calculate a value over a fixed time window before the current transaction (for example, the last 1 hour or last 24 hours). They only use data from the past — never from the future.

**Why we need them:** A single transaction alone does not tell much about fraud. What matters is the card's recent behavior — for example, how many transactions this card made in the last hour, or how much money it spent in the last 24 hours. A sudden burst of transactions or an unusually large amount compared to normal behavior is a strong fraud signal.

**Why "rolling" specifically:** To capture this behavior correctly, we must only use transactions that happened *before* the current one — never transactions from the future. In production, the model scores a transaction the moment it happens, so it can only see the past. A rolling time window (last 1 hour, last 24 hours) gives us exactly that: card behavior calculated only from past data, matching what the model will actually have at real decision time.

**Rolling features used (per `card1`):**
- `card1_txn_count_1h`, `card1_txn_count_24h` — how many transactions this card made in the last hour / 24 hours (velocity signal)
- `card1_amt_sum_1h`, `card1_amt_sum_24h` — total amount spent in the window
- `card1_amt_mean_24h` — average amount spent in the last 24 hours
- `card1_amt_std_24h` — how much the amount changes (volatility signal)
- `card1_time_since_last` — seconds since this card's last transaction
- `amt_to_mean_24h_ratio` — current amount divided by the rolling mean (shows if this transaction is unusually large for this card)

### Transaction Features

Come directly from the transaction event, available the moment it happens:
- `TransactionAmt`, `ProductCD`, `C1-C14`, `D1-D15`, `V1-V339`, `M1-M9`, `dist1`, `dist2`

### Identity / Device Features

Also event-level, not stored as reference data, because device information can change every transaction (new device, new browser session):
- `id_01`, `id_02`, `DeviceType`, `DeviceInfo`

### User Reference Features (broadcast state)

Slow-changing data about the card/user, looked up (not computed per event):
- `card2, card3, card4, card5, card6` — card type and issuer info
- `addr1, addr2` — billing address
- `P_emaildomain`, `R_emaildomain` — account email domains

This data changes rarely (daily or less), so it fits well as broadcast state — loaded once, refreshed on a schedule, not queried per transaction.

### Merchant Reference Features (broadcast state)

- `ProductCD` — the only merchant-category signal in this dataset
- Fraud rate per `ProductCD`, computed once from training history, used as a merchant risk score proxy

This design keeps the hot path fast: transaction and identity data score in real time, while user and merchant data come from a broadcast cache that avoids a slow lookup on every single event.


### ONNX Runtime
Selecting ONNX Runtime (via `onnxruntime-java`) represents the optimal architectural decision. It delivers the ultra-low latency required to meet the project's SLA ($p99 < 100\text{ms}$) and outperforms alternative frameworks without introducing the compilation complexity of C-based runtimes (such as Treelite).