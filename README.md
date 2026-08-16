# btc-pool-miner

Distributed Bitcoin pool miner in Python. A **pool connector** speaks Stratum V1 to the
pool, and any number of **GPU miner** PCs pick up work through a GitHub repo used as a
shared message bus.

```
        Stratum V1                GitHub Contents API
 pool  <----------->  connector  <-------------------->  miner PC 0
                                    jobs.txt  (rewritten)   miner PC 1
                                    shares.txt(appended)    ...
```

| File | Role |
|---|---|
| `pool_connector.py` | Stratum client. Writes each new job to `jobs.txt` (full rewrite), polls `shares.txt`, submits every share to the pool, then clears the file and waits for the next one. |
| `miner.py` | Reads `jobs.txt`, builds the 80-byte header, hashes on GPU, appends found shares to `shares.txt`. |
| `sha256_torch.py` | Batched double-SHA-256 written in PyTorch tensor ops (CUDA if available, CPU fallback). |
| `gh_store.py` | Stdlib-only GitHub Contents-API helper with conflict retry. |
| `test_sha256.py` | Verifies the kernel against `hashlib` and real block 125552. |
| `config.json` | Pool URL, wallet, repo, batch size. |

## Setup

```bash
pip install torch          # CUDA build for real GPU mining
export GH_TOKEN=ghp_xxxxxxxxxxxx
```

## Run

Connector (one machine — it asks how many PCs are joining):

```bash
python pool_connector.py
python pool_connector.py --pcs 4 --create-repo   # non-interactive
```

Miner (one per PC, `--pc-id` is 0-based):

```bash
python miner.py --pc-id 0 --pcs 4
python miner.py --pc-id 1 --pcs 4        # on the second PC
```

Each PC gets its own extranonce2 prefix **and** its own 1/N slice of the 32-bit nonce
range, so no two machines ever repeat work.

## File formats

`jobs.txt` — always exactly one JSON line, the current job:

```json
{"job_id":"ca79f74d","prevhash":"...","coinb1":"...","coinb2":"...",
 "merkle_branch":[...],"version":"20000000","nbits":"1702...","ntime":"689...",
 "extranonce1":"00002095de61","extranonce2_size":6,"difficulty":262144.0,
 "target":"00000fff...","num_pcs":1,"issued_at":"..."}
```

`shares.txt` — one JSON line per share, emptied after submission:

```json
{"job_id":"ca79f74d","extranonce2":"000000000000","ntime":"689...",
 "nonce":"1a2b3c4d","hash":"0000...","worker":"pc0-host","found_at":"..."}
```

## Verified

* `test_sha256.py` reproduces block 125552's hash exactly — torch output matches `hashlib`.
* Connector authorizes on `btc.kryptex.network:7014` and writes live jobs to `jobs.txt`.
* An injected test share was picked up, submitted, answered by the pool, and `shares.txt`
  was cleared automatically.

## Reality check on hashrate

This is a correct, working miner, but a PyTorch SHA-256 does ~10⁵–10⁷ H/s while the
Bitcoin network runs at ~10²¹ H/s. Expect zero accepted shares at real pool difficulty —
treat it as a learning/instrumentation project, not an earner. Tune throughput with
`--batch`, and rotate the access token before using this repo for anything real.
