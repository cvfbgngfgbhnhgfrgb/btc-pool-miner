#!/usr/bin/env python3
"""
miner.py
========
GPU (PyTorch/CUDA) Bitcoin miner worker.

  jobs.txt (GitHub)  --> build block header --> hash on GPU
  found share        --> appended to shares.txt (GitHub)
                         pool_connector.py submits it and clears the file.

Run one instance per PC:

    export GH_TOKEN=ghp_xxx
    python miner.py --pc-id 0 --pcs 1

--pc-id splits the 32-bit nonce space so PCs never duplicate work.
"""

import argparse
import binascii
import hashlib
import json
import os
import struct
import time
from datetime import datetime, timezone

import torch

from gh_store import GitHubStore
from sha256_torch import hash_batch, midstate, pick_device

HERE = os.path.dirname(os.path.abspath(__file__))


def log(*a):
    print(f"[{datetime.now().strftime('%H:%M:%S')}]", *a, flush=True)


def load_config():
    with open(os.path.join(HERE, "config.json")) as f:
        return json.load(f)


def sha256d(b):
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


def swap_endian_words(hex_str):
    """Stratum prevhash: 8 x 4-byte words, each needing byte reversal."""
    b = binascii.unhexlify(hex_str)
    return b"".join(b[i:i + 4][::-1] for i in range(0, len(b), 4))


def build_header_parts(job, extranonce2_hex):
    """Return (header_first76_bytes, target_int)."""
    coinbase = binascii.unhexlify(
        job["coinb1"] + job["extranonce1"] + extranonce2_hex + job["coinb2"])
    merkle_root = sha256d(coinbase)
    for branch in job["merkle_branch"]:
        merkle_root = sha256d(merkle_root + binascii.unhexlify(branch))

    version = binascii.unhexlify(job["version"])[::-1]
    prevhash = swap_endian_words(job["prevhash"])
    ntime = binascii.unhexlify(job["ntime"])[::-1]
    nbits = binascii.unhexlify(job["nbits"])[::-1]
    header76 = version + prevhash + merkle_root + ntime + nbits
    assert len(header76) == 76, len(header76)
    return header76


def header_words(header76):
    """19 big-endian uint32 words of the first 76 header bytes."""
    return list(struct.unpack(">19I", header76))


def mine_job(job, store, cfg, pc_id, num_pcs, shares_path, worker_name,
             device, deadline_check):
    """Scan this PC's nonce slice for the given job. Returns hashes done."""
    en2_size = int(job["extranonce2_size"])
    target = int(job["target"], 16)
    batch = int(cfg["miner"]["batch_size"])

    # each PC gets its own extranonce2 prefix too -> fully disjoint search space
    extranonce2 = (pc_id).to_bytes(en2_size, "big").hex()
    header76 = build_header_parts(job, extranonce2)
    w = header_words(header76)
    mid, K_t = midstate(w[:16], device)
    tail = w[16:19]

    hi_target = min(target >> 224, 0xFFFFFFFF)  # GPU prefilter threshold
    span = (1 << 32) // num_pcs
    start = pc_id * span
    end = start + span
    hashes = 0
    t0 = time.time()
    nonce = start

    while nonce < end:
        if deadline_check():
            break
        n = min(batch, end - nonce)
        nonces = torch.arange(nonce, nonce + n, dtype=torch.int64, device=device)
        out = hash_batch(mid, K_t, tail, nonces, device)
        # Prefilter: the most-significant 32 bits of the little-endian hash are
        # digest word[7] byte-swapped. Keep candidates <= target >> 224.
        w7 = out[:, 7]
        msb32 = (((w7 & 0xFF) << 24) | ((w7 & 0xFF00) << 8) |
                 ((w7 >> 8) & 0xFF00) | ((w7 >> 24) & 0xFF))
        cand = torch.nonzero(msb32 <= hi_target, as_tuple=False).flatten()
        hashes += n
        nonce += n

        for idx in cand.tolist():
            nv = int(nonces[idx].item())
            hdr = header76 + struct.pack(">I", nv)
            h = sha256d(hdr)
            hv = int.from_bytes(h[::-1], "big")
            if hv <= target:
                share = {
                    "job_id": job["job_id"],
                    "extranonce2": extranonce2,
                    "ntime": job["ntime"],
                    "nonce": struct.pack(">I", nv).hex(),
                    "hash": h[::-1].hex(),
                    "worker": worker_name,
                    "found_at": datetime.now(timezone.utc).isoformat(),
                }
                log(f"SHARE FOUND nonce={share['nonce']} hash={share['hash'][:20]}...")
                try:
                    store.append_file(shares_path, json.dumps(share),
                                      f"share {share['nonce']} from {worker_name}")
                    log("share written to shares.txt")
                except Exception as e:
                    log("share upload failed:", e)

        if hashes % (batch * 8) == 0:
            hs = hashes / max(time.time() - t0, 1e-6)
            log(f"job {job['job_id']}  {hashes/1e6:.2f} MH  {hs/1e3:.1f} kH/s")

    return hashes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pc-id", type=int, default=0, help="0-based index of this PC")
    ap.add_argument("--pcs", type=int, default=None, help="total PCs in the swarm")
    ap.add_argument("--worker", default=None, help="label written into shares")
    ap.add_argument("--device", default=None, choices=["auto", "cuda", "cpu"])
    ap.add_argument("--batch", type=int, default=None)
    args = ap.parse_args()

    cfg = load_config()
    if args.batch:
        cfg["miner"]["batch_size"] = args.batch
    g = cfg["github"]
    store = GitHubStore(g["owner"], g["repo"], g["branch"],
                        os.environ.get(g["token_env"]))

    device = pick_device(args.device or cfg["miner"].get("device", "auto"))
    log(f"device: {device}" + (f" ({torch.cuda.get_device_name(0)})"
                               if device.type == "cuda" else " (no CUDA found)"))
    worker = args.worker or f"pc{args.pc_id}-{os.uname().nodename if hasattr(os,'uname') else 'win'}"

    jobs_path = g["jobs_path"]
    shares_path = g["shares_path"]
    poll = float(g.get("poll_seconds", 3))

    cur_id = None
    total = 0
    t_start = time.time()

    while True:
        try:
            text, _ = store.get_file(jobs_path)
            if not text or not text.strip():
                log("waiting for a job in jobs.txt ...")
                time.sleep(poll)
                continue
            job = json.loads(text.strip().splitlines()[-1])
        except Exception as e:
            log("job fetch error:", e)
            time.sleep(poll)
            continue

        num_pcs = args.pcs or int(job.get("num_pcs", 1))
        if job["job_id"] == cur_id:
            time.sleep(poll)
            continue
        cur_id = job["job_id"]
        log(f"mining job {cur_id}  diff={job.get('difficulty')}  "
            f"slice {args.pc_id + 1}/{num_pcs}")

        last_check = [time.time()]

        def deadline_check():
            # every few seconds, see whether the pool pushed a newer job
            if time.time() - last_check[0] < max(poll, 5):
                return False
            last_check[0] = time.time()
            try:
                t, _ = store.get_file(jobs_path)
                if t and t.strip():
                    newest = json.loads(t.strip().splitlines()[-1])
                    return newest["job_id"] != cur_id
            except Exception:
                pass
            return False

        try:
            total += mine_job(job, store, cfg, args.pc_id, num_pcs,
                              shares_path, worker, device, deadline_check)
        except KeyboardInterrupt:
            break
        except Exception as e:
            log("mining error:", e)
            time.sleep(poll)

        log(f"total {total/1e6:.2f} MH  avg {total/max(time.time()-t_start,1e-6)/1e3:.1f} kH/s")


if __name__ == "__main__":
    main()
