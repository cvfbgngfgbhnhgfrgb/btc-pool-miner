#!/usr/bin/env python3
"""
pool_connector.py
=================
Talks Stratum V1 to the mining pool and uses a GitHub repo as the bus:

  pool  --job-->  jobs.txt   (REWRITTEN on every new job, never appended)
  pool  <-share-- shares.txt (polled; every line submitted, then file CLEARED)

Run this on ONE machine. Miners (miner.py) can run on any number of PCs.

    export GH_TOKEN=ghp_xxx
    python pool_connector.py            # asks how many PCs are joining
    python pool_connector.py --pcs 4    # or pass it
"""

import argparse
import binascii
import hashlib
import json
import os
import queue
import socket
import struct
import sys
import threading
import time
from datetime import datetime, timezone

from gh_store import GitHubStore

HERE = os.path.dirname(os.path.abspath(__file__))


def log(*a):
    print(f"[{datetime.now().strftime('%H:%M:%S')}]", *a, flush=True)


def load_config():
    with open(os.path.join(HERE, "config.json")) as f:
        return json.load(f)


def parse_stratum_url(url):
    u = url.replace("stratum+tcp://", "").replace("stratum://", "")
    host, _, port = u.partition(":")
    return host, int(port or 3333)


# --------------------------------------------------------------------------
# Stratum V1 client
# --------------------------------------------------------------------------
class StratumClient:
    def __init__(self, host, port, user, password):
        self.host, self.port = host, port
        self.user, self.password = user, password
        self.sock = None
        self.rfile = None
        self.lock = threading.Lock()
        self.msg_id = 1
        self.responses = {}
        self.resp_evt = threading.Event()

        self.extranonce1 = None
        self.extranonce2_size = 4
        self.difficulty = 1.0
        self.job_queue = queue.Queue()
        self.running = False
        self.accepted = 0
        self.rejected = 0

    # -- connection --
    def connect(self):
        log(f"connecting to {self.host}:{self.port} ...")
        self.sock = socket.create_connection((self.host, self.port), timeout=30)
        self.sock.settimeout(None)
        self.rfile = self.sock.makefile("r", encoding="utf-8", newline="\n")
        self.running = True
        threading.Thread(target=self._reader, daemon=True).start()

        sub = self._call("mining.subscribe", ["btc-pool-miner/1.0"], wait=True)
        if not sub or sub.get("error"):
            raise RuntimeError(f"subscribe failed: {sub}")
        res = sub["result"]
        self.extranonce1 = res[1]
        self.extranonce2_size = int(res[2])
        log(f"subscribed  extranonce1={self.extranonce1} en2_size={self.extranonce2_size}")

        auth = self._call("mining.authorize", [self.user, self.password], wait=True)
        if not auth or auth.get("result") is not True:
            raise RuntimeError(f"authorize failed: {auth}")
        log(f"authorized as {self.user}")

    def _send(self, obj):
        line = json.dumps(obj) + "\n"
        with self.lock:
            self.sock.sendall(line.encode())

    def _call(self, method, params, wait=False, timeout=30):
        with self.lock:
            mid = self.msg_id
            self.msg_id += 1
        self._send_raw({"id": mid, "method": method, "params": params})
        if not wait:
            return mid
        deadline = time.time() + timeout
        while time.time() < deadline:
            if mid in self.responses:
                return self.responses.pop(mid)
            time.sleep(0.02)
        return None

    def _send_raw(self, obj):
        line = json.dumps(obj) + "\n"
        with self.lock:
            self.sock.sendall(line.encode())

    def _reader(self):
        try:
            for line in self.rfile:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if msg.get("id") is not None and "method" not in msg:
                    self.responses[msg["id"]] = msg
                else:
                    self._handle_notify(msg)
        except Exception as e:
            log("reader stopped:", e)
        finally:
            self.running = False

    def _handle_notify(self, msg):
        m = msg.get("method")
        p = msg.get("params", [])
        if m == "mining.notify":
            job = {
                "job_id": p[0], "prevhash": p[1], "coinb1": p[2], "coinb2": p[3],
                "merkle_branch": p[4], "version": p[5], "nbits": p[6],
                "ntime": p[7], "clean_jobs": bool(p[8]),
            }
            self.job_queue.put(job)
            log(f"new job {job['job_id']} clean={job['clean_jobs']}")
        elif m == "mining.set_difficulty":
            self.difficulty = float(p[0])
            log(f"difficulty -> {self.difficulty}")
        elif m == "mining.set_extranonce":
            self.extranonce1 = p[0]
            self.extranonce2_size = int(p[1])
            log("extranonce updated")
        elif m == "client.reconnect":
            log("pool asked to reconnect")
            self.running = False

    def submit(self, job_id, extranonce2, ntime, nonce):
        r = self._call("mining.submit",
                       [self.user, job_id, extranonce2, ntime, nonce],
                       wait=True, timeout=20)
        ok = bool(r and r.get("result") is True)
        if ok:
            self.accepted += 1
        else:
            self.rejected += 1
        return ok, (r or {}).get("error")

    def close(self):
        self.running = False
        try:
            self.sock.close()
        except Exception:
            pass


# --------------------------------------------------------------------------
# helpers shared with the miner
# --------------------------------------------------------------------------
def bits_to_target(nbits_hex):
    nbits = int(nbits_hex, 16)
    exp = nbits >> 24
    mant = nbits & 0xFFFFFF
    return mant * (1 << (8 * (exp - 3)))


def difficulty_to_target(diff):
    diff1 = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
    return int(diff1 / max(diff, 1e-12))


# --------------------------------------------------------------------------
# main loop
# --------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pcs", type=int, default=None,
                    help="number of PCs (miners) joining the swarm")
    ap.add_argument("--create-repo", action="store_true",
                    help="create the GitHub repo if it does not exist")
    ap.add_argument("--private", action="store_true")
    args = ap.parse_args()

    cfg = load_config()

    num_pcs = args.pcs or cfg["swarm"].get("num_pcs") or 0
    if not args.pcs:
        try:
            ans = input(f"How many PCs are joining? [{num_pcs or 1}]: ").strip()
            if ans:
                num_pcs = int(ans)
        except EOFError:
            pass
    num_pcs = max(1, int(num_pcs or 1))
    log(f"swarm size: {num_pcs} PC(s) -> nonce space split into {num_pcs} slice(s)")

    g = cfg["github"]
    store = GitHubStore(g["owner"], g["repo"], g["branch"],
                        os.environ.get(g["token_env"]))
    if args.create_repo:
        created = store.ensure_repo(args.private, "Distributed BTC pool miner (jobs.txt / shares.txt bus)")
        log("repo created" if created else "repo already exists")

    host, port = parse_stratum_url(cfg["pool"]["url"])
    client = StratumClient(host, port, cfg["pool"]["user"], cfg["pool"]["password"])
    client.connect()

    jobs_path = g["jobs_path"]
    shares_path = g["shares_path"]
    poll = float(g.get("poll_seconds", 3))

    # make sure shares.txt exists and is empty at startup
    store.put_file(shares_path, "", "init: clear shares.txt")
    log(f"cleared {shares_path}")

    last_job = None
    stop = threading.Event()

    # ---- thread A: pool -> jobs.txt (rewrite) ----
    def job_writer():
        nonlocal last_job
        while not stop.is_set():
            try:
                job = client.job_queue.get(timeout=1)
            except queue.Empty:
                continue
            # drain to newest job only
            while True:
                try:
                    job = client.job_queue.get_nowait()
                except queue.Empty:
                    break
            doc = {
                "job_id": job["job_id"],
                "prevhash": job["prevhash"],
                "coinb1": job["coinb1"],
                "coinb2": job["coinb2"],
                "merkle_branch": job["merkle_branch"],
                "version": job["version"],
                "nbits": job["nbits"],
                "ntime": job["ntime"],
                "clean_jobs": job["clean_jobs"],
                "extranonce1": client.extranonce1,
                "extranonce2_size": client.extranonce2_size,
                "difficulty": client.difficulty,
                "target": f"{difficulty_to_target(client.difficulty):064x}",
                "num_pcs": num_pcs,
                "issued_at": datetime.now(timezone.utc).isoformat(),
            }
            last_job = doc
            try:
                # REWRITE jobs.txt (single current job, one JSON line)
                store.put_file(jobs_path, json.dumps(doc) + "\n",
                               f"job {doc['job_id']} @ diff {doc['difficulty']}")
                log(f"jobs.txt <- job {doc['job_id']} (diff {doc['difficulty']})")
            except Exception as e:
                log("job write error:", e)

    # ---- thread B: shares.txt -> pool, then clear ----
    def share_submitter():
        last_sha = None
        while not stop.is_set():
            try:
                text, sha = store.get_file(shares_path)
                if text is None:
                    time.sleep(poll)
                    continue
                lines = [l for l in text.splitlines() if l.strip()]
                if not lines:
                    last_sha = sha
                    time.sleep(poll)
                    continue
                if sha == last_sha:
                    time.sleep(poll)
                    continue
                log(f"{len(lines)} share(s) found in shares.txt")
                for line in lines:
                    try:
                        s = json.loads(line)
                    except json.JSONDecodeError:
                        log("bad share line skipped:", line[:80])
                        continue
                    ok, err = client.submit(s["job_id"], s["extranonce2"],
                                            s["ntime"], s["nonce"])
                    tag = "ACCEPTED" if ok else f"REJECTED {err}"
                    log(f"  share {s['nonce']} from {s.get('worker','?')}: {tag}")
                # clear the file so miners can write fresh shares
                new_sha = store.put_file(shares_path, "",
                                         f"submitted {len(lines)} share(s)", sha)
                last_sha = new_sha
                log(f"{shares_path} cleared; waiting for next share")
            except Exception as e:
                log("share loop error:", e)
            time.sleep(poll)

    ta = threading.Thread(target=job_writer, daemon=True)
    tb = threading.Thread(target=share_submitter, daemon=True)
    ta.start(); tb.start()

    try:
        while client.running:
            time.sleep(5)
        log("connection lost")
    except KeyboardInterrupt:
        log("stopping...")
    finally:
        stop.set()
        client.close()
        log(f"accepted={client.accepted} rejected={client.rejected}")


if __name__ == "__main__":
    main()
