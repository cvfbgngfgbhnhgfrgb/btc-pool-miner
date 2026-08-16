"""Tiny GitHub Contents-API helper used as the shared 'message bus'
between pool_connector.py (writes jobs.txt, consumes shares.txt) and
miner.py (reads jobs.txt, appends shares.txt).

Only stdlib is used so the miner box needs nothing but python + torch.
"""

import base64
import json
import os
import time
import urllib.error
import urllib.request

API = "https://api.github.com"


class GitHubStore:
    def __init__(self, owner, repo, branch="main", token=None):
        self.owner = owner
        self.repo = repo
        self.branch = branch
        self.token = token or os.environ.get("GH_TOKEN", "")
        if not self.token:
            raise RuntimeError("No GitHub token: set GH_TOKEN env var.")

    # ---------------- low level ----------------
    def _req(self, method, path, body=None, retries=4):
        url = path if path.startswith("http") else API + path
        data = json.dumps(body).encode() if body is not None else None
        for attempt in range(retries):
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header("Authorization", "Bearer " + self.token)
            req.add_header("Accept", "application/vnd.github+json")
            req.add_header("X-GitHub-Api-Version", "2022-11-28")
            req.add_header("User-Agent", "btc-pool-miner")
            if data:
                req.add_header("Content-Type", "application/json")
            try:
                with urllib.request.urlopen(req, timeout=30) as r:
                    raw = r.read()
                    return r.status, (json.loads(raw) if raw else {})
            except urllib.error.HTTPError as e:
                raw = e.read()
                try:
                    payload = json.loads(raw)
                except Exception:
                    payload = {"message": raw.decode("utf-8", "replace")}
                # 409/422 = race on sha, 5xx = transient -> retry
                if e.code in (409, 422, 500, 502, 503) and attempt < retries - 1:
                    time.sleep(1.0 + attempt)
                    continue
                return e.code, payload
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(1.0 + attempt)
                    continue
                raise
        return 0, {}

    # ---------------- repo ----------------
    def ensure_repo(self, private=False, description=""):
        st, _ = self._req("GET", f"/repos/{self.owner}/{self.repo}")
        if st == 200:
            return False
        st, resp = self._req("POST", "/user/repos", {
            "name": self.repo,
            "private": private,
            "description": description,
            "auto_init": True,
        })
        if st not in (200, 201):
            raise RuntimeError(f"repo create failed: {st} {resp}")
        time.sleep(2)
        return True

    # ---------------- files ----------------
    def get_file(self, path):
        """-> (text, sha) ; (None, None) if missing."""
        st, resp = self._req(
            "GET", f"/repos/{self.owner}/{self.repo}/contents/{path}?ref={self.branch}")
        if st == 404:
            return None, None
        if st != 200:
            raise RuntimeError(f"get_file {path}: {st} {resp}")
        content = resp.get("content", "")
        text = base64.b64decode(content).decode("utf-8", "replace") if content else ""
        return text, resp.get("sha")

    def put_file(self, path, text, message, sha=None):
        """Overwrite (rewrite) a file. Returns new sha, or None on conflict."""
        if sha is None:
            _, sha = self.get_file(path)
        body = {
            "message": message,
            "content": base64.b64encode(text.encode()).decode(),
            "branch": self.branch,
        }
        if sha:
            body["sha"] = sha
        st, resp = self._req(
            "PUT", f"/repos/{self.owner}/{self.repo}/contents/{path}", body)
        if st in (200, 201):
            return resp["content"]["sha"]
        if st in (409, 422):
            return None
        raise RuntimeError(f"put_file {path}: {st} {resp}")

    def append_file(self, path, text, message, attempts=6):
        """Read-modify-write append; retries on concurrent-write conflicts."""
        for i in range(attempts):
            cur, sha = self.get_file(path)
            cur = cur or ""
            if cur and not cur.endswith("\n"):
                cur += "\n"
            new_sha = self.put_file(path, cur + text, message, sha)
            if new_sha:
                return new_sha
            time.sleep(0.5 + i * 0.7)
        raise RuntimeError(f"append_file {path}: too many conflicts")
