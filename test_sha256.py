"""Validate the torch SHA-256d kernel against hashlib and real block 125552."""
import binascii, hashlib, struct, torch
from sha256_torch import hash_batch, midstate, pick_device

def sha256d(b): return hashlib.sha256(hashlib.sha256(b).digest()).digest()

ver = struct.pack("<I", 1)
prev = binascii.unhexlify("00000000000008a3a41b85b8b29ad444def299fee21793cd8b9e567eab02cd81")[::-1]
mrk = binascii.unhexlify("2b12fcf1b09288fcaff797d71e950e71ae42b91e8bdb2304758dfcffc2b620e3")[::-1]
tim = struct.pack("<I", 1305998791)
bits = struct.pack("<I", 0x1a44b9f2)
nonce_le = struct.pack("<I", 2504433986)
header76 = ver + prev + mrk + tim + bits
nv = struct.unpack(">I", nonce_le)[0]

want = sha256d(header76 + nonce_le)[::-1].hex()
print("expected:", want)
assert want == "00000000000000001e8d6829a8a21adc5d38d0a473b144b6765798e61f98bd1d"

dev = pick_device("auto")
w = list(struct.unpack(">19I", header76))
mid, K_t = midstate(w[:16], dev)
nonces = torch.tensor([nv, nv + 1, nv + 2], dtype=torch.int64, device=dev)
out = hash_batch(mid, K_t, w[16:19], nonces, dev)

for i, n in enumerate(nonces.tolist()):
    digest = b"".join(struct.pack(">I", int(x)) for x in out[i].tolist())
    got = digest[::-1].hex()
    ref = sha256d(header76 + struct.pack(">I", n))[::-1].hex()
    print(f"nonce {n}: torch={got[:24]} hashlib={ref[:24]} {'OK' if got == ref else 'MISMATCH'}")
    assert got == ref
print("word[7]==0 test for real block:", int(out[0, 7].item()) == 0)
print("ALL TESTS PASSED")
