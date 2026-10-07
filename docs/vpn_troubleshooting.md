# VPN Troubleshooting Playbook

If you connect to the FEN gateway through a VPN, split-tunnel, or
SSH-over-HTTPS proxy, SDK calls may fail in ways that look like gateway
outages but are actually local network issues. This playbook covers the
five most common patterns and their fixes.

---

## 0. First sanity check

Before assuming VPN, run:

```python
import requests
r = requests.get("https://fen-esdh.ch/health", timeout=5)
print(r.status_code, r.text)
```

If this returns `200 {"status": "ok", ...}`, the gateway is reachable from
your laptop. If it hangs or errors, you have a network problem the SDK
cannot solve.

---

## 1. Connection refused / unreachable

**Symptom:** `requests.exceptions.ConnectionError: HTTPSConnectionPool(...)`

The SDK cannot reach the gateway. The VPN tunnel is not routing traffic
for `fen-esdh.ch` (or your local `localhost:8080`).

### 1.1 Split-tunnel

Some VPNs (notably Cisco AnyConnect with split-tunnel enabled) only route
corporate traffic; the FEN gateway hostname may not be on the corporate
include list.

**Fix:** Add `*.fen-esdh.ch` to the split-tunnel include list, or
disable split-tunnel for this session. Contact your IT department if
you cannot change this setting.

### 1.2 Local Docker on VPN

If `CESDH_DATA_HUB_ENDPOINT=http://localhost:8080` and you are on VPN,
`localhost` resolves to your laptop, not the Docker host. Use the host's
actual IP on the VPN subnet:

```bash
# Find the Docker host's VPN IP:
docker network inspect bridge | grep Gateway
# Or, if running on the FEN jump host:
echo "$JUMP_HOST_IP"  # usually something like 10.42.0.17
```

Then:

```bash
export CESDH_DATA_HUB_ENDPOINT=http://10.42.0.17:8080
```

### 1.3 Firewall or proxy blocking

Corporate firewalls sometimes block non-standard ports or require an
HTTP proxy. Check whether `curl -v https://fen-esdh.ch/health` succeeds.
If it only works through a proxy, set the standard environment variables:

```bash
export HTTPS_PROXY=http://proxy.corp.example.com:8080
```

---

## 2. SSL / TLS errors

### 2.1 `certificate verify failed`

**Symptom:** `requests.exceptions.SSLError: certificate verify failed`

The VPN's HTTPS interception certificate is not trusted by your Python
install. Most corporate VPNs install a CA cert in the OS trust store but
not in Python's `certifi` bundle.

**Fix:** Set `REQUESTS_CA_BUNDLE` to the VPN's CA cert path:

```bash
export REQUESTS_CA_BUNDLE=/etc/ssl/certs/corp-ca.pem
```

On macOS, you may need to export the cert from Keychain Access first:

```bash
security find-certificate -a -p /Library/Keychains/System.keychain > /tmp/corp-ca.pem
export REQUESTS_CA_BUNDLE=/tmp/corp-ca.pem
```

### 2.2 `ssl.SSLCertVerificationError`

This is the same underlying error surfaced through Python's `ssl` module
directly. The fix is the same: point `REQUESTS_CA_BUNDLE` at the right
CA bundle.

---

## 3. DNS resolution failures

**Symptom:** `socket.gaierror: Name or service not known` or
`getaddrinfo failed`

DNS does not resolve `fen-esdh.ch`. Most VPN clients push their own DNS
servers when the tunnel comes up; if the gateway hostname is not in the
pushed zone, lookups fail.

### 3.1 Use the IP directly

In your `.env` or shell:

```bash
export CESDH_DATA_HUB_ENDPOINT=http://10.42.0.17:8080
```

### 3.2 Add a hosts entry

| OS | File |
|---|---|
| Linux / macOS | `/etc/hosts` |
| Windows | `C:\Windows\System32\drivers\etc\hosts` |

Add a line:

```
10.42.0.17  fen-esdh.ch  gateway.fen-esdh.ch
```

Replace the IP with the actual gateway IP from your VPN administrator.

---

## 4. Slow first request (60+ seconds, then succeeds)

This is the **gateway cold-start**: Ollama loading the `llama3.1:8b`
model into memory on the first natural-language query after the stack
starts. The SDK's internal retry mechanism handles this, so the request
eventually succeeds, but the user-facing delay is real.

- **Wait it out.** The first call after `docker compose up` can take
  60-120 seconds. Subsequent calls are fast.
- **Pre-warm.** Run a no-op call right after the stack comes up:

  ```python
  import cesdh
  cesdh.list_datasets(repository="quickstart", limit=1)
  ```

- **Avoid `/search` for warm-up.** The `/search` endpoint triggers the
  LLM translator. Use `/catalog/datasets` (via `list_datasets`) or
  `/health` instead.

---

## 5. Certificate pinning (advanced)

If your VPN enforces certificate pinning and the gateway's certificate
chain does not match the pinned fingerprint, you will see
`SSL: CERTIFICATE_VERIFY_FAILED` with a specific fingerprint mismatch.
This is rare but happens with strict ZTNA setups (Cloudflare Access,
Zscaler Private Access).

**Workaround using the system trust store:**

```python
# pip install truststore
import truststore
truststore.inject_into_ssl()
```

Run this before importing `cesdh`. The `truststore` package delegates
certificate validation to the OS trust store, which is where the VPN
client installs its CA.

---

## 6. Getting help

If this playbook does not cover your symptom:

1. Run the sanity check from Section 0 and note the output.
2. Copy the full Python traceback.
3. Note your OS, Python version (`python3 --version`), and VPN client.
4. Send these to the ESDH manager with the subject line
   **"[CESDH Trial] VPN issue"**.
