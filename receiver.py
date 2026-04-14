import hashlib
import hmac
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


def verify_signature(secret: str, timestamp: str, body: bytes, signature: str) -> bool:
    expected = "v1=" + hmac.new(
        secret.encode("utf-8"),
        timestamp.encode("utf-8") + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature, expected)


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(length)
        body = raw_body.decode("utf-8", errors="replace")
        signing_secret = os.getenv("PLANSIGNAL_WEBHOOK_SECRET", "").strip()
        max_skew_seconds = int(os.getenv("PLANSIGNAL_WEBHOOK_MAX_SKEW_SECONDS", "300"))
        timestamp = self.headers.get("X-PlanSignal-Timestamp", "")
        signature = self.headers.get("X-PlanSignal-Signature", "")
        event_type = self.headers.get("X-PlanSignal-Event", "")

        if signing_secret:
            try:
                timestamp_value = int(timestamp)
            except ValueError:
                timestamp_value = 0
            age_seconds = abs(int(time.time()) - timestamp_value)
            if (
                not timestamp
                or not signature
                or timestamp_value <= 0
                or age_seconds > max_skew_seconds
                or not verify_signature(signing_secret, timestamp, raw_body, signature)
            ):
                print("\n=== WEBHOOK REJECTED ===")
                print(self.path)
                print(f"event={event_type or 'unknown'}")
                print(f"signature verification failed or timestamp outside {max_skew_seconds}s window")
                self.send_response(401)
                self.end_headers()
                self.wfile.write(b"invalid signature")
                return

        print("\n=== WEBHOOK ===")
        print(self.path)
        if event_type:
            print(f"event={event_type}")
        if signature:
            print(f"signature={signature}")
        print(body)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

HTTPServer(("127.0.0.1", 9001), Handler).serve_forever()
"""
$env:PLANSIGNAL_WEBHOOK_SECRET="pswhsec_PeHZTUYWSqiJ2IpnO77nnKvJUnHeMlJc"
python receiver.py
"""