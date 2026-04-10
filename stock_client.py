from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request


DEFAULT_BASE_URL = "http://127.0.0.1:8000"


def fetch_json(url: str) -> dict:
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read().decode("utf-8"))


def build_url(base_url: str, path: str, params: dict[str, str] | None = None) -> str:
    query = f"?{urllib.parse.urlencode(params)}" if params else ""
    return f"{base_url.rstrip('/')}{path}{query}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Tiny client for local stock API")
    parser.add_argument(
        "command",
        choices=["health", "symbols", "search", "quote", "history"],
        help="API action to run",
    )
    parser.add_argument("--symbol", help="Ticker symbol, e.g. AAPL")
    parser.add_argument("--query", help="Search query")
    parser.add_argument("--limit", type=int, default=5, help="Result limit")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL")
    args = parser.parse_args()

    if args.command in {"quote", "history"} and not args.symbol:
        parser.error("--symbol required for quote/history")
    if args.command == "search" and not args.query:
        parser.error("--query required for search")

    if args.command == "health":
        url = build_url(args.base_url, "/health")
    elif args.command == "symbols":
        url = build_url(args.base_url, "/v1/symbols", {"limit": str(args.limit)})
    elif args.command == "search":
        url = build_url(
            args.base_url,
            "/v1/search",
            {"q": args.query or "", "limit": str(args.limit)},
        )
    elif args.command == "quote":
        url = build_url(args.base_url, f"/v1/quotes/{args.symbol.upper()}")
    else:
        url = build_url(
            args.base_url,
            f"/v1/history/{args.symbol.upper()}",
            {"limit": str(args.limit)},
        )

    try:
        payload = fetch_json(url)
    except Exception as exc:
        print(f"request failed: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
