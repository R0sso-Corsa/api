from __future__ import annotations

import csv
import html
import io
import json
import os
import re
import secrets
import sqlite3
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from flask import Flask, Response, g, jsonify, request, send_from_directory


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "root" / "stock_api"
DB_PATH = DATA_DIR / "stock_api.db"
DEFAULT_API_TOKEN = os.environ.get("STOCK_API_TOKEN", "change-me-local-token")
DEFAULT_HOST = os.environ.get("STOCK_API_HOST", "127.0.0.1")
DEFAULT_PORT = int(os.environ.get("STOCK_API_PORT", "8000"))
ALPHA_VANTAGE_API_KEY = os.environ.get(
    "ALPHA_VANTAGE_API_KEY",
    "FPP4ZN1O556ERK3K",
).strip()

app = Flask(__name__)


SCHEMA = """
CREATE TABLE IF NOT EXISTS symbols (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    exchange TEXT NOT NULL,
    currency TEXT NOT NULL,
    sector TEXT,
    industry TEXT,
    country TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quotes (
    symbol TEXT PRIMARY KEY,
    price REAL NOT NULL,
    change_amount REAL NOT NULL,
    change_percent REAL NOT NULL,
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    previous_close REAL NOT NULL,
    volume INTEGER NOT NULL,
    market_cap INTEGER,
    as_of TEXT NOT NULL,
    FOREIGN KEY(symbol) REFERENCES symbols(symbol) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS daily_bars (
    symbol TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open_price REAL NOT NULL,
    high_price REAL NOT NULL,
    low_price REAL NOT NULL,
    close_price REAL NOT NULL,
    adjusted_close REAL NOT NULL,
    volume INTEGER NOT NULL,
    PRIMARY KEY(symbol, trade_date),
    FOREIGN KEY(symbol) REFERENCES symbols(symbol) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_daily_bars_symbol_date
ON daily_bars(symbol, trade_date DESC);
"""


DEMO_SYMBOLS = [
    {
        "symbol": "AAPL",
        "name": "Apple Inc.",
        "exchange": "NASDAQ",
        "currency": "USD",
        "sector": "Technology",
        "industry": "Consumer Electronics",
        "country": "US",
    },
    {
        "symbol": "MSFT",
        "name": "Microsoft Corporation",
        "exchange": "NASDAQ",
        "currency": "USD",
        "sector": "Technology",
        "industry": "Software",
        "country": "US",
    },
    {
        "symbol": "NVDA",
        "name": "NVIDIA Corporation",
        "exchange": "NASDAQ",
        "currency": "USD",
        "sector": "Technology",
        "industry": "Semiconductors",
        "country": "US",
    },
]


DEMO_QUOTES = [
    {
        "symbol": "AAPL",
        "price": 198.11,
        "change_amount": 1.82,
        "change_percent": 0.93,
        "open_price": 196.80,
        "high_price": 199.48,
        "low_price": 196.12,
        "previous_close": 196.29,
        "volume": 61234567,
        "market_cap": 3045000000000,
        "as_of": "2026-04-10T20:00:00Z",
    },
    {
        "symbol": "MSFT",
        "name": "Microsoft Corporation",
        "price": 431.55,
        "change_amount": -2.14,
        "change_percent": -0.49,
        "open_price": 434.10,
        "high_price": 435.42,
        "low_price": 429.50,
        "previous_close": 433.69,
        "volume": 24112233,
        "market_cap": 3201000000000,
        "as_of": "2026-04-10T20:00:00Z",
    },
    {
        "symbol": "NVDA",
        "price": 121.84,
        "change_amount": 3.76,
        "change_percent": 3.18,
        "open_price": 118.54,
        "high_price": 122.10,
        "low_price": 117.90,
        "previous_close": 118.08,
        "volume": 552211998,
        "market_cap": 2988000000000,
        "as_of": "2026-04-10T20:00:00Z",
    },
]


DEMO_BARS = [
    ("AAPL", "2026-04-06", 194.52, 197.10, 193.88, 196.42, 196.42, 55123001),
    ("AAPL", "2026-04-07", 196.44, 198.00, 195.30, 197.55, 197.55, 48765019),
    ("AAPL", "2026-04-08", 197.70, 199.20, 196.92, 198.65, 198.65, 50321012),
    ("AAPL", "2026-04-09", 198.82, 199.01, 195.90, 196.29, 196.29, 53210744),
    ("AAPL", "2026-04-10", 196.80, 199.48, 196.12, 198.11, 198.11, 61234567),
    ("MSFT", "2026-04-06", 426.18, 429.70, 425.42, 428.81, 428.81, 21231090),
    ("MSFT", "2026-04-07", 429.04, 433.60, 428.70, 432.92, 432.92, 23114552),
    ("MSFT", "2026-04-08", 433.21, 436.18, 432.48, 435.04, 435.04, 22811045),
    ("MSFT", "2026-04-09", 435.26, 436.40, 432.22, 433.69, 433.69, 22001411),
    ("MSFT", "2026-04-10", 434.10, 435.42, 429.50, 431.55, 431.55, 24112233),
    ("NVDA", "2026-04-06", 114.30, 116.11, 113.88, 115.92, 115.92, 421006522),
    ("NVDA", "2026-04-07", 116.05, 117.40, 114.85, 116.88, 116.88, 398113200),
    ("NVDA", "2026-04-08", 117.14, 119.02, 116.44, 118.94, 118.94, 436005001),
    ("NVDA", "2026-04-09", 118.80, 119.26, 116.72, 118.08, 118.08, 409995210),
    ("NVDA", "2026-04-10", 118.54, 122.10, 117.90, 121.84, 121.84, 552211998),
]


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        ensure_data_dir()
        connection = sqlite3.connect(DB_PATH)
        connection.row_factory = sqlite3.Row
        g.db = connection
    return g.db


@app.teardown_appcontext
def close_db(_: BaseException | None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


@app.after_request
def disable_cache(response: Response) -> Response:
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.errorhandler(Exception)
def handle_unexpected_error(error: Exception) -> tuple[Response, int]:
    message = str(error).strip() or error.__class__.__name__
    return jsonify({"error": f"server error: {message}"}), 500


def init_db() -> None:
    ensure_data_dir()
    with sqlite3.connect(DB_PATH) as connection:
        connection.executescript(SCHEMA)
        connection.commit()


def upsert_symbol(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO symbols (
            symbol, name, exchange, currency, sector, industry, country, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            name=excluded.name,
            exchange=excluded.exchange,
            currency=excluded.currency,
            sector=excluded.sector,
            industry=excluded.industry,
            country=excluded.country,
            updated_at=excluded.updated_at
        """,
        (
            payload["symbol"].upper(),
            payload["name"],
            payload["exchange"],
            payload["currency"],
            payload.get("sector"),
            payload.get("industry"),
            payload.get("country"),
            utc_now(),
        ),
    )


def upsert_quote(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO quotes (
            symbol, price, change_amount, change_percent, open_price, high_price,
            low_price, previous_close, volume, market_cap, as_of
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            price=excluded.price,
            change_amount=excluded.change_amount,
            change_percent=excluded.change_percent,
            open_price=excluded.open_price,
            high_price=excluded.high_price,
            low_price=excluded.low_price,
            previous_close=excluded.previous_close,
            volume=excluded.volume,
            market_cap=excluded.market_cap,
            as_of=excluded.as_of
        """,
        (
            payload["symbol"].upper(),
            payload["price"],
            payload["change_amount"],
            payload["change_percent"],
            payload["open_price"],
            payload["high_price"],
            payload["low_price"],
            payload["previous_close"],
            payload["volume"],
            payload.get("market_cap"),
            payload.get("as_of", utc_now()),
        ),
    )


def upsert_bar(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
    connection.execute(
        """
        INSERT INTO daily_bars (
            symbol, trade_date, open_price, high_price, low_price, close_price,
            adjusted_close, volume
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol, trade_date) DO UPDATE SET
            open_price=excluded.open_price,
            high_price=excluded.high_price,
            low_price=excluded.low_price,
            close_price=excluded.close_price,
            adjusted_close=excluded.adjusted_close,
            volume=excluded.volume
        """,
        (
            payload["symbol"].upper(),
            payload["trade_date"],
            payload["open_price"],
            payload["high_price"],
            payload["low_price"],
            payload["close_price"],
            payload["adjusted_close"],
            payload["volume"],
        ),
    )


def seed_demo_data() -> None:
    with sqlite3.connect(DB_PATH) as connection:
        connection.row_factory = sqlite3.Row
        for symbol in DEMO_SYMBOLS:
            upsert_symbol(connection, symbol)
        for quote in DEMO_QUOTES:
            upsert_quote(connection, quote)
        for bar in DEMO_BARS:
            upsert_bar(
                connection,
                {
                    "symbol": bar[0],
                    "trade_date": bar[1],
                    "open_price": bar[2],
                    "high_price": bar[3],
                    "low_price": bar[4],
                    "close_price": bar[5],
                    "adjusted_close": bar[6],
                    "volume": bar[7],
                },
            )
        connection.commit()


def bootstrap() -> None:
    init_db()
    with sqlite3.connect(DB_PATH) as connection:
        row = connection.execute("SELECT COUNT(*) AS count FROM symbols").fetchone()
        count = row[0] if row is not None else 0
    if count == 0:
        seed_demo_data()


def require_api_token() -> Response | None:
    expected = DEFAULT_API_TOKEN
    presented = request.headers.get("X-API-Key", "")
    if not secrets.compare_digest(presented, expected):
        return jsonify({"error": "unauthorized"}), 401
    return None


def parse_positive_int(raw_value: str | None, default: int, cap: int) -> int:
    if raw_value is None:
        return default
    try:
        parsed = int(raw_value)
    except ValueError:
        return default
    return max(1, min(parsed, cap))


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {key: row[key] for key in row.keys()}


def quote_payload_from_bars(symbol_row: sqlite3.Row, latest_bar: dict[str, Any], previous_close: float) -> dict[str, Any]:
    change_amount = round(latest_bar["close_price"] - previous_close, 4)
    change_percent = round((change_amount / previous_close) * 100, 4) if previous_close else 0.0
    return {
        "symbol": symbol_row["symbol"],
        "price": latest_bar["close_price"],
        "change_amount": change_amount,
        "change_percent": change_percent,
        "open_price": latest_bar["open_price"],
        "high_price": latest_bar["high_price"],
        "low_price": latest_bar["low_price"],
        "previous_close": previous_close,
        "volume": latest_bar["volume"],
        "market_cap": None,
        "as_of": f"{latest_bar['trade_date']}T20:00:00Z",
    }


def build_alpha_vantage_url(symbol: str) -> str:
    params = urllib.parse.urlencode(
        {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol.upper(),
            "outputsize": "compact",
            "apikey": ALPHA_VANTAGE_API_KEY,
        }
    )
    return f"https://www.alphavantage.co/query?{params}"


def _normalize_provider_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        cleaned_parts = []
        for item in value:
            text = _normalize_provider_cell(item)
            if text:
                cleaned_parts.append(text)
        return ",".join(cleaned_parts)
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalize_provider_row(row: dict[Any, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in row.items():
        normalized_key = _normalize_provider_cell(key).lower()
        if not normalized_key:
            # csv.DictReader can put overflow columns under a None key.
            continue
        normalized[normalized_key] = _normalize_provider_cell(value)
    return normalized


def normalize_provider_header(header: str) -> str:
    collapsed = "".join(ch for ch in header.lower() if ch.isalnum())
    aliases = {
        "date": "date",
        "data": "date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "last": "close",
        "volume": "volume",
        "vol": "volume",
    }
    return aliases.get(collapsed, collapsed)


def build_provider_reader(raw_csv: str) -> csv.DictReader:
    lines = []
    for raw_line in raw_csv.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("<"):
            continue
        lines.append(line)

    if not lines:
        raise RuntimeError("stooq returned empty CSV payload")

    sample = "\n".join(lines[:5])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
        delimiter = dialect.delimiter
    except csv.Error:
        delimiter = ";"
        if sample.count(",") >= sample.count(";"):
            delimiter = ","

    reader = csv.reader(lines, delimiter=delimiter)
    headers: list[str] | None = None
    data_rows: list[list[str]] = []
    required_headers = {"date", "open", "high", "low", "close"}
    fallback_headers = ["date", "open", "high", "low", "close", "volume"]

    def looks_like_data_row(values: list[str]) -> bool:
        if len(values) < 5:
            return False
        first = values[0]
        if len(first) < 8 or first.count("-") < 2:
            return False
        return True

    for row in reader:
        cleaned = [_normalize_provider_cell(cell) for cell in row]
        if not any(cleaned):
            continue

        normalized_headers = [normalize_provider_header(cell) for cell in cleaned]
        if headers is None and required_headers.issubset(set(normalized_headers)):
            headers = normalized_headers
            continue

        if headers is None and looks_like_data_row(cleaned):
            headers = fallback_headers[: len(cleaned)]
            data_rows.append(cleaned)
            continue

        if headers is not None:
            data_rows.append(cleaned)

    if headers is None:
        date_row_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[,\t;| ].*)?$")
        parsed_rows: list[list[str]] = []
        for line in lines:
            if not date_row_pattern.match(line):
                continue
            for candidate_delimiter in [delimiter, ",", ";", "\t", "|", " "]:
                split = next(csv.reader([line], delimiter=candidate_delimiter))
                cleaned = [_normalize_provider_cell(cell) for cell in split if _normalize_provider_cell(cell)]
                if len(cleaned) >= 5:
                    parsed_rows.append(cleaned)
                    break

        if parsed_rows:
            headers = fallback_headers[: len(parsed_rows[0])]
            data_rows = parsed_rows
        else:
            print(f"Unrecognized provider CSV sample: {sample[:300]}")
            raise RuntimeError("stooq CSV header not recognized")

    return csv.DictReader(
        io.StringIO("\n".join([",".join(headers)] + [",".join(row) for row in data_rows]))
    )


def fetch_alpha_vantage_daily_history(symbol: str, exchange: str) -> list[dict[str, Any]]:
    if not ALPHA_VANTAGE_API_KEY:
        raise RuntimeError("Alpha Vantage API key missing")

    url = build_alpha_vantage_url(symbol)
    request_obj = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/135.0 Safari/537.36"
            )
        },
    )
    try:
        with urllib.request.urlopen(request_obj, timeout=20) as response:
            raw_body = response.read().decode("utf-8")
    except urllib.error.URLError as exc:
        raise RuntimeError(f"alpha vantage request failed: {exc}") from exc

    rows: list[dict[str, Any]] = []
    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("alpha vantage returned invalid JSON") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("alpha vantage returned unexpected payload")

    error_message = payload.get("Error Message") or payload.get("Information") or payload.get("Note")
    if error_message:
        raise RuntimeError(f"alpha vantage error: {error_message}")

    series = payload.get("Time Series (Daily)")
    if not isinstance(series, dict) or not series:
        raise RuntimeError("alpha vantage returned no daily series")

    malformed_example: dict[str, Any] | None = None
    for trade_date in sorted(series.keys()):
        day_values = series.get(trade_date)
        if not isinstance(day_values, dict):
            if malformed_example is None:
                malformed_example = {"trade_date": trade_date, "values": day_values}
            continue
        try:
            rows.append(
                {
                    "trade_date": trade_date,
                    "open_price": float(day_values["1. open"]),
                    "high_price": float(day_values["2. high"]),
                    "low_price": float(day_values["3. low"]),
                    "close_price": float(day_values["4. close"]),
                    "adjusted_close": float(day_values["4. close"]),
                    "volume": int(float(day_values.get("5. volume", 0) or 0)),
                }
            )
        except (KeyError, TypeError, ValueError):
            if malformed_example is None:
                malformed_example = {"trade_date": trade_date, "values": day_values}
            continue

    if not rows:
        if malformed_example is not None:
            print(f"Malformed Alpha Vantage row for {symbol}: {malformed_example}")
            raise RuntimeError("alpha vantage returned malformed daily rows")
        raise RuntimeError("alpha vantage returned no usable rows")
    return rows


def build_chart_svg(symbol: str, history_rows: list[dict[str, Any]]) -> str:
    width = 900
    height = 320
    padding = 36
    dates = [row["trade_date"] for row in history_rows]
    prices = [float(row["close_price"]) for row in history_rows]
    min_price = min(prices)
    max_price = max(prices)
    if max_price == min_price:
        max_price += 1.0
        min_price -= 1.0

    plot_width = width - (padding * 2)
    plot_height = height - (padding * 2)

    points = []
    for index, price in enumerate(prices):
        x = padding if len(prices) == 1 else padding + (plot_width * index / (len(prices) - 1))
        y = padding + ((max_price - price) / (max_price - min_price)) * plot_height
        points.append(f"{x:.2f},{y:.2f}")

    line_color = "#0f766e"
    fill_color = "#ccfbf1"
    text_color = "#0f172a"
    grid_color = "#cbd5e1"
    polyline = " ".join(points)
    bottom = height - padding
    area_points = f"{padding:.2f},{bottom:.2f} {polyline} {width - padding:.2f},{bottom:.2f}"
    last_price = prices[-1]

    return f"""<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\" role=\"img\" aria-label=\"{html.escape(symbol)} price chart\">
  <rect width=\"100%\" height=\"100%\" fill=\"#f8fafc\" rx=\"16\"/>
  <text x=\"{padding}\" y=\"28\" fill=\"{text_color}\" font-size=\"20\" font-family=\"Segoe UI, Arial, sans-serif\" font-weight=\"700\">{html.escape(symbol)} Close Price</text>
  <text x=\"{padding}\" y=\"50\" fill=\"{text_color}\" font-size=\"12\" font-family=\"Segoe UI, Arial, sans-serif\">{html.escape(dates[0])} to {html.escape(dates[-1])}</text>
  <line x1=\"{padding}\" y1=\"{bottom}\" x2=\"{width - padding}\" y2=\"{bottom}\" stroke=\"{grid_color}\" stroke-width=\"1\"/>
  <line x1=\"{padding}\" y1=\"{padding}\" x2=\"{padding}\" y2=\"{bottom}\" stroke=\"{grid_color}\" stroke-width=\"1\"/>
  <text x=\"8\" y=\"{padding + 4}\" fill=\"{text_color}\" font-size=\"11\" font-family=\"Segoe UI, Arial, sans-serif\">{max_price:.2f}</text>
  <text x=\"8\" y=\"{bottom}\" fill=\"{text_color}\" font-size=\"11\" font-family=\"Segoe UI, Arial, sans-serif\">{min_price:.2f}</text>
  <polygon points=\"{area_points}\" fill=\"{fill_color}\"/>
  <polyline points=\"{polyline}\" fill=\"none\" stroke=\"{line_color}\" stroke-width=\"3\" stroke-linejoin=\"round\" stroke-linecap=\"round\"/>
  <circle cx=\"{points[-1].split(',')[0]}\" cy=\"{points[-1].split(',')[1]}\" r=\"4\" fill=\"{line_color}\"/>
  <text x=\"{width - 140}\" y=\"28\" fill=\"{text_color}\" font-size=\"18\" font-family=\"Segoe UI, Arial, sans-serif\" font-weight=\"700\">${last_price:.2f}</text>
</svg>"""


@app.get("/")
def index() -> Response:
    return jsonify(
        {
            "name": "Local Stock API",
            "status": "ok",
            "version": "1.1.0",
            "docs": {
                "health": "/health",
                "dashboard": "/dashboard",
                "symbols": "/v1/symbols",
                "search": "/v1/search?q=app",
                "quote": "/v1/quotes/AAPL",
                "history": "/v1/history/AAPL?limit=5",
                "chart": "/v1/chart/AAPL.svg?days=30",
                "admin_upsert": "/v1/admin/symbols",
                "admin_import_csv": "/v1/admin/import/csv",
                "admin_sync_alpha_vantage": "/v1/admin/sync/alpha-vantage/AAPL",
            },
        }
    )


@app.get("/dashboard")
def dashboard() -> Response:
    return send_from_directory(BASE_DIR, "new.html")


@app.get("/health")
def health() -> Response:
    db = get_db()
    count_row = db.execute("SELECT COUNT(*) AS count FROM symbols").fetchone()
    return jsonify(
        {
            "status": "healthy",
            "database": str(DB_PATH),
            "symbol_count": count_row["count"] if count_row else 0,
            "time": utc_now(),
        }
    )


@app.get("/v1/symbols")
def list_symbols() -> Response:
    db = get_db()
    limit = parse_positive_int(request.args.get("limit"), default=25, cap=250)
    rows = db.execute(
        """
        SELECT symbol, name, exchange, currency, sector, industry, country, updated_at
        FROM symbols
        ORDER BY symbol ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return jsonify({"items": [row_to_dict(row) for row in rows], "count": len(rows)})


@app.get("/v1/search")
def search_symbols() -> Response:
    query = (request.args.get("q") or "").strip()
    if not query:
        return jsonify({"error": "missing query param q"}), 400
    db = get_db()
    limit = parse_positive_int(request.args.get("limit"), default=10, cap=50)
    wildcard = f"%{query.upper()}%"
    rows = db.execute(
        """
        SELECT symbol, name, exchange, currency, sector, industry, country, updated_at
        FROM symbols
        WHERE UPPER(symbol) LIKE ? OR UPPER(name) LIKE ?
        ORDER BY symbol ASC
        LIMIT ?
        """,
        (wildcard, wildcard, limit),
    ).fetchall()
    return jsonify(
        {
            "query": query,
            "items": [row_to_dict(row) for row in rows],
            "count": len(rows),
        }
    )


@app.get("/v1/quotes/<symbol>")
def get_quote(symbol: str) -> Response:
    db = get_db()
    row = db.execute(
        """
        SELECT
            s.symbol, s.name, s.exchange, s.currency, s.sector, s.industry, s.country,
            q.price, q.change_amount, q.change_percent, q.open_price, q.high_price,
            q.low_price, q.previous_close, q.volume, q.market_cap, q.as_of
        FROM symbols s
        JOIN quotes q ON q.symbol = s.symbol
        WHERE s.symbol = ?
        """,
        (symbol.upper(),),
    ).fetchone()
    if row is None:
        return jsonify({"error": "symbol not found"}), 404
    return jsonify(row_to_dict(row))


@app.get("/v1/history/<symbol>")
def get_history(symbol: str) -> Response:
    db = get_db()
    limit = parse_positive_int(request.args.get("limit"), default=30, cap=3650)
    rows = db.execute(
        """
        SELECT symbol, trade_date, open_price, high_price, low_price, close_price,
               adjusted_close, volume
        FROM daily_bars
        WHERE symbol = ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (symbol.upper(), limit),
    ).fetchall()
    if not rows:
        return jsonify({"error": "symbol not found"}), 404
    return jsonify({"symbol": symbol.upper(), "items": [row_to_dict(row) for row in rows]})


@app.get("/v1/chart/<symbol>.svg")
def get_chart(symbol: str) -> Response:
    db = get_db()
    days = parse_positive_int(request.args.get("days"), default=30, cap=3650)
    rows = db.execute(
        """
        SELECT trade_date, close_price
        FROM daily_bars
        WHERE symbol = ?
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        (symbol.upper(), days),
    ).fetchall()
    if not rows:
        return jsonify({"error": "symbol not found"}), 404
    history_rows = [row_to_dict(row) for row in reversed(rows)]
    svg = build_chart_svg(symbol.upper(), history_rows)
    return Response(svg, mimetype="image/svg+xml")


@app.post("/v1/admin/symbols")
def admin_upsert_symbol() -> Response:
    auth_error = require_api_token()
    if auth_error is not None:
        return auth_error

    payload = request.get_json(silent=True) or {}
    required = {"symbol", "name", "exchange", "currency"}
    missing = sorted(required - payload.keys())
    if missing:
        return jsonify({"error": f"missing fields: {', '.join(missing)}"}), 400

    db = get_db()
    upsert_symbol(db, payload)
    db.commit()
    return jsonify({"status": "ok", "symbol": payload["symbol"].upper()})


@app.post("/v1/admin/quotes")
def admin_upsert_quote() -> Response:
    auth_error = require_api_token()
    if auth_error is not None:
        return auth_error

    payload = request.get_json(silent=True) or {}
    required = {
        "symbol",
        "price",
        "change_amount",
        "change_percent",
        "open_price",
        "high_price",
        "low_price",
        "previous_close",
        "volume",
    }
    missing = sorted(required - payload.keys())
    if missing:
        return jsonify({"error": f"missing fields: {', '.join(missing)}"}), 400

    db = get_db()
    symbol_exists = db.execute(
        "SELECT 1 FROM symbols WHERE symbol = ?",
        (payload["symbol"].upper(),),
    ).fetchone()
    if symbol_exists is None:
        return jsonify({"error": "symbol must exist before quote insert"}), 400
    upsert_quote(db, payload)
    db.commit()
    return jsonify({"status": "ok", "symbol": payload["symbol"].upper()})


@app.post("/v1/admin/bars")
def admin_upsert_bar() -> Response:
    auth_error = require_api_token()
    if auth_error is not None:
        return auth_error

    payload = request.get_json(silent=True) or {}
    required = {
        "symbol",
        "trade_date",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "adjusted_close",
        "volume",
    }
    missing = sorted(required - payload.keys())
    if missing:
        return jsonify({"error": f"missing fields: {', '.join(missing)}"}), 400

    db = get_db()
    symbol_exists = db.execute(
        "SELECT 1 FROM symbols WHERE symbol = ?",
        (payload["symbol"].upper(),),
    ).fetchone()
    if symbol_exists is None:
        return jsonify({"error": "symbol must exist before bar insert"}), 400
    upsert_bar(db, payload)
    db.commit()
    return jsonify(
        {
            "status": "ok",
            "symbol": payload["symbol"].upper(),
            "trade_date": payload["trade_date"],
        }
    )


@app.post("/v1/admin/import/csv")
def admin_import_csv() -> Response:
    auth_error = require_api_token()
    if auth_error is not None:
        return auth_error

    if "file" not in request.files:
        return jsonify({"error": "attach CSV as multipart form field named file"}), 400

    symbol = (request.form.get("symbol") or "").upper().strip()
    if not symbol:
        return jsonify({"error": "form field symbol required"}), 400

    db = get_db()
    symbol_exists = db.execute(
        "SELECT 1 FROM symbols WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    if symbol_exists is None:
        return jsonify({"error": "symbol must exist before CSV import"}), 400

    file_storage = request.files["file"]
    raw_text = file_storage.stream.read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(raw_text))

    imported = 0
    for row in reader:
        upsert_bar(
            db,
            {
                "symbol": symbol,
                "trade_date": row["trade_date"],
                "open_price": float(row["open_price"]),
                "high_price": float(row["high_price"]),
                "low_price": float(row["low_price"]),
                "close_price": float(row["close_price"]),
                "adjusted_close": float(row.get("adjusted_close", row["close_price"])),
                "volume": int(row["volume"]),
            },
        )
        imported += 1
    db.commit()
    return jsonify({"status": "ok", "symbol": symbol, "imported_rows": imported})


@app.post("/v1/admin/sync/stooq/<symbol>")
@app.post("/v1/admin/sync/alpha-vantage/<symbol>")
def admin_sync_alpha_vantage(symbol: str) -> Response:
    auth_error = require_api_token()
    if auth_error is not None:
        return auth_error

    db = get_db()
    symbol_row = db.execute(
        "SELECT symbol, name, exchange, currency FROM symbols WHERE symbol = ?",
        (symbol.upper(),),
    ).fetchone()
    if symbol_row is None:
        return jsonify({"error": "symbol not found"}), 404

    try:
        remote_rows = fetch_alpha_vantage_daily_history(symbol_row["symbol"], symbol_row["exchange"])
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 502
    except Exception as exc:
        return jsonify({"error": f"live sync failed: {exc}"}), 500

    for remote_row in remote_rows:
        upsert_bar(
            db,
            {
                "symbol": symbol_row["symbol"],
                **remote_row,
            },
        )

    latest = remote_rows[-1]
    previous_close = remote_rows[-2]["close_price"] if len(remote_rows) > 1 else latest["close_price"]
    try:
        upsert_quote(db, quote_payload_from_bars(symbol_row, latest, previous_close))
    except Exception as exc:
        return jsonify({"error": f"quote update failed: {exc}"}), 500
    db.commit()
    return jsonify(
        {
            "status": "ok",
            "symbol": symbol_row["symbol"],
            "provider": "alpha_vantage",
            "rows_synced": len(remote_rows),
            "latest_trade_date": latest["trade_date"],
            "price": latest["close_price"],
        }
    )


@app.get("/v1/export/<symbol>")
def export_symbol_history(symbol: str) -> Response:
    db = get_db()
    rows = db.execute(
        """
        SELECT trade_date, open_price, high_price, low_price, close_price,
               adjusted_close, volume
        FROM daily_bars
        WHERE symbol = ?
        ORDER BY trade_date ASC
        """,
        (symbol.upper(),),
    ).fetchall()
    if not rows:
        return jsonify({"error": "symbol not found"}), 404

    output = io.StringIO()
    writer = csv.DictWriter(
        output,
        fieldnames=[
            "trade_date",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "adjusted_close",
            "volume",
        ],
    )
    writer.writeheader()
    for row in rows:
        writer.writerow(row_to_dict(row))

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={symbol.upper()}_history.csv"},
    )


def create_app() -> Flask:
    bootstrap()
    return app


if __name__ == "__main__":
    bootstrap()
    print(f"Stock API DB: {DB_PATH}")
    print(f"Stock API token: {DEFAULT_API_TOKEN}")
    print(f"Alpha Vantage API key configured: {'yes' if ALPHA_VANTAGE_API_KEY else 'no'}")
    print(f"Listening on http://{DEFAULT_HOST}:{DEFAULT_PORT}")
    app.run(host=DEFAULT_HOST, port=DEFAULT_PORT, debug=False)
