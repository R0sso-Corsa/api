from __future__ import annotations

import os
from pathlib import Path


APP_DIR = Path(__file__).resolve().parent
PROJECT_DIR = APP_DIR.parent
DATA_DIR = PROJECT_DIR / "data"
SAMPLES_DIR = DATA_DIR / "samples"
STORE_DIR = DATA_DIR / "store"
TEMPLATES_DIR = PROJECT_DIR / "templates"

PLANNING_DATA_BASE_URL = "https://www.planning.data.gov.uk/entity.json"
PLANNING_DATA_AUTHORITY_CSV_URL = "https://files.planning.data.gov.uk/dataset/local-planning-authority.csv"
DEFAULT_SAMPLE_FILE = SAMPLES_DIR / "planning_data_applications.json"
DEFAULT_WATCHLIST_STORE = STORE_DIR / "watchlists.json"
DEFAULT_USAGE_STORE = STORE_DIR / "usage.json"
DEFAULT_SQLITE_DB = STORE_DIR / "plansignal.db"
DEMO_ORGANIZATION_NAME = "PlanSignal Demo Org"
DEMO_API_KEY = "plansignal-demo-key"
DEMO_USER_EMAIL = "demo@plansignal.local"
DEMO_USER_PASSWORD = "plansignal-demo-password"
LIVE_PLANNING_DEFAULT_LIMIT = 1000
LIVE_OVERLAY_DEFAULT_LIMIT = 200
SCHEDULER_POLL_SECONDS = int(os.getenv("PLANSIGNAL_SCHEDULER_POLL_SECONDS", "60"))
SESSION_TTL_HOURS = int(os.getenv("PLANSIGNAL_SESSION_TTL_HOURS", "24"))
WEBHOOK_MAX_ATTEMPTS = int(os.getenv("PLANSIGNAL_WEBHOOK_MAX_ATTEMPTS", "5"))
WEBHOOK_RETRY_BASE_SECONDS = int(os.getenv("PLANSIGNAL_WEBHOOK_RETRY_BASE_SECONDS", "60"))
WEBHOOK_SIGNATURE_TOLERANCE_SECONDS = int(os.getenv("PLANSIGNAL_WEBHOOK_SIGNATURE_TOLERANCE_SECONDS", "300"))
APP_BASE_URL = os.getenv("PLANSIGNAL_APP_BASE_URL", "http://localhost:8000")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
STRIPE_PRICE_STARTER = os.getenv("STRIPE_PRICE_STARTER", "price_starter_local")
STRIPE_PRICE_GROWTH = os.getenv("STRIPE_PRICE_GROWTH", "price_growth_local")
STRIPE_PRICE_SCALE = os.getenv("STRIPE_PRICE_SCALE", "price_scale_local")
SMTP_HOST = os.getenv("PLANSIGNAL_SMTP_HOST")
SMTP_PORT = int(os.getenv("PLANSIGNAL_SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("PLANSIGNAL_SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("PLANSIGNAL_SMTP_PASSWORD")
SMTP_USE_TLS = os.getenv("PLANSIGNAL_SMTP_USE_TLS", "true").lower() != "false"
SMTP_FROM_EMAIL = os.getenv("PLANSIGNAL_SMTP_FROM_EMAIL", "reports@plansignal.local")

PRIMARY_ICP = {
    "name": "Small and mid-sized planning consultancies and land buyers in England",
    "why_first": [
        "Already monitor planning activity manually",
        "Missed status changes have real commercial cost",
        "One won site or client can justify spend",
        "Sales cycle is shorter than large-enterprise procurement",
    ],
    "core_job": "Show relevant planning activity in target areas and explain when meaningful changes happen.",
    "pain_points": [
        "Multiple council portals",
        "Incomplete public coverage",
        "Manual status tracking",
        "Long document review",
        "Messy exports",
        "Missed changes between manual checks",
    ],
}

STAGE_MAP = [
    {
        "stage": 1,
        "name": "Define ICP",
        "status": "implemented",
        "code": ["plansignal/app/config.py", "plansignal/app/main.py"],
    },
    {
        "stage": 2,
        "name": "Define core schema",
        "status": "implemented",
        "code": ["plansignal/app/schemas.py"],
    },
    {
        "stage": 3,
        "name": "Set up source ingestion from Planning Data API",
        "status": "implemented-with-sample-fallback",
        "code": ["plansignal/app/services/ingestion.py"],
    },
    {
        "stage": 4,
        "name": "Build first normalizer",
        "status": "implemented",
        "code": ["plansignal/app/services/normalizer.py"],
    },
    {
        "stage": 5,
        "name": "Watchlists and alerts",
        "status": "implemented",
        "code": ["plansignal/app/main.py", "plansignal/app/services/storage.py", "plansignal/app/services/query.py"],
    },
    {
        "stage": 6,
        "name": "Delivery and premium API surface",
        "status": "implemented",
        "code": ["plansignal/app/main.py", "plansignal/app/services/query.py"],
    },
    {
        "stage": 7,
        "name": "Phase 2 foundation: persistence, auth, dashboard",
        "status": "implemented",
        "code": [
            "plansignal/app/main.py",
            "plansignal/app/services/db.py",
            "plansignal/app/services/auth.py",
            "plansignal/templates/dashboard.html",
        ],
    },
]
