"""
access_control.py
=================
User access management for the stock market report.

Two roles:
  basic    (free)  — price history only: price chart, volume, performance,
                     volatility, price correlation heatmap, summary table
  advanced (paid)  — everything above + technical indicators per ticker,
                     indicator correlation heatmap (SMA/RSI/MACD/Bollinger)

Users are stored in users.json next to this file.
On first run, four demo accounts are created automatically.

Usage
-----
    from access_control import UserManager, User

    mgr  = UserManager()

    # Authenticate (raises PermissionError on bad credentials)
    user = mgr.authenticate("analyst", "analyst123")

    # Check a specific feature
    if user.can("technical_indicators"):
        ...

    # Or let the manager raise for you
    mgr.require(user, "technical_indicators")
"""

import hashlib
import json
import os
from datetime import datetime

# ---------------------------------------------------------------------------
# What each role can see in the report
# ---------------------------------------------------------------------------

PERMISSIONS = {
    "basic": {
        "summary_table",
        "price_timeseries",
        "normalized_performance",
        "volume",
        "volatility",
        "price_correlation",
    },
    "advanced": {
        "summary_table",
        "price_timeseries",
        "normalized_performance",
        "volume",
        "volatility",
        "price_correlation",
        "indicator_correlation",
        "technical_indicators",   # per-ticker SMA/Bollinger/RSI/MACD panels
    },
}

ROLE_LABEL = {
    "basic":    "Basic (Free)",
    "advanced": "Advanced (Paid)",
}


# ---------------------------------------------------------------------------
# User
# ---------------------------------------------------------------------------

class User:
    def __init__(self, username: str, role: str, full_name: str = ""):
        if role not in PERMISSIONS:
            raise ValueError(f"Unknown role '{role}'.")
        self.username  = username
        self.role      = role
        self.full_name = full_name or username

    def can(self, feature: str) -> bool:
        return feature in PERMISSIONS[self.role]

    def label(self) -> str:
        return ROLE_LABEL[self.role]

    def __repr__(self):
        return f"<User '{self.username}' role='{self.role}'>"


# ---------------------------------------------------------------------------
# UserManager
# ---------------------------------------------------------------------------

class UserManager:
    """
    File-backed user store (users.json).
    Created automatically with demo accounts on first run.
    """

    _DEFAULTS = [
        # username        password         role        full_name
        ("admin",         "admin123",      "advanced", "Admin"),
        ("analyst",       "analyst123",    "advanced", "Senior Analyst"),
        ("basic_user",    "basic123",      "basic",    "Basic User"),
        ("viewer",        "viewer123",     "basic",    "Read-Only Viewer"),
    ]

    def __init__(self, store_path: str = None):
        if store_path is None:
            store_path = os.path.join(os.path.dirname(__file__), "users.json")
        self.store_path = store_path
        self._db: dict = {}
        self._load()
        if not self._db:
            self._seed()

    # ── persistence ──────────────────────────────────────────────────────────

    def _load(self):
        if os.path.exists(self.store_path):
            with open(self.store_path) as f:
                self._db = json.load(f)

    def _save(self):
        with open(self.store_path, "w") as f:
            json.dump(self._db, f, indent=2)

    def _seed(self):
        for username, password, role, full_name in self._DEFAULTS:
            self._db[username] = {
                "hash":      self._hash(password),
                "role":      role,
                "full_name": full_name,
                "created":   datetime.now().isoformat(),
            }
        self._save()
        print(f"[Access] Created {len(self._DEFAULTS)} default users → {self.store_path}")

    @staticmethod
    def _hash(pw: str) -> str:
        return hashlib.sha256(pw.encode()).hexdigest()

    # ── public API ────────────────────────────────────────────────────────────

    def authenticate(self, username: str, password: str) -> User:
        """Return a User on success, raise PermissionError on failure."""
        rec = self._db.get(username)
        if rec is None or rec["hash"] != self._hash(password):
            raise PermissionError(f"Invalid username or password for '{username}'.")
        return User(username, rec["role"], rec.get("full_name", username))

    def require(self, user: User, feature: str) -> None:
        """Raise PermissionError if user cannot access feature."""
        if not user.can(feature):
            raise PermissionError(
                f"'{user.username}' ({user.label()}) does not have access to '{feature}'. "
                f"Upgrade to Advanced to unlock this."
            )

    def create_user(self, username: str, password: str, role: str, full_name: str = "") -> None:
        if username in self._db:
            raise ValueError(f"User '{username}' already exists.")
        if role not in PERMISSIONS:
            raise ValueError(f"Invalid role '{role}'. Must be: {list(PERMISSIONS)}")
        self._db[username] = {
            "hash":      self._hash(password),
            "role":      role,
            "full_name": full_name or username,
            "created":   datetime.now().isoformat(),
        }
        self._save()
        print(f"[Access] User '{username}' created with role '{role}'.")

    def update_role(self, username: str, new_role: str) -> None:
        if username not in self._db:
            raise KeyError(f"User '{username}' not found.")
        if new_role not in PERMISSIONS:
            raise ValueError(f"Invalid role '{new_role}'.")
        self._db[username]["role"] = new_role
        self._save()
        print(f"[Access] '{username}' role changed to '{new_role}'.")

    def list_users(self) -> list[dict]:
        return [
            {"username": u, "role": d["role"], "full_name": d.get("full_name", u)}
            for u, d in self._db.items()
        ]


# ---------------------------------------------------------------------------
# Quick demo when run directly
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mgr = UserManager()

    print("\n── Users ──────────────────────────────")
    for u in mgr.list_users():
        print(f"  {u['username']:15s}  {u['role']}")

    print("\n── Permission matrix ───────────────────")
    all_features = sorted(set(f for p in PERMISSIONS.values() for f in p))
    print(f"  {'Feature':<30} {'basic':^8} {'advanced':^10}")
    print(f"  {'-'*30} {'-'*8} {'-'*10}")
    for feat in all_features:
        b = "✅" if feat in PERMISSIONS["basic"]    else "❌"
        a = "✅" if feat in PERMISSIONS["advanced"] else "❌"
        print(f"  {feat:<30} {b:^8} {a:^10}")

    print("\n── Auth demo ───────────────────────────")
    u = mgr.authenticate("basic_user", "basic123")
    print(f"  Logged in: {u}")
    print(f"  can('price_timeseries')     → {u.can('price_timeseries')}")
    print(f"  can('technical_indicators') → {u.can('technical_indicators')}")