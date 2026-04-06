import streamlit as st
import streamlit.components.v1 as st_components
import pandas as pd
import anthropic
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import base64
import threading
import urllib.parse
import secrets
import requests as _http
import hmac
import hashlib
import re
from datetime import datetime, timedelta
import io

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Feature Request Dashboard",
    page_icon="logo.svg",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS (matches L2 dashboard styling) ────────────────
st.markdown("""
<style>
    .stApp { background-color: #2D333B; }
    .block-container { padding-top: 1rem !important; }
    [data-testid="stAppViewBlockContainer"] { padding-top: 1rem !important; }
    header[data-testid="stHeader"] { height: 2rem !important; }

    .header-container {
        display: flex; align-items: center; gap: 16px; padding: 0 0 0.25rem 0;
    }
    .header-container img { width: 48px; height: 48px; }
    .header-container h1 { color: #00E676; margin: 0; font-size: 2rem; }
    .header-subtitle { color: #9E9E9E; font-size: 0.95rem; margin-top: -4px; padding-bottom: 1rem; }

    [data-testid="stMetric"] {
        background-color: #373E47; border: 1px solid #444C56; border-radius: 10px; padding: 16px;
    }
    [data-testid="stMetricLabel"] { color: #9E9E9E !important; }
    [data-testid="stMetricValue"] { color: #E0E0E0 !important; }
    [data-testid="stMetricDelta"] { color: #00E676 !important; }

    [data-testid="stSidebar"] { background-color: #333A44; border-right: 1px solid #444C56; }
    [data-testid="stSidebar"] .stMarkdown h2 { color: #00E676; }

    .stTabs [data-baseweb="tab"] { color: #9E9E9E; }
    .stTabs [aria-selected="true"] { color: #00E676 !important; border-bottom-color: #00E676 !important; }

    .stButton > button[kind="primary"] {
        background-color: #00E676; color: #2D333B; border: none; font-weight: 600;
    }
    .stButton > button[kind="primary"]:hover { background-color: #00C853; color: #2D333B; }

    .stDownloadButton > button {
        background-color: #373E47; color: #00E676; border: 1px solid #00E676;
    }
    .stDownloadButton > button:hover { background-color: #00E676; color: #2D333B; }

    .streamlit-expanderHeader { color: #E0E0E0; background-color: #373E47; }
    hr { border-color: #444C56; }
    [data-baseweb="select"] { background-color: #373E47; }
    .stDataFrame { border: 1px solid #444C56; border-radius: 8px; overflow-x: auto !important; }

    .progress-banner {
        background-color: #1A2F1A; border: 1px solid #00E676; border-radius: 8px;
        padding: 12px 20px; margin-bottom: 16px;
    }
    .progress-banner .progress-text { color: #00E676; font-weight: 600; }

    .cat-stat-card {
        background-color: #373E47; border: 1px solid #444C56; border-radius: 8px;
        padding: 12px 16px; margin-bottom: 8px;
    }
    .cat-stat-card .cat-name { color: #00E676; font-weight: 600; font-size: 0.9rem; }
    .cat-stat-card .cat-detail { color: #9E9E9E; font-size: 0.8rem; }

    /* Hide the zero-height iframe used for JS injection */
    iframe[height="0"] { display: block !important; position: absolute; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# CONFIGURATION — Update these to match your Google Sheet
# ============================================================

_APP_DIR = os.path.dirname(os.path.abspath(__file__))

SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "YOUR_SHEET_ID_HERE")
MAIN_TAB = os.environ.get("MAIN_TAB_NAME", "Stories")

# Map logical keys → exact column header names in your sheet.
# Set any value to "" to disable that column.
COLUMNS = {
    "id":           "id",
    "timestamp":    "created_at",
    "title":        "name",
    "description":  "description",
    "type":         "type",           # "feature", "bug", "chore" in Shortcut
    "product_area": "product_area",
    "submitter":    "requester",
    "owners":       "owners",
    "company":      "",               # not in Shortcut exports
    "priority":     "priority",
    "severity":     "severity",
    "status":       "state",
    "labels":       "labels",
    "epic":         "epic",
    "team":         "team",
    "use_case":     "",
    "impact":       "",
    "link":         "app_url",
}

# Exact value in the "type" column for feature requests.
FEATURE_REQUEST_TYPE = "feature"

# Email domain for internal team members — tickets submitted by this domain
# with no customer keywords in the description are pre-filtered as internal.
INTERNAL_DOMAIN = "fieldguide.io"

# Keywords that suggest a customer is mentioned in the description.
CUSTOMER_KEYWORDS = [
    "customer", "client", "account", "partner", "user", "they ", "their team",
    "company", "org ", "organization", "enterprise", "prospect", "vendor",
]

# How many tickets to send Claude per batch during analysis.
ANALYSIS_BATCH_SIZE = 20

# Columns shown in the main data table (logical key names from COLUMNS dict above)
DISPLAY_KEYS = ["id", "title", "link", "timestamp", "submitter", "contact", "product_area", "priority", "severity", "status", "labels", "epic"]

# Max tickets sent to the chatbot as context
CHATBOT_MAX_TICKETS = None  # No limit — include all tickets

# Persistent data directory — use Railway volume mount if available, else local
_DATA_DIR = "/data" if os.path.isdir("/data") else _APP_DIR
CONTACTS_FILE = os.path.join(_DATA_DIR, "contacts.json")
CONTACTS_PROGRESS_FILE = os.path.join(_DATA_DIR, "contacts_progress.json")
SUMMARIES_FILE = os.path.join(_DATA_DIR, "fr_summaries.json")
SUMMARIES_PROGRESS_FILE = os.path.join(_DATA_DIR, "fr_summaries_progress.json")
SUMMARY_BATCH_SIZE = 20

_contacts_lock = threading.Lock()
_summaries_lock = threading.Lock()

# ============================================================
# GOOGLE SHEETS AUTH
# ============================================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]


@st.cache_resource
def get_gsheet_client():
    """Return an authenticated gspread client, or None on failure."""
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
        return gspread.authorize(creds)

    try:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]), scopes=SCOPES
        )
        return gspread.authorize(creds)
    except Exception:
        pass

    if os.path.exists("service_account.json"):
        creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
        return gspread.authorize(creds)

    return None


# ============================================================
# GOOGLE OAUTH AUTHENTICATION
# ============================================================

import streamlit.components.v1 as _stc

_AUTH_COOKIE = "fg_fr_auth"
_COOKIE_TTL_HOURS = 24


def _set_auth_cookie(user_info):
    """Set the auth cookie via JavaScript (invisible component)."""
    encoded = _encode_auth(user_info)
    max_age = _COOKIE_TTL_HOURS * 3600
    _stc.html(
        f'<script>document.cookie="{_AUTH_COOKIE}={encoded}; path=/; max-age={max_age}; SameSite=Lax";</script>',
        height=0,
    )


def _clear_auth_cookie():
    """Delete the auth cookie via JavaScript."""
    _stc.html(
        f'<script>document.cookie="{_AUTH_COOKIE}=; path=/; max-age=0";</script>',
        height=0,
    )


def _read_auth_cookie():
    """Read the auth cookie from HTTP request headers."""
    try:
        return st.context.cookies.get(_AUTH_COOKIE)
    except Exception:
        return None


def _encode_auth(user):
    secret = os.environ.get("COOKIE_SECRET", os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "fg-dashboard"))
    exp = (datetime.utcnow() + timedelta(hours=_COOKIE_TTL_HOURS)).isoformat()
    payload = json.dumps({"u": user, "e": exp}, separators=(",", ":"))
    sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return base64.b64encode(f"{payload}.{sig}".encode()).decode()


def _decode_auth(value):
    try:
        secret = os.environ.get("COOKIE_SECRET", os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "fg-dashboard"))
        decoded = base64.b64decode(value.encode()).decode()
        payload_str, sig = decoded.rsplit(".", 1)
        expected = hmac.new(secret.encode(), payload_str.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return None
        data = json.loads(payload_str)
        if datetime.fromisoformat(data["e"]) < datetime.utcnow():
            return None
        return data["u"]
    except Exception:
        return None


_ALLOWED_DOMAIN = "fieldguide.io"
_GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v3/userinfo"
_OAUTH_SCOPES = "openid email profile"
_ACCESS_LOG_TAB = "Access Log"
_ACCESS_LOG_FILE = os.path.join(_APP_DIR, "access_log.json")
_ACTIVITY_LOG_TAB = "Activity Log"
_ACTIVITY_LOG_FILE = os.path.join(_APP_DIR, "activity_log.json")


def _get_oauth_creds():
    client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "")
    client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "")
    redirect_uri = os.environ.get("GOOGLE_OAUTH_REDIRECT_URI", "http://localhost:8501")
    return client_id, client_secret, redirect_uri


def _build_auth_params():
    client_id, _, redirect_uri = _get_oauth_creds()
    state = secrets.token_urlsafe(32)
    return {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": _OAUTH_SCOPES,
        "state": state,
        "access_type": "online",
        "prompt": "select_account",
    }


def _exchange_code(code, state):

    client_id, client_secret, redirect_uri = _get_oauth_creds()
    try:
        resp = _http.post(_GOOGLE_TOKEN_URL, data={
            "code": code,
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
        }, timeout=10)
    except Exception as e:
        return None, f"Token exchange error: {e}"

    if not resp.ok:
        return None, "Token exchange failed. Please try again."

    access_token = resp.json().get("access_token")
    if not access_token:
        return None, "No access token received."

    try:
        ui_resp = _http.get(_GOOGLE_USERINFO_URL,
            headers={"Authorization": f"Bearer {access_token}"}, timeout=10)
    except Exception as e:
        return None, f"User info error: {e}"

    if not ui_resp.ok:
        return None, "Failed to retrieve user info."

    info = ui_resp.json()
    email = info.get("email", "")
    if not email.lower().endswith(f"@{_ALLOWED_DOMAIN}"):
        return None, f"Access denied. Only @{_ALLOWED_DOMAIN} accounts are permitted."

    return {"email": email, "name": info.get("name", email), "picture": info.get("picture", "")}, None


def _show_login_page():
    client_id, _, _ = _get_oauth_creds()
    if not client_id:
        st.error(
            "Google OAuth is not configured. "
            "Set GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET, and "
            "GOOGLE_OAUTH_REDIRECT_URI environment variables."
        )
        return

    if "_auth_error" in st.session_state:
        st.error(st.session_state.pop("_auth_error"))

    auth_params = _build_auth_params()

    logo_html = ""
    logo_path = os.path.join(_APP_DIR, "logo.svg")
    if os.path.exists(logo_path):
        with open(logo_path, "r") as f:
            logo_svg = f.read()
        logo_b64 = base64.b64encode(logo_svg.encode()).decode()
        logo_html = f'<img src="data:image/svg+xml;base64,{logo_b64}" style="width:60px;height:60px;margin-bottom:8px;" />'

    hidden_fields = "".join(
        f'<input type="hidden" name="{k}" value="{v}">'
        for k, v in auth_params.items()
    )

    st.markdown(f"""
    <style>
        .stApp {{ background-color: #2D333B; }}
        .login-wrapper {{
            display: flex; justify-content: center; align-items: center;
            min-height: 70vh; padding: 2rem;
        }}
        .login-card {{
            background-color: #373E47; border: 1px solid #444C56; border-radius: 16px;
            padding: 48px 40px; max-width: 420px; width: 100%; text-align: center;
            box-shadow: 0 8px 32px rgba(0,0,0,0.4);
        }}
        .login-card h1 {{ color: #00E676; font-size: 1.7rem; margin: 12px 0 8px 0; }}
        .login-card .login-sub {{ color: #9E9E9E; font-size: 0.95rem; margin-bottom: 32px; }}
        .login-note {{ color: #616a75; font-size: 0.75rem; margin-top: 20px; }}
        .google-btn {{
            display: inline-flex; align-items: center; gap: 12px;
            background-color: #ffffff; color: #3c4043; font-size: 15px;
            font-weight: 500; padding: 12px 24px; border-radius: 8px;
            border: 1px solid #dadce0; cursor: pointer;
            font-family: -apple-system, sans-serif;
        }}
        .google-btn:hover {{ background-color: #f8f9fa; box-shadow: 0 2px 8px rgba(0,0,0,0.25); }}
    </style>
    <div class="login-wrapper">
        <div class="login-card">
            {logo_html}
            <h1>Feature Request Dashboard</h1>
            <div class="login-sub">Sign in with your Fieldguide Google account to continue.</div>
            <form action="{_GOOGLE_AUTH_URL}" method="GET">
                {hidden_fields}
                <button type="submit" class="google-btn">
                    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 48 48" width="20" height="20"><path fill="#4285F4" d="M44.5 20H24v8.5h11.8C34.7 33.9 29.1 37 24 37c-7.2 0-13-5.8-13-13s5.8-13 13-13c3.1 0 5.9 1.1 8.1 2.9l6.4-6.4C34.6 4.1 29.6 2 24 2 11.8 2 2 11.8 2 24s9.8 22 22 22c11 0 21-8 21-22 0-1.3-.2-2.7-.5-4z"/><path fill="#34A853" d="M6.3 14.7l7 5.1C15.1 16.2 19.2 13 24 13c3.1 0 5.9 1.1 8.1 2.9l6.4-6.4C34.6 4.1 29.6 2 24 2 16.2 2 9.4 7.3 6.3 14.7z"/><path fill="#FBBC05" d="M24 46c5.5 0 10.5-1.8 14.4-4.9l-6.7-5.5C29.7 37.5 27 38.5 24 38.5c-5.1 0-9.4-3.2-11.1-7.7l-7 5.4C9.2 42.3 16.1 46 24 46z"/><path fill="#EA4335" d="M44.5 20H24v8.5h11.8c-1 3-3.2 5.5-6.1 7.1l6.7 5.5C41.1 37.3 45 31.1 45 24c0-1.3-.2-2.7-.5-4z"/></svg>
                    Sign in with Google
                </button>
            </form>
            <div class="login-note">Only @fieldguide.io accounts are permitted.</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def _is_admin():
    email = st.session_state.get("_auth_user", {}).get("email", "").lower()
    raw = os.environ.get("DASHBOARD_ADMIN_EMAILS", "")
    admins = [e.strip().lower() for e in raw.split(",") if e.strip()]
    return bool(admins) and email in admins


def _log_visit(user_info):
    entry = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "email": user_info.get("email", ""),
        "name": user_info.get("name", ""),
    }
    # Local JSON file
    try:
        existing = []
        if os.path.exists(_ACCESS_LOG_FILE):
            with open(_ACCESS_LOG_FILE, "r") as f:
                existing = json.load(f)
        existing.append(entry)
        with open(_ACCESS_LOG_FILE, "w") as f:
            json.dump(existing, f, indent=2)
    except Exception:
        pass
    # Google Sheets
    try:
        client = get_gsheet_client()
        if client and SHEET_ID != "YOUR_SHEET_ID_HERE":
            ss = client.open_by_key(SHEET_ID)
            try:
                ws = ss.worksheet(_ACCESS_LOG_TAB)
            except Exception:
                ws = ss.add_worksheet(title=_ACCESS_LOG_TAB, rows=5000, cols=3)
                ws.append_row(["Timestamp", "Email", "Name"])
            ws.append_row([entry["timestamp"], entry["email"], entry["name"]])
    except Exception:
        pass


def _load_access_log():
    try:
        client = get_gsheet_client()
        if client and SHEET_ID != "YOUR_SHEET_ID_HERE":
            ss = client.open_by_key(SHEET_ID)
            ws = ss.worksheet(_ACCESS_LOG_TAB)
            rows = ws.get_all_records()
            if rows:
                return pd.DataFrame(rows)
    except Exception:
        pass
    if os.path.exists(_ACCESS_LOG_FILE):
        try:
            with open(_ACCESS_LOG_FILE, "r") as f:
                data = json.load(f)
            if data:
                return pd.DataFrame(data).rename(
                    columns={"timestamp": "Timestamp", "email": "Email", "name": "Name"}
                )
        except Exception:
            pass
    return pd.DataFrame(columns=["Timestamp", "Email", "Name"])


# ── Activity tracking helpers ────────────────────────────────────────────────

def _log_session_start(user_info, session_id):
    """Log a new session to the Activity Log sheet. Returns the sheet row number."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry = {
        "session_id": session_id,
        "email": user_info.get("email", ""),
        "name": user_info.get("name", ""),
        "start": now,
        "last_active": now,
        "duration_min": 0,
    }
    # Local JSON
    try:
        existing = []
        if os.path.exists(_ACTIVITY_LOG_FILE):
            with open(_ACTIVITY_LOG_FILE, "r") as f:
                existing = json.load(f)
        existing.append(entry)
        with open(_ACTIVITY_LOG_FILE, "w") as f:
            json.dump(existing, f, indent=2)
    except Exception:
        pass
    # Google Sheets
    try:
        client = get_gsheet_client()
        if client and SHEET_ID != "YOUR_SHEET_ID_HERE":
            ss = client.open_by_key(SHEET_ID)
            try:
                ws = ss.worksheet(_ACTIVITY_LOG_TAB)
            except Exception:
                ws = ss.add_worksheet(title=_ACTIVITY_LOG_TAB, rows=5000, cols=6)
                ws.append_row(["Session ID", "Email", "Name", "Start", "Last Active", "Duration (min)"])
            result = ws.append_row([
                session_id, entry["email"], entry["name"], now, now, "0"
            ])
            try:
                updated_range = result["updates"]["updatedRange"]
                row_num = int(re.search(r"A(\d+)", updated_range.split("!")[-1]).group(1))
                return row_num
            except Exception:
                return None
    except Exception:
        pass
    return None


def _send_heartbeat(session_id, session_start, sheet_row):
    """Update the Activity Log with current timestamp and duration."""
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    duration_min = round((now - session_start).total_seconds() / 60, 1)
    # Update local JSON
    try:
        if os.path.exists(_ACTIVITY_LOG_FILE):
            with open(_ACTIVITY_LOG_FILE, "r") as f:
                existing = json.load(f)
            for entry in existing:
                if entry.get("session_id") == session_id:
                    entry["last_active"] = now_str
                    entry["duration_min"] = duration_min
            with open(_ACTIVITY_LOG_FILE, "w") as f:
                json.dump(existing, f, indent=2)
    except Exception:
        pass
    # Update Google Sheets row
    if sheet_row:
        try:
            client = get_gsheet_client()
            if client and SHEET_ID != "YOUR_SHEET_ID_HERE":
                ss = client.open_by_key(SHEET_ID)
                ws = ss.worksheet(_ACTIVITY_LOG_TAB)
                ws.update(f"E{sheet_row}:F{sheet_row}", [[now_str, str(duration_min)]])
        except Exception:
            pass


def _load_activity_log():
    """Return activity log as a DataFrame."""
    try:
        client = get_gsheet_client()
        if client and SHEET_ID != "YOUR_SHEET_ID_HERE":
            ss = client.open_by_key(SHEET_ID)
            ws = ss.worksheet(_ACTIVITY_LOG_TAB)
            rows = ws.get_all_records()
            if rows:
                return pd.DataFrame(rows)
    except Exception:
        pass
    if os.path.exists(_ACTIVITY_LOG_FILE):
        try:
            with open(_ACTIVITY_LOG_FILE, "r") as f:
                data = json.load(f)
            if data:
                df = pd.DataFrame(data)
                df.columns = ["Session ID", "Email", "Name", "Start", "Last Active", "Duration (min)"]
                return df
        except Exception:
            pass
    return pd.DataFrame(columns=["Session ID", "Email", "Name", "Start", "Last Active", "Duration (min)"])


# ============================================================
# CUSTOM FIELDS PARSING
# Shortcut sometimes stores product_area / priority / severity
# inside a JSON array in the custom_fields column rather than
# as top-level columns.  This fills them in when empty.
# ============================================================

def _parse_custom_fields_text(cf_raw: str) -> dict:
    """
    Parse Shortcut's custom_fields text format: 'Key=Value\nKey2=Value2'
    Returns a dict of {lowercase_key: value}.
    Falls back to JSON parsing if the string looks like JSON.
    """
    cf_raw = cf_raw.strip()
    if not cf_raw or cf_raw in ("nan", "[]", "{}"):
        return {}

    # Try JSON first
    if cf_raw.startswith("[") or cf_raw.startswith("{"):
        try:
            cf = json.loads(cf_raw)
            result = {}
            items = cf if isinstance(cf, list) else [cf]
            for item in items:
                if isinstance(item, dict):
                    k = (item.get("name") or item.get("field_name") or "").lower().strip()
                    v = str(item.get("value") or item.get("value_name") or "").strip()
                    if k and v and v != "nan":
                        result[k] = v
            return result
        except (json.JSONDecodeError, TypeError):
            pass

    # Plain text: split on newlines or semicolons, then split each on first "="
    result = {}
    for part in cf_raw.replace(";", "\n").split("\n"):
        part = part.strip()
        if "=" not in part:
            continue
        key, _, val = part.partition("=")
        key = key.strip().lower()
        val = val.strip()
        if key and val and val != "nan":
            result[key] = val
    return result


def _fill_from_custom_fields(df: pd.DataFrame) -> pd.DataFrame:
    if "custom_fields" not in df.columns:
        return df

    # Map: substring to match in the key → target df column
    # Uses "in" matching so "user priority" matches "feedback: user priority"
    keyword_to_col = [
        ("product area",   COLUMNS.get("product_area", "product_area")),
        ("product_area",   COLUMNS.get("product_area", "product_area")),
        ("priority",       COLUMNS.get("priority", "priority")),
        ("severity",       COLUMNS.get("severity", "severity")),
        ("skill set",      "skill_set"),
        ("skill_set",      "skill_set"),
        ("technical area", "technical_area"),
        ("technical_area", "technical_area"),
    ]

    for idx, row in df.iterrows():
        cf_raw = str(row.get("custom_fields", ""))
        parsed = _parse_custom_fields_text(cf_raw)
        if not parsed:
            continue

        for keyword, col in keyword_to_col:
            if col not in df.columns:
                continue
            current = str(df.at[idx, col]).strip()
            if current and current not in ("nan", "None", ""):
                continue  # already populated
            # Find a key that contains the keyword
            for k, v in parsed.items():
                if keyword in k:
                    df.at[idx, col] = v
                    break

    return df


# ============================================================
# DATA LOAD
# ============================================================

@st.cache_data(ttl=3600)
def load_feature_requests() -> pd.DataFrame:
    """Pull the main sheet and return only open feature-request rows from 2025+."""
    client = get_gsheet_client()
    if client is None:
        st.error("❌ Google Sheets not configured. Set GOOGLE_SERVICE_ACCOUNT_JSON env var.")
        return pd.DataFrame()

    try:
        sh = client.open_by_key(SHEET_ID)
        ws = sh.worksheet(MAIN_TAB)
        df = pd.DataFrame(ws.get_all_records())

        if df.empty:
            return df

        # Filter to feature requests
        type_col = COLUMNS.get("type", "")
        if FEATURE_REQUEST_TYPE and type_col and type_col in df.columns:
            df = df[
                df[type_col].astype(str).str.strip().str.lower()
                == FEATURE_REQUEST_TYPE.lower()
            ]

        # Exclude completed tickets
        if "is_completed" in df.columns:
            df = df[df["is_completed"].astype(str).str.strip().str.lower() != "true"]

        # Parse timestamp and filter to 2025+
        ts_col = COLUMNS.get("timestamp", "")
        if ts_col and ts_col in df.columns:
            df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")
            df = df[df[ts_col] >= "2025-01-01"]

        # Fill product_area / priority / severity from custom_fields if empty
        df = _fill_from_custom_fields(df)

        return df.reset_index(drop=True)

    except Exception as e:
        st.error(f"Error loading Google Sheet: {e}")
        return pd.DataFrame()


# ============================================================
# ANTHROPIC CLIENT
# ============================================================

@st.cache_resource
def get_anthropic_client():
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        try:
            api_key = st.secrets["ANTHROPIC_API_KEY"]
        except Exception:
            pass
    if not api_key:
        return None
    return anthropic.Anthropic(api_key=api_key)


# ============================================================
# CONTACT EXTRACTION
# ============================================================

def load_contacts() -> dict:
    if os.path.exists(CONTACTS_FILE):
        try:
            with open(CONTACTS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_contacts(contacts: dict):
    with _contacts_lock:
        with open(CONTACTS_FILE, "w") as f:
            json.dump(contacts, f)


def load_contacts_progress() -> dict:
    if os.path.exists(CONTACTS_PROGRESS_FILE):
        try:
            with open(CONTACTS_PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"done": 0, "total": 0, "running": False}


def _is_internal_heuristic(requester: str, description: str) -> bool:
    """Fast pre-filter: internal domain requester with no customer keywords = internal."""
    if INTERNAL_DOMAIN and INTERNAL_DOMAIN in requester.lower():
        desc_lower = description.lower()
        if not any(kw in desc_lower for kw in CUSTOMER_KEYWORDS):
            return True
    return False


def _analyze_batch(ai: anthropic.Anthropic, batch: list[dict]) -> list[dict]:
    """Send a batch of tickets to Claude and get back is_customer + contact for each."""
    ticket_lines = []
    for t in batch:
        ticket_lines.append(
            f"[{t['id']}] Title: {t['title']}\n"
            f"Requester: {t['requester']}\n"
            f"Description: {t['description'][:600]}"
        )

    prompt = f"""You are reviewing Shortcut feature request tickets. For each ticket determine:
1. Is it customer-driven? (A real customer/client/account requested it, even if filed internally on their behalf.)
2. Is there a named contact person to notify? (e.g. "Britni from Wipfli suggested this")

Tickets:

{"=" * 60}
{"=" * 60 + chr(10) + "=" * 60 + chr(10)}.join(ticket_lines)

Respond with a JSON array — one object per ticket, in the same order:
[
  {{
    "id": "<ticket id>",
    "is_customer_ticket": true or false,
    "name": "First Last or null",
    "company": "Company or null",
    "role": "role or null"
  }}
]
No other text."""

    # Build the prompt properly
    tickets_text = ("\n" + "=" * 60 + "\n").join(ticket_lines)
    prompt = f"""You are reviewing Shortcut feature request tickets. For each ticket determine:
1. Is it customer-driven? (A real customer/client/account requested it, even if filed internally on their behalf.)
2. Is there a named contact person to notify? (e.g. "Britni from Wipfli suggested this")

Tickets:

{tickets_text}

Respond with a JSON array — one object per ticket, in the same order:
[{{"id": "<id>", "is_customer_ticket": true/false, "name": "or null", "company": "or null", "role": "or null"}}]
No other text."""

    try:
        resp = ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=ANALYSIS_BATCH_SIZE * 80,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = "\n".join(text.split("\n")[1:]).rstrip("`").strip()
        return json.loads(text)
    except Exception:
        # On failure mark all as customer tickets (safe default — don't hide anything)
        return [{"id": t["id"], "is_customer_ticket": True, "name": None, "company": None, "role": None} for t in batch]


def _run_contact_extraction_thread(df: pd.DataFrame, ai: anthropic.Anthropic):
    contacts = load_contacts()
    id_col    = COLUMNS.get("id", "id")
    title_col = COLUMNS.get("title", "name")
    desc_col  = COLUMNS.get("description", "description")
    req_col   = COLUMNS.get("submitter", "requester")

    # Stage 1: instant heuristic pre-filter
    to_analyze = []
    for _, row in df.iterrows():
        ticket_id   = str(row.get(id_col, ""))
        if ticket_id in contacts:
            continue
        title       = str(row.get(title_col, ""))
        description = str(row.get(desc_col, ""))
        requester   = str(row.get(req_col, ""))

        if _is_internal_heuristic(requester, description):
            contacts[ticket_id] = {"is_customer_ticket": False, "name": None, "company": None, "role": None}
        else:
            to_analyze.append({"id": ticket_id, "title": title, "description": description, "requester": requester})

    # Save heuristic results immediately so the UI updates fast
    save_contacts(contacts)
    total = len(to_analyze)

    # Stage 2: batch AI analysis for borderline tickets
    for batch_start in range(0, total, ANALYSIS_BATCH_SIZE):
        batch = to_analyze[batch_start: batch_start + ANALYSIS_BATCH_SIZE]
        results = _analyze_batch(ai, batch)

        for r in results:
            tid = str(r.get("id", ""))
            contacts[tid] = {
                "is_customer_ticket": r.get("is_customer_ticket", True),
                "name":    r.get("name"),
                "company": r.get("company"),
                "role":    r.get("role"),
            }

        done = batch_start + len(batch)
        save_contacts(contacts)
        with open(CONTACTS_PROGRESS_FILE, "w") as f:
            json.dump({"done": done, "total": total, "running": True}, f)

    save_contacts(contacts)
    with open(CONTACTS_PROGRESS_FILE, "w") as f:
        json.dump({"done": total, "total": total, "running": False}, f)


def start_contact_extraction(df: pd.DataFrame, ai: anthropic.Anthropic):
    t = threading.Thread(
        target=_run_contact_extraction_thread,
        args=(df, ai),
        daemon=True,
    )
    t.start()


# ============================================================
# TICKET SUMMARY EXTRACTION (cached, incremental)
# ============================================================

def load_summaries() -> dict:
    if os.path.exists(SUMMARIES_FILE):
        try:
            with open(SUMMARIES_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_summaries(summaries: dict):
    with _summaries_lock:
        with open(SUMMARIES_FILE, "w") as f:
            json.dump(summaries, f)


def load_summaries_progress() -> dict:
    if os.path.exists(SUMMARIES_PROGRESS_FILE):
        try:
            with open(SUMMARIES_PROGRESS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {"done": 0, "total": 0, "running": False}


def _summarize_batch(ai: anthropic.Anthropic, batch: list) -> list:
    ticket_lines = []
    for t in batch:
        ticket_lines.append(
            f"[{t['id']}] Title: {t['title']}\n"
            f"Description: {t['description'][:600]}"
        )
    tickets_text = ("\n" + "=" * 60 + "\n").join(ticket_lines)
    prompt = f"""Summarize each feature request ticket in ONE concise sentence (max 120 chars).
Capture the core ask — what the user wants and why.

Tickets:

{tickets_text}

Respond with a JSON array — one object per ticket, in the same order:
[{{"id": "<id>", "summary": "one sentence summary"}}]
No other text."""
    try:
        resp = ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=SUMMARY_BATCH_SIZE * 60,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = "\n".join(text.split("\n")[1:]).rstrip("`").strip()
        return json.loads(text)
    except Exception:
        return [{"id": t["id"], "summary": t["title"]} for t in batch]


def _run_summary_extraction_thread(df: pd.DataFrame, ai: anthropic.Anthropic):
    summaries = load_summaries()
    id_col = COLUMNS.get("id", "id")
    title_col = COLUMNS.get("title", "name")
    desc_col = COLUMNS.get("description", "description")

    to_summarize = []
    for _, row in df.iterrows():
        ticket_id = str(row.get(id_col, ""))
        if ticket_id in summaries:
            continue
        to_summarize.append({
            "id": ticket_id,
            "title": str(row.get(title_col, "")),
            "description": str(row.get(desc_col, "")),
        })

    total = len(to_summarize)

    for batch_start in range(0, total, SUMMARY_BATCH_SIZE):
        batch = to_summarize[batch_start: batch_start + SUMMARY_BATCH_SIZE]
        results = _summarize_batch(ai, batch)
        for r in results:
            summaries[str(r.get("id", ""))] = r.get("summary", "")
        done = batch_start + len(batch)
        save_summaries(summaries)
        with open(SUMMARIES_PROGRESS_FILE, "w") as f:
            json.dump({"done": done, "total": total, "running": True}, f)

    save_summaries(summaries)
    with open(SUMMARIES_PROGRESS_FILE, "w") as f:
        json.dump({"done": total, "total": total, "running": False}, f)


def start_summary_extraction(df: pd.DataFrame, ai: anthropic.Anthropic):
    t = threading.Thread(target=_run_summary_extraction_thread, args=(df, ai), daemon=True)
    t.start()


def apply_contacts_to_df(df: pd.DataFrame, contacts: dict) -> pd.DataFrame:
    """Add a 'contact' display column built from extracted contact data."""
    id_col = COLUMNS.get("id", "id")
    df = df.copy()
    tids = df[id_col].astype(str)
    names = tids.map(lambda t: contacts.get(t, {}).get("name") or "")
    companies = tids.map(lambda t: contacts.get(t, {}).get("company") or "")
    both = (names != "") & (companies != "")
    df["contact"] = ""
    df.loc[both, "contact"] = names[both] + " (" + companies[both] + ")"
    df.loc[~both, "contact"] = names[~both].where(names[~both] != "", companies[~both])
    return df


# ============================================================
# CHATBOT HELPERS
# ============================================================

def _get(row: pd.Series, key: str, default: str = "") -> str:
    col = COLUMNS.get(key, "")
    if col and col in row.index:
        val = row.get(col, "")
        return str(val).strip() if pd.notna(val) and str(val).strip() else default
    return default


def format_tickets_for_context(df: pd.DataFrame, contacts: dict, summaries: dict | None = None) -> str:
    if df.empty:
        return "No feature request tickets available."

    id_col = COLUMNS.get("id", "id")
    sample = df if CHATBOT_MAX_TICKETS is None else df.head(CHATBOT_MAX_TICKETS)
    if summaries is None:
        summaries = {}
    lines = []

    for i, (_, row) in enumerate(sample.iterrows(), start=1):
        title       = _get(row, "title") or f"Ticket #{i}"
        ticket_id   = _get(row, "id")
        area        = _get(row, "product_area")
        priority    = _get(row, "priority")
        severity    = _get(row, "severity")
        status      = _get(row, "status")

        # Contact from extraction
        c = contacts.get(str(row.get(id_col, "")), {})
        contact_name    = c.get("name") or ""
        contact_company = c.get("company") or ""
        contact_role    = c.get("role") or ""

        # Use cached summary if available, else fall back to truncated description
        summary = summaries.get(str(row.get(id_col, "")), "")
        if not summary:
            raw_desc = _get(row, "description")
            summary = (raw_desc[:200] + "...") if len(raw_desc) > 200 else raw_desc

        id_part = f"sc-{ticket_id}" if ticket_id else f"#{i}"
        header = f"[{id_part}] {title}"
        if area:      header += f"  |  {area}"
        if priority:  header += f"  |  {priority}"
        if severity:  header += f"  |  {severity}"
        if status:    header += f"  |  {status}"

        body_lines = [header]
        if summary:
            body_lines.append(f"   Summary: {summary}")
        if contact_name:
            contact_line = f"   Contact: {contact_name}"
            if contact_company: contact_line += f" ({contact_company})"
            if contact_role:    contact_line += f" — {contact_role}"
            body_lines.append(contact_line)

        lines.append("\n".join(body_lines))

    return "\n\n".join(lines)


def build_system_prompt(df: pd.DataFrame, contacts: dict, summaries: dict | None = None) -> str:
    ticket_count = len(df) if CHATBOT_MAX_TICKETS is None else min(len(df), CHATBOT_MAX_TICKETS)
    tickets_text = format_tickets_for_context(df, contacts, summaries)

    return f"""You are a product analyst specializing in NPI (New Product Introduction) impact assessment.

You have access to {ticket_count} customer feature request ticket(s). When the user describes an NPI change — a new feature, product update, architectural change, or capability — analyze ALL tickets and identify which ones are relevant or impacted.

## Feature Request Tickets

{tickets_text}

---

## Your Job

When the user describes an NPI change, respond with ONLY a JSON object — no markdown, no explanation, no code fences. The JSON must have this exact structure:

{{"summary": "1-2 sentence summary of how much demand this NPI covers and any major unaddressed themes", "tickets": [{{"id": "<ticket id>", "relevance": "Direct|Partial|Related", "reason": "one sentence why"}}]}}

Rules:
- "Direct" = the NPI fully or substantially fulfills the request
- "Partial" = the NPI partially addresses or affects the request
- "Related" = adjacent area worth considering
- Be thorough. Include borderline tickets and err on the side of inclusion.
- Sort by relevance: Direct first, then Partial, then Related.
- Use the ticket IDs exactly as they appear in the data (e.g. "12345").
- Return ONLY the JSON object. No other text."""


# ============================================================
# UTILITY
# ============================================================

def resolve_col(key: str, df: pd.DataFrame):
    col = COLUMNS.get(key, "")
    return col if col and col in df.columns else None


# ============================================================
# AUTH GATE (must be after function defs, before main)
# ============================================================

# Restore session from cookie (reads from HTTP headers — always reliable)
if not st.session_state.get("_auth_user"):
    _cookie_val = _read_auth_cookie()
    if _cookie_val:
        _restored = _decode_auth(_cookie_val)
        if _restored:
            st.session_state["_auth_user"] = _restored

# Handle OAuth callback
_qp = st.query_params
if "code" in _qp:
    with st.spinner("Signing you in…"):
        _user, _err = _exchange_code(_qp.get("code", ""), _qp.get("state", ""))
    if _err:
        st.session_state["_auth_error"] = _err
    else:
        st.session_state["_auth_user"] = _user
        threading.Thread(target=_log_visit, args=(_user,), daemon=True).start()
    st.query_params.clear()
    st.rerun()

if not st.session_state.get("_auth_user"):
    _show_login_page()
    st.stop()

# Refresh the auth cookie on every authenticated page load (keeps it alive for new tabs)
if not st.session_state.get("_cookie_set"):
    _set_auth_cookie(st.session_state["_auth_user"])
    st.session_state["_cookie_set"] = True

# ── Activity tracking (session start + heartbeat) ────────────────────────────
if "_session_id" not in st.session_state:
    st.session_state["_session_id"] = secrets.token_hex(8)
    st.session_state["_session_start"] = datetime.now()
    st.session_state["_last_heartbeat"] = datetime.now()
    threading.Thread(
        target=lambda: st.session_state.update({"_activity_row": _log_session_start(st.session_state["_auth_user"], st.session_state["_session_id"])}),
        daemon=True,
    ).start()

# Send heartbeat on each interaction (throttled to once per 55 seconds)
_hb_now = datetime.now()
_hb_last = st.session_state.get("_last_heartbeat", datetime.min)
if (_hb_now - _hb_last).total_seconds() >= 55:
    _send_heartbeat(
        st.session_state["_session_id"],
        st.session_state["_session_start"],
        st.session_state.get("_activity_row"),
    )
    st.session_state["_last_heartbeat"] = _hb_now


# ============================================================
# MAIN APP
# ============================================================

def main():
    # ── Handle menu actions via query params ──────────────────
    _qp = st.query_params
    if _qp.get("_action") == "refresh":
        st.query_params.clear()
        st.cache_data.clear()
        st.rerun()
    elif _qp.get("_action") == "signout":
        st.query_params.clear()
        if "_auth_user" in st.session_state:
            del st.session_state["_auth_user"]
        _clear_auth_cookie()
        st.rerun()

    # ── Inject custom items into the three-dot menu via JS ────
    _auth_user = st.session_state.get("_auth_user", {})
    _user_email = _auth_user.get("email", "")
    st_components.html(f"""
    <script>
    (function() {{
        var doc = window.parent.document;
        var ITEMS = [
            {{label: '{_user_email}', action: null, style: 'color:#9E9E9E;font-size:0.8rem;padding:0.4rem 1rem;cursor:default;'}},
            {{label: '🔄 Refresh Data', action: 'refresh', style: 'padding:0.4rem 1rem;cursor:pointer;color:#E0E0E0;font-size:0.875rem;'}},
            {{label: '🚪 Sign Out', action: 'signout', style: 'padding:0.4rem 1rem;cursor:pointer;color:#E0E0E0;font-size:0.875rem;'}},
        ];

        function inject(menuList) {{
            if (menuList.querySelector('.fr-custom-item')) return;
            var sep = doc.createElement('hr');
            sep.style.cssText = 'border:none;border-top:1px solid #444C56;margin:4px 0;';
            sep.className = 'fr-custom-item';
            menuList.prepend(sep);
            ITEMS.slice().reverse().forEach(function(item) {{
                var li = doc.createElement('li');
                li.className = 'fr-custom-item';
                li.setAttribute('role', 'option');
                li.style.cssText = item.style + 'list-style:none;';
                li.textContent = item.label;
                if (item.action) {{
                    li.onmouseenter = function() {{ this.style.backgroundColor='#444C56'; }};
                    li.onmouseleave = function() {{ this.style.backgroundColor=''; }};
                    li.onclick = function() {{ window.parent.location.search = '?_action=' + item.action; }};
                }}
                menuList.prepend(li);
            }});
        }}

        new MutationObserver(function(mutations) {{
            for (var i = 0; i < mutations.length; i++) {{
                for (var j = 0; j < mutations[i].addedNodes.length; j++) {{
                    var node = mutations[i].addedNodes[j];
                    if (node.nodeType === 1 && node.querySelector) {{
                        var ul = node.querySelector('ul[role="listbox"]');
                        if (ul) inject(ul);
                    }}
                }}
            }}
        }}).observe(doc.body, {{childList: true, subtree: true}});
    }})();
    </script>
    """, height=0)

    # ── Header: logo + title ─────────────────────────────────
    app_dir = os.path.dirname(os.path.abspath(__file__))
    logo_path = os.path.join(app_dir, "logo.svg")
    if os.path.exists(logo_path):
        with open(logo_path, "r") as f:
            logo_svg = f.read()
        logo_b64 = base64.b64encode(logo_svg.encode()).decode()
        st.markdown(f"""
        <div class="header-container">
            <img src="data:image/svg+xml;base64,{logo_b64}" />
            <h1>Feature Request Dashboard</h1>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown('<h1 style="color:#00E676;">Feature Request Dashboard</h1>', unsafe_allow_html=True)
    st.markdown('<div class="header-subtitle">Customer feature requests from Shortcut · NPI impact analysis</div>', unsafe_allow_html=True)

    # ── Load data ────────────────────────────────────────────
    with st.spinner("Loading feature requests…"):
        df = load_feature_requests()

    # ── Load contacts + auto-start extraction if needed ─────
    contacts = load_contacts()
    progress = load_contacts_progress()
    ai = get_anthropic_client()

    # Count unanalyzed tickets (fast set operation)
    id_col_name = COLUMNS.get("id", "id")
    _unanalyzed_count = 0
    if not df.empty:
        all_ids = set(df[id_col_name].astype(str))
        _unanalyzed_count = len(all_ids - set(contacts.keys()))

    # Summary extraction (cached, incremental)
    summaries = load_summaries()
    sum_progress = load_summaries_progress()

    if not df.empty and ai:
        unsummarized_ids = all_ids - set(summaries.keys())
        if unsummarized_ids and not sum_progress.get("running"):
            start_summary_extraction(df, ai)
            sum_progress = {"running": True, "done": 0, "total": len(unsummarized_ids)}

    any_running = progress.get("running") or sum_progress.get("running")
    if any_running:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=4000, key="analysis_refresh")

    if progress.get("running"):
        done  = progress.get("done", 0)
        total = progress.get("total", 1)
        st.markdown(f"""
        <div class="progress-banner">
            <span class="progress-text">Analyzing tickets… {done}/{total} processed — filtering to customer requests</span>
        </div>
        """, unsafe_allow_html=True)
        st.progress(done / total if total > 0 else 0)
    elif _unanalyzed_count > 0 and ai:
        if st.button(f"Analyze {_unanalyzed_count} new tickets", type="primary"):
            start_contact_extraction(df, ai)

    if sum_progress.get("running"):
        done  = sum_progress.get("done", 0)
        total = sum_progress.get("total", 1)
        st.markdown(f"""
        <div class="progress-banner">
            <span class="progress-text">Summarizing tickets… {done}/{total} processed — building chatbot context</span>
        </div>
        """, unsafe_allow_html=True)
        st.progress(done / total if total > 0 else 0)

    # Merge contacts into df and filter to customer tickets
    if not df.empty:
        df = apply_contacts_to_df(df, contacts)
        id_col_name = COLUMNS.get("id", "id")
        # Only exclude tickets that have been analyzed AND flagged as non-customer
        if contacts:
            def _is_not_customer(row):
                tid = str(row.get(id_col_name, ""))
                c = contacts.get(tid)
                if c is None:
                    return False  # not yet analyzed — keep it
                return c.get("is_customer_ticket") is False
            df = df[~df.apply(_is_not_customer, axis=1)]

    # ── Tabs ─────────────────────────────────────────────────
    _admin_mode = _is_admin()
    if _admin_mode:
        tab1, tab3, tab_admin = st.tabs(
            ["📋 Feature Requests", "📊 Google Sheet", "Admin"]
        )
    else:
        tab1, tab3 = st.tabs(["📋 Feature Requests", "📊 Google Sheet"])
        tab_admin = None

    # ════════════════════════════════════════════════════════
    # TAB 1 — FEATURE REQUESTS DASHBOARD
    # ════════════════════════════════════════════════════════
    with tab1:
        if df.empty:
            st.warning("No feature request tickets found. Check your sheet ID, tab name, and column configuration.")
            st.stop()

        st.markdown(f'<div style="color:#9E9E9E;font-size:0.9rem;margin-bottom:12px;">Showing {len(df):,} open feature requests from 2025</div>', unsafe_allow_html=True)

        def metric_card(label, value, sub=None):
            sub_html = f'<div style="color:#9E9E9E;font-size:0.78rem;margin-top:4px;">{sub}</div>' if sub else ""
            return f"""
            <div style="background-color:#373E47;border:1px solid #444C56;border-radius:10px;
                        padding:16px 20px;min-height:100px;display:flex;flex-direction:column;justify-content:space-between;">
                <div style="color:#9E9E9E;font-size:0.85rem;font-weight:700;">{label}</div>
                <div style="color:#E0E0E0;font-size:2rem;font-weight:700;line-height:1.1;">{value}</div>
                {sub_html}
            </div>"""

        priority_col = resolve_col("priority", df)
        status_col   = resolve_col("status", df)
        area_col     = resolve_col("product_area", df)

        high = df[priority_col].astype(str).str.lower().isin(["high", "critical"]).sum() if priority_col else "—"
        open_count = (~df[status_col].astype(str).str.lower().isin(
            ["completed", "done", "cancelled", "canceled", "archived"]
        )).sum() if status_col else "—"
        area_count = df[area_col].nunique() if area_col else "—"
        contacts_found = sum(1 for v in contacts.values() if v.get("name"))

        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.markdown(metric_card("Total Requests", f"{len(df):,}"), unsafe_allow_html=True)
        with m2:
            pct = f"{int(high)/len(df)*100:.0f}% of total" if isinstance(high, (int, float)) and len(df) > 0 else ""
            st.markdown(metric_card("High Priority", high, sub=pct), unsafe_allow_html=True)
        with m3:
            st.markdown(metric_card("Open", open_count), unsafe_allow_html=True)
        with m4:
            st.markdown(metric_card("Product Areas", area_count, sub=f"{contacts_found} contacts extracted"), unsafe_allow_html=True)

        st.markdown("---")

        # ── Contact extraction status ─────────────────────────
        contacts_extracted = sum(1 for v in contacts.values() if v.get("name"))
        analyzed = len(contacts)
        if analyzed < len(df):
            remaining = len(df) - analyzed
            st.caption(f"Analysis running — {analyzed}/{len(df)} tickets reviewed · {contacts_extracted} contacts found · {remaining} remaining")
        else:
            st.caption(f"Analysis complete — {analyzed} tickets reviewed · {contacts_extracted} contacts found")

        st.markdown("---")

        # ── NPI Impact Analysis ──────────────────────────────
        npi_col1, npi_col2 = st.columns([5, 1])
        with npi_col1:
            npi_input = st.text_input(
                "🔬 NPI Impact Analysis",
                placeholder="Describe an NPI change to find relevant tickets — e.g. 'We're adding bulk PDF export to the reporting module'",
                label_visibility="collapsed",
            )
        with npi_col2:
            if st.session_state.get("npi_results"):
                if st.button("✕ Clear NPI", use_container_width=True):
                    st.session_state.pop("npi_results", None)
                    st.session_state.pop("npi_summary", None)
                    st.rerun()

        # Run NPI analysis when input changes
        if npi_input and npi_input != st.session_state.get("npi_last_query"):
            ai = get_anthropic_client()
            if ai is None:
                st.error("ANTHROPIC_API_KEY is not configured.")
            else:
                with st.spinner("Analyzing NPI impact across all tickets…"):
                    system_prompt = build_system_prompt(df, contacts, summaries)
                    try:
                        resp = ai.messages.create(
                            model="claude-sonnet-4-20250514",
                            max_tokens=8192,
                            thinking={"type": "enabled", "budget_tokens": 4096},
                            system=system_prompt,
                            messages=[{"role": "user", "content": npi_input}],
                        )
                        raw_text = ""
                        for block in resp.content:
                            if block.type == "text":
                                raw_text = block.text.strip()
                        # Strip code fences if present
                        if raw_text.startswith("```"):
                            raw_text = "\n".join(raw_text.split("\n")[1:]).rstrip("`").strip()
                        result = json.loads(raw_text)
                        st.session_state["npi_results"] = result.get("tickets", [])
                        st.session_state["npi_summary"] = result.get("summary", "")
                        st.session_state["npi_last_query"] = npi_input
                        st.rerun()
                    except (json.JSONDecodeError, Exception) as e:
                        st.error(f"Failed to parse NPI analysis: {e}")

        # ── Filters ──────────────────────────────────────────
        with st.expander("🔍 Search & Filter", expanded=False):
            fc1, fc2, fc3, fc4 = st.columns(4)

            with fc1:
                search = st.text_input("Keyword", placeholder="Search title / description…")
            with fc2:
                if area_col:
                    areas = ["All"] + sorted(df[area_col].dropna().astype(str).unique().tolist())
                    area_filter = st.selectbox("Product Area", areas)
                else:
                    area_filter = "All"
            with fc3:
                if priority_col:
                    pris = ["All"] + sorted(df[priority_col].dropna().astype(str).unique().tolist())
                    priority_filter = st.selectbox("Priority", pris)
                else:
                    priority_filter = "All"
            with fc4:
                if status_col:
                    statuses = ["All"] + sorted(df[status_col].dropna().astype(str).unique().tolist())
                    status_filter = st.selectbox("Status", statuses)
                else:
                    status_filter = "All"

        # Apply filters
        fdf = df.copy()
        title_col = resolve_col("title", fdf)
        desc_col  = resolve_col("description", fdf)

        if search:
            mask = pd.Series([False] * len(fdf), index=fdf.index)
            for col in [title_col, desc_col]:
                if col:
                    mask |= fdf[col].astype(str).str.contains(search, case=False, na=False)
            fdf = fdf[mask]

        if area_filter != "All" and area_col:
            fdf = fdf[fdf[area_col].astype(str) == area_filter]
        if priority_filter != "All" and priority_col:
            fdf = fdf[fdf[priority_col].astype(str) == priority_filter]
        if status_filter != "All" and status_col:
            fdf = fdf[fdf[status_col].astype(str) == status_filter]

        # Apply NPI filter if active
        npi_results = st.session_state.get("npi_results")
        npi_active = npi_results is not None and len(npi_results) > 0
        id_col_name = COLUMNS.get("id", "id")

        if npi_active:
            npi_summary = st.session_state.get("npi_summary", "")
            if npi_summary:
                st.info(f"**NPI Summary:** {npi_summary}")

            # Build lookup from NPI results
            relevance_map = {}
            reason_map = {}
            relevance_order = {"Direct": 0, "Partial": 1, "Related": 2}
            for r in npi_results:
                tid = str(r.get("id", ""))
                relevance_map[tid] = r.get("relevance", "Related")
                reason_map[tid] = r.get("reason", "")

            # Filter to only NPI-matched tickets
            npi_ids = set(relevance_map.keys())
            fdf = fdf[fdf[id_col_name].astype(str).isin(npi_ids)]

            # Add relevance columns
            fdf = fdf.copy()
            fdf["Relevance"] = fdf[id_col_name].astype(str).map(relevance_map).fillna("Related")
            fdf["Reason"] = fdf[id_col_name].astype(str).map(reason_map).fillna("")
            fdf["_rel_order"] = fdf["Relevance"].map(relevance_order).fillna(2)
            fdf = fdf.sort_values("_rel_order")

            # Show counts by relevance
            direct_n = (fdf["Relevance"] == "Direct").sum()
            partial_n = (fdf["Relevance"] == "Partial").sum()
            related_n = (fdf["Relevance"] == "Related").sum()
            st.caption(f"NPI matched {len(fdf):,} tickets — {direct_n} Direct · {partial_n} Partial · {related_n} Related")

            # Export contacts button
            contact_rows = []
            for _, row in fdf.iterrows():
                tid = str(row.get(id_col_name, ""))
                c = contacts.get(tid, {})
                name = c.get("name") or ""
                company = c.get("company") or ""
                role = c.get("role") or ""
                if name:
                    contact_rows.append({
                        "Name": name,
                        "Company": company,
                        "Role": role,
                        "Ticket": _get(row, "title"),
                        "Relevance": relevance_map.get(tid, ""),
                    })
            if contact_rows:
                contact_df = pd.DataFrame(contact_rows)
                buf = io.StringIO()
                contact_df.to_csv(buf, index=False)
                st.download_button(
                    f"📧 Export {len(contact_rows)} Contact(s)",
                    buf.getvalue(),
                    file_name=f"npi_contacts_{datetime.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                )
        else:
            if len(fdf) != len(df):
                st.caption(f"Showing {len(fdf):,} of {len(df):,} tickets")

        # ── Table ─────────────────────────────────────────────
        all_display_keys = DISPLAY_KEYS
        display_cols = []

        # If NPI is active, prepend Relevance and Reason columns
        if npi_active:
            display_cols = ["Relevance", "Reason"]

        for k in all_display_keys:
            if k == "contact":
                if "contact" in fdf.columns:
                    display_cols.append("contact")
            else:
                col = COLUMNS.get(k)
                if col and col in fdf.columns:
                    display_cols.append(col)

        if not display_cols:
            display_cols = list(fdf.columns[:6])

        # Remove internal sort column from display
        show_df = fdf[[c for c in display_cols if c in fdf.columns]]

        col_config = {}
        link_col = COLUMNS.get("link", "")
        if link_col and link_col in show_df.columns:
            col_config[link_col] = st.column_config.LinkColumn(
                "Link",
                display_text="Shortcut Link",
            )

        st.dataframe(show_df, use_container_width=True, height=380, column_config=col_config)

        # ── Detail view ───────────────────────────────────────
        st.markdown("---")
        st.subheader("🔎 Ticket Detail")

        if title_col and not fdf.empty:
            options = ["— select a ticket —"] + fdf[title_col].astype(str).tolist()
            selected = st.selectbox("Select ticket", options)

            if selected != "— select a ticket —":
                row = fdf[fdf[title_col].astype(str) == selected].iloc[0]

                dcol1, dcol2 = st.columns([3, 1])
                with dcol1:
                    if desc_col:
                        st.markdown("**Description**")
                        st.markdown(str(row.get(desc_col, "—")))

                with dcol2:
                    # Contact person (extracted)
                    contact_val = str(row.get("contact", "")).strip()
                    if contact_val:
                        st.markdown(f"**Contact:** {contact_val}")
                    else:
                        id_col_name = COLUMNS.get("id", "id")
                        tid = str(row.get(id_col_name, ""))
                        if tid in contacts:
                            st.markdown("**Contact:** _(none found)_")
                        else:
                            st.markdown("**Contact:** _(not yet extracted)_")

                    for key in ["submitter", "product_area", "priority", "severity", "status", "labels", "epic", "team", "timestamp"]:
                        col_name = COLUMNS.get(key, "")
                        if col_name and col_name in row.index and pd.notna(row.get(col_name)):
                            val = row.get(col_name)
                            if hasattr(val, "strftime"):
                                val = val.strftime("%Y-%m-%d")
                            label = key.replace("_", " ").title()
                            st.markdown(f"**{label}:** {val}")

        # ── Charts ────────────────────────────────────────────
        st.markdown("---")
        cc1, cc2 = st.columns(2)

        with cc1:
            if area_col and area_col in df.columns:
                st.subheader("By Product Area")
                area_counts = df[area_col].astype(str).value_counts().reset_index()
                area_counts.columns = ["Product Area", "Count"]
                st.bar_chart(area_counts.set_index("Product Area"))

        with cc2:
            ts_col = resolve_col("timestamp", df)
            if ts_col:
                st.subheader("Submissions Over Time")
                df_t = df[[ts_col]].dropna().copy()
                df_t["month"] = df_t[ts_col].dt.to_period("M").astype(str)
                monthly = df_t["month"].value_counts().sort_index().reset_index()
                monthly.columns = ["Month", "Count"]
                st.bar_chart(monthly.set_index("Month"))

        # ── Downloads ─────────────────────────────────────────
        st.markdown("---")
        dl1, dl2 = st.columns(2)

        with dl1:
            buf = io.StringIO()
            fdf.to_csv(buf, index=False)
            st.download_button(
                "⬇️ Download Filtered CSV",
                buf.getvalue(),
                file_name=f"fr_filtered_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with dl2:
            buf2 = io.StringIO()
            df.to_csv(buf2, index=False)
            st.download_button(
                "⬇️ Download All Feature Requests",
                buf2.getvalue(),
                file_name=f"fr_all_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True,
            )

    # ════════════════════════════════════════════════════════
    # TAB 3 — GOOGLE SHEET EMBED
    # ════════════════════════════════════════════════════════
    with tab3:
        st.subheader("📊 Source Google Sheet")
        if SHEET_ID == "YOUR_SHEET_ID_HERE":
            st.info("Set your GOOGLE_SHEET_ID environment variable to embed the sheet here.")
        else:
            st.markdown(
                f'<iframe src="https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit'
                f'?usp=sharing&rm=minimal" width="100%" height="720" frameborder="0"></iframe>',
                unsafe_allow_html=True,
            )

    # ════════════════════════════════════════════════════════
    # TAB ADMIN (admin users only)
    # ════════════════════════════════════════════════════════
    if _admin_mode and tab_admin is not None:
        with tab_admin:
            st.subheader("Access Log")
            st.markdown("Every time a user authenticates, their login is recorded here.")

            log_df = _load_access_log()

            if log_df.empty:
                st.info("No visits recorded yet. Logs are written when users sign in.")
            else:
                log_df.columns = [c.capitalize() for c in log_df.columns]
                if "Timestamp" in log_df.columns:
                    log_df["Timestamp"] = pd.to_datetime(log_df["Timestamp"], errors="coerce")
                    log_df = log_df.sort_values("Timestamp", ascending=False).reset_index(drop=True)
                    log_df["Timestamp"] = log_df["Timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

                total_visits = len(log_df)
                unique_users = log_df["Email"].nunique() if "Email" in log_df.columns else 0
                m1, m2 = st.columns(2)
                m1.metric("Total logins", total_visits)
                m2.metric("Unique users", unique_users)

                st.divider()

                if "Email" in log_df.columns:
                    st.markdown("**Logins per user**")
                    counts = (
                        log_df.groupby("Email")
                        .agg(Logins=("Email", "count"), Last_seen=("Timestamp", "max"))
                        .reset_index()
                        .rename(columns={"Last_seen": "Last seen"})
                        .sort_values("Logins", ascending=False)
                    )
                    st.dataframe(counts, use_container_width=True, hide_index=True)
                    st.divider()

                st.markdown("**Full login history**")
                st.dataframe(log_df, use_container_width=True, hide_index=True)

            # ── Activity Tracking ────────────────────────────────────────
            st.divider()
            st.subheader("Activity Tracking")
            st.markdown("Session-level tracking: who visited, when, and for how long.")

            activity_df = _load_activity_log()
            if activity_df.empty:
                st.info("No activity recorded yet. Activity is tracked automatically for authenticated users.")
            else:
                # Normalize columns
                col_remap = {}
                for c in activity_df.columns:
                    cl = c.lower().replace(" ", "_").replace("(", "").replace(")", "")
                    if "session" in cl:
                        col_remap[c] = "Session ID"
                    elif "email" in cl:
                        col_remap[c] = "Email"
                    elif "name" in cl and "session" not in cl:
                        col_remap[c] = "Name"
                    elif "start" in cl:
                        col_remap[c] = "Start"
                    elif "last" in cl:
                        col_remap[c] = "Last Active"
                    elif "duration" in cl:
                        col_remap[c] = "Duration (min)"
                activity_df = activity_df.rename(columns=col_remap)

                if "Last Active" in activity_df.columns:
                    activity_df["Last Active"] = pd.to_datetime(activity_df["Last Active"], errors="coerce")
                if "Start" in activity_df.columns:
                    activity_df["Start"] = pd.to_datetime(activity_df["Start"], errors="coerce")
                if "Duration (min)" in activity_df.columns:
                    activity_df["Duration (min)"] = pd.to_numeric(activity_df["Duration (min)"], errors="coerce")

                # Currently active users (heartbeat within last 2 minutes)
                if "Last Active" in activity_df.columns:
                    now = datetime.now()
                    active = activity_df[activity_df["Last Active"] >= now - timedelta(minutes=2)]
                    st.markdown("**Currently active**")
                    if active.empty:
                        st.caption("No users currently active.")
                    else:
                        for _, row in active.iterrows():
                            label = row.get("Name") or row.get("Email", "Unknown")
                            dur = row.get("Duration (min)", 0)
                            st.markdown(f"🟢 **{label}** — active for {dur:.0f} min")

                st.divider()

                # Summary metrics
                total_sessions = len(activity_df)
                unique_visitors = activity_df["Email"].nunique() if "Email" in activity_df.columns else 0
                avg_duration = activity_df["Duration (min)"].mean() if "Duration (min)" in activity_df.columns else 0
                m1, m2, m3 = st.columns(3)
                m1.metric("Total sessions", total_sessions)
                m2.metric("Unique visitors", unique_visitors)
                m3.metric("Avg duration (min)", f"{avg_duration:.1f}")

                st.divider()

                # Per-user usage breakdown
                if "Email" in activity_df.columns and "Duration (min)" in activity_df.columns:
                    st.markdown("**Usage per user**")
                    la_col = "Last Active" if "Last Active" in activity_df.columns else "Start"
                    user_stats = (
                        activity_df.groupby("Email")
                        .agg(
                            Sessions=("Email", "count"),
                            Total_min=("Duration (min)", "sum"),
                            Avg_min=("Duration (min)", "mean"),
                            Last_visit=(la_col, "max"),
                        )
                        .reset_index()
                        .rename(columns={"Total_min": "Total (min)", "Avg_min": "Avg (min)", "Last_visit": "Last visit"})
                        .sort_values("Total (min)", ascending=False)
                    )
                    user_stats["Total (min)"] = user_stats["Total (min)"].round(1)
                    user_stats["Avg (min)"] = user_stats["Avg (min)"].round(1)
                    if "Last visit" in user_stats.columns:
                        user_stats["Last visit"] = user_stats["Last visit"].dt.strftime("%Y-%m-%d %H:%M:%S")
                    st.dataframe(user_stats, use_container_width=True, hide_index=True)
                    st.divider()

                # Full session history
                st.markdown("**Full session history**")
                display_df = activity_df.sort_values("Start", ascending=False).reset_index(drop=True) if "Start" in activity_df.columns else activity_df
                fmt_df = display_df.copy()
                for col in ["Start", "Last Active"]:
                    if col in fmt_df.columns:
                        fmt_df[col] = fmt_df[col].dt.strftime("%Y-%m-%d %H:%M:%S")
                if "Duration (min)" in fmt_df.columns:
                    fmt_df["Duration (min)"] = fmt_df["Duration (min)"].round(1)
                st.dataframe(fmt_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
