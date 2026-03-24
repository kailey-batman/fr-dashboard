import streamlit as st
import pandas as pd
import anthropic
import gspread
from google.oauth2.service_account import Credentials
import json
import os
import base64
import threading
from datetime import datetime
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

    .header-container {
        display: flex; align-items: center; gap: 16px; padding: 0.5rem 0 0.5rem 0;
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
</style>
""", unsafe_allow_html=True)

# ============================================================
# CONFIGURATION — Update these to match your Google Sheet
# ============================================================

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
}

# Exact value in the "type" column for feature requests.
FEATURE_REQUEST_TYPE = "feature"

# Columns shown in the main data table (logical key names from COLUMNS dict above)
DISPLAY_KEYS = ["id", "timestamp", "title", "submitter", "contact", "product_area", "priority", "severity", "status", "labels", "epic"]

# Max tickets sent to the chatbot as context
CHATBOT_MAX_TICKETS = 500

# File used to persist extracted contacts across redeploys
CONTACTS_FILE = "contacts.json"
CONTACTS_PROGRESS_FILE = "contacts_progress.json"

_contacts_lock = threading.Lock()

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
# CUSTOM FIELDS PARSING
# Shortcut sometimes stores product_area / priority / severity
# inside a JSON array in the custom_fields column rather than
# as top-level columns.  This fills them in when empty.
# ============================================================

def _fill_from_custom_fields(df: pd.DataFrame) -> pd.DataFrame:
    if "custom_fields" not in df.columns:
        return df

    # Match by lowercase — handles "Product Area", "product area", "product_area", etc.
    target_cols = {
        "product area":  COLUMNS.get("product_area", "product_area"),
        "product_area":  COLUMNS.get("product_area", "product_area"),
        "priority":      COLUMNS.get("priority", "priority"),
        "severity":      COLUMNS.get("severity", "severity"),
        "skill set":     "skill_set",
        "skill_set":     "skill_set",
        "technical area": "technical_area",
        "technical_area": "technical_area",
    }

    for idx, row in df.iterrows():
        cf_raw = str(row.get("custom_fields", "")).strip()
        if not cf_raw or cf_raw in ("nan", "[]", "{}"):
            continue
        try:
            cf = json.loads(cf_raw)
            if not isinstance(cf, list):
                continue
            for item in cf:
                if not isinstance(item, dict):
                    continue
                # Support both {"name":..., "value":...} and {"field_name":..., "value":...}
                field_name = (item.get("name") or item.get("field_name") or "").lower().strip()
                value = str(item.get("value") or item.get("value_name") or "").strip()
                if field_name in target_cols and value and value != "nan":
                    col = target_cols[field_name]
                    if col in df.columns:
                        current = str(df.at[idx, col]).strip()
                        if not current or current in ("nan", "None", ""):
                            df.at[idx, col] = value
        except (json.JSONDecodeError, TypeError):
            continue

    return df


# ============================================================
# DATA LOAD
# ============================================================

@st.cache_data(ttl=300)
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


def _extract_contact_api(ai: anthropic.Anthropic, title: str, description: str, requester: str) -> dict:
    prompt = f"""You are reading a Shortcut ticket. Answer two things:

1. Is this ticket customer-submitted or customer-driven? A customer ticket mentions a specific customer, client, company, or end-user requesting the feature — even if an internal engineer filed it on their behalf. An internal ticket is purely an engineering initiative with no customer mention.

2. Who is the named contact person (if any) — someone who should be notified about updates? Look for explicit name mentions like "Britni from Wipfli suggested this." The contact may differ from the requester field.

Ticket title: {title}
Requester (system field): {requester}
Description:
{description[:1500]}

Respond with JSON only — no other text:
{{
  "is_customer_ticket": true or false,
  "name": "First Last or null",
  "company": "Company name or null",
  "role": "their role or null"
}}"""

    try:
        resp = ai.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        text = resp.content[0].text.strip()
        if text.startswith("```"):
            text = "\n".join(text.split("\n")[1:])
            text = text.rstrip("`").strip()
        return json.loads(text)
    except Exception:
        return {"is_customer_ticket": None, "name": None, "company": None, "role": None}


def _run_contact_extraction_thread(df: pd.DataFrame, ai: anthropic.Anthropic):
    contacts = load_contacts()
    id_col    = COLUMNS.get("id", "id")
    title_col = COLUMNS.get("title", "name")
    desc_col  = COLUMNS.get("description", "description")
    req_col   = COLUMNS.get("submitter", "requester")
    total = len(df)

    for i, (_, row) in enumerate(df.iterrows()):
        ticket_id = str(row.get(id_col, f"row_{i}"))
        if ticket_id in contacts:
            continue

        title       = str(row.get(title_col, ""))
        description = str(row.get(desc_col, ""))
        requester   = str(row.get(req_col, ""))

        contacts[ticket_id] = _extract_contact_api(ai, title, description, requester)

        if i % 5 == 0 or i == total - 1:
            save_contacts(contacts)
            with open(CONTACTS_PROGRESS_FILE, "w") as f:
                json.dump({"done": i + 1, "total": total, "running": True}, f)

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


def apply_contacts_to_df(df: pd.DataFrame, contacts: dict) -> pd.DataFrame:
    """Add a 'contact' display column built from extracted contact data."""
    id_col = COLUMNS.get("id", "id")

    def _contact_str(row):
        tid = str(row.get(id_col, ""))
        c = contacts.get(tid, {})
        name = c.get("name") or ""
        company = c.get("company") or ""
        if name and company:
            return f"{name} ({company})"
        return name or company or ""

    df = df.copy()
    df["contact"] = df.apply(_contact_str, axis=1)
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


def format_tickets_for_context(df: pd.DataFrame, contacts: dict) -> str:
    if df.empty:
        return "No feature request tickets available."

    id_col = COLUMNS.get("id", "id")
    sample = df.head(CHATBOT_MAX_TICKETS)
    lines = []

    for i, (_, row) in enumerate(sample.iterrows(), start=1):
        title       = _get(row, "title") or f"Ticket #{i}"
        ticket_id   = _get(row, "id")
        area        = _get(row, "product_area")
        priority    = _get(row, "priority")
        severity    = _get(row, "severity")
        submitter   = _get(row, "submitter")
        owners      = _get(row, "owners")
        ts          = _get(row, "timestamp")
        description = _get(row, "description")
        labels      = _get(row, "labels")
        epic        = _get(row, "epic")
        team        = _get(row, "team")
        status      = _get(row, "status")

        # Contact from extraction
        c = contacts.get(str(row.get(id_col, "")), {})
        contact_name    = c.get("name") or ""
        contact_company = c.get("company") or ""
        contact_role    = c.get("role") or ""

        id_part = f"sc-{ticket_id}" if ticket_id else f"#{i}"
        header = f"[{id_part}] {title}"
        if area:      header += f"  |  Area: {area}"
        if priority:  header += f"  |  Priority: {priority}"
        if severity:  header += f"  |  Severity: {severity}"
        if status:    header += f"  |  State: {status}"
        if submitter: header += f"  |  Requester: {submitter}"
        if ts:        header += f"  |  Created: {str(ts)[:10]}"

        body_lines = [header]
        if description:
            body_lines.append(f"   Description: {description[:400]}{'...' if len(description) > 400 else ''}")
        if contact_name:
            contact_line = f"   Contact: {contact_name}"
            if contact_company: contact_line += f" ({contact_company})"
            if contact_role:    contact_line += f" — {contact_role}"
            body_lines.append(contact_line)
        if labels: body_lines.append(f"   Labels: {labels}")
        if epic:   body_lines.append(f"   Epic: {epic}")
        if team:   body_lines.append(f"   Team: {team}")
        if owners: body_lines.append(f"   Owners: {owners}")

        lines.append("\n".join(body_lines))

    total = len(df)
    suffix = ""
    if total > CHATBOT_MAX_TICKETS:
        suffix = f"\n\n[Note: Showing {CHATBOT_MAX_TICKETS} of {total} total tickets due to context limits.]"

    return "\n\n".join(lines) + suffix


def build_system_prompt(df: pd.DataFrame, contacts: dict) -> str:
    ticket_count = min(len(df), CHATBOT_MAX_TICKETS)
    tickets_text = format_tickets_for_context(df, contacts)

    return f"""You are a product analyst specializing in NPI (New Product Introduction) impact assessment.

You have access to {ticket_count} customer feature request ticket(s). When the user describes an NPI change — a new feature, product update, architectural change, or capability — analyze ALL tickets and identify which ones are relevant or impacted.

## Feature Request Tickets

{tickets_text}

---

## Your Job

When the user describes an NPI change, respond with:

1. **Brief restatement** of the NPI change as you understand it (one sentence).

2. **Directly Addressed (N tickets):** — tickets the NPI change fully or substantially fulfills.
   - #[number] **[Title]** — [one sentence explaining why this NPI addresses it] | Contact: [name if known]

3. **Potentially Impacted (N tickets):** — tickets in the same area that may be partially addressed, affected, or made obsolete.
   - #[number] **[Title]** — [one sentence on the impact] | Contact: [name if known]

4. **Related Context (N tickets):** — tickets in adjacent areas worth considering together.
   - #[number] **[Title]** — [one sentence on the connection]

5. **Summary** — 2–3 sentences: how much existing customer demand does this NPI cover? Are there major unaddressed themes?

**Be thorough.** Include borderline tickets and note your uncertainty. Err on the side of inclusion.

For follow-up questions, answer conversationally using the ticket data above."""


# ============================================================
# UTILITY
# ============================================================

def resolve_col(key: str, df: pd.DataFrame):
    col = COLUMNS.get(key, "")
    return col if col and col in df.columns else None


# ============================================================
# MAIN APP
# ============================================================

def main():
    # ── Header ───────────────────────────────────────────────
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

    # ── Sidebar ─────────────────────────────────────────────
    with st.sidebar:
        if os.path.exists(logo_path):
            st.image(logo_path, width=60)
        st.markdown("---")
        st.markdown("### Data")
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.markdown("---")
        st.caption(f"Last loaded: {datetime.now().strftime('%H:%M:%S')}")

        # Debug: show raw custom_fields sample
        with st.expander("🔧 Debug", expanded=False):
            if not df.empty and "custom_fields" in df.columns:
                sample = df["custom_fields"].dropna().astype(str)
                sample = sample[sample.str.strip().str.len() > 5]
                if not sample.empty:
                    st.text("First non-empty custom_fields value:")
                    st.code(sample.iloc[0][:500])
                else:
                    st.text("custom_fields column is empty for all rows.")

    # ── Load data ────────────────────────────────────────────
    with st.spinner("Loading feature requests…"):
        df = load_feature_requests()

    # ── Load contacts + auto-refresh if extraction running ───
    contacts = load_contacts()
    progress = load_contacts_progress()
    if progress.get("running"):
        st.markdown(
            '<meta http-equiv="refresh" content="4">',
            unsafe_allow_html=True,
        )
        done  = progress.get("done", 0)
        total = progress.get("total", 1)
        st.info(f"Extracting contacts… {done}/{total} tickets processed. Page auto-refreshes.")

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
    tab1, tab2, tab3 = st.tabs(["📋 Feature Requests", "💬 NPI Chatbot", "📊 Google Sheet"])

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

        # ── Contact extraction controls ───────────────────────
        contacts_extracted = sum(1 for v in contacts.values() if v.get("name"))
        cex1, cex2 = st.columns([4, 1])
        with cex1:
            if progress.get("running"):
                st.caption(f"Extracting contacts: {progress.get('done', 0)}/{progress.get('total', 0)}")
            else:
                st.caption(f"Contact extraction: {contacts_extracted} contacts found across {len(contacts)} tickets analyzed.")
        with cex2:
            ai = get_anthropic_client()
            if not progress.get("running") and ai:
                if st.button("Extract Contacts", use_container_width=True):
                    start_contact_extraction(df, ai)
                    st.rerun()

        st.markdown("---")

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

        if len(fdf) != len(df):
            st.caption(f"Showing {len(fdf):,} of {len(df):,} tickets")

        # ── Table ─────────────────────────────────────────────
        all_display_keys = DISPLAY_KEYS
        display_cols = []
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

        st.dataframe(fdf[display_cols], use_container_width=True, height=380)

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
    # TAB 2 — NPI CHATBOT
    # ════════════════════════════════════════════════════════
    with tab2:
        st.subheader("💬 NPI Impact Chatbot")
        st.markdown(
            "Describe a **New Product Introduction (NPI) change** — a new feature, product update, "
            "or architectural change — and I'll identify which feature request tickets would be "
            "impacted or addressed, including who to notify."
        )

        if df.empty:
            st.warning("No feature request tickets loaded. Please fix your data source first.")
            st.stop()

        if "chat_messages" not in st.session_state:
            st.session_state.chat_messages = []

        bar1, bar2 = st.columns([5, 1])
        with bar1:
            ticket_count = min(len(df), CHATBOT_MAX_TICKETS)
            label = f"Analyzing {ticket_count:,} feature request tickets"
            if ticket_count < len(df):
                label += f" ({ticket_count / len(df) * 100:.0f}% of total)"
            if contacts_extracted:
                label += f" · {contacts_extracted} contacts extracted"
            st.caption(label)
        with bar2:
            if st.button("🗑️ Clear Chat", use_container_width=True):
                st.session_state.chat_messages = []
                st.rerun()

        for msg in st.session_state.chat_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        if user_input := st.chat_input("e.g. 'We're adding bulk PDF export to the reporting module'…"):
            st.session_state.chat_messages.append({"role": "user", "content": user_input})
            with st.chat_message("user"):
                st.markdown(user_input)

            with st.chat_message("assistant"):
                placeholder = st.empty()
                full_response = ""

                ai = get_anthropic_client()
                if ai is None:
                    full_response = "❌ ANTHROPIC_API_KEY is not configured."
                    placeholder.markdown(full_response)
                else:
                    system_prompt = build_system_prompt(df, contacts)
                    api_msgs = [
                        {"role": m["role"], "content": m["content"]}
                        for m in st.session_state.chat_messages
                    ]

                    try:
                        with ai.messages.stream(
                            model="claude-opus-4-6",
                            max_tokens=4096,
                            thinking={"type": "adaptive"},
                            system=system_prompt,
                            messages=api_msgs,
                        ) as stream:
                            for chunk in stream.text_stream:
                                full_response += chunk
                                placeholder.markdown(full_response + "▌")

                        placeholder.markdown(full_response)

                    except Exception as e:
                        full_response = f"❌ Claude API error: {e}"
                        placeholder.markdown(full_response)

            st.session_state.chat_messages.append(
                {"role": "assistant", "content": full_response}
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


if __name__ == "__main__":
    main()
