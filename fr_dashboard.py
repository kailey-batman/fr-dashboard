import streamlit as st
import pandas as pd
import anthropic
import gspread
from google.oauth2.service_account import Credentials
import json
import os
from datetime import datetime
import io

# ============================================================
# PAGE CONFIG
# ============================================================
st.set_page_config(
    page_title="Feature Request Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

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
# In Shortcut this is typically lowercase "feature" — confirm against your sheet.
# Set to None to skip type-filtering and show all rows.
FEATURE_REQUEST_TYPE = "feature"

# Columns shown in the main data table (logical key names from COLUMNS dict above)
DISPLAY_KEYS = ["id", "timestamp", "title", "submitter", "product_area", "priority", "severity", "status", "labels", "epic"]

# Max tickets sent to the chatbot as context (to stay well inside the context window)
CHATBOT_MAX_TICKETS = 500

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
    # 1) Railway / Docker env var
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        creds = Credentials.from_service_account_info(json.loads(sa_json), scopes=SCOPES)
        return gspread.authorize(creds)

    # 2) Streamlit secrets
    try:
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]), scopes=SCOPES
        )
        return gspread.authorize(creds)
    except Exception:
        pass

    # 3) Local file
    if os.path.exists("service_account.json"):
        creds = Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
        return gspread.authorize(creds)

    return None


@st.cache_data(ttl=300)
def load_feature_requests() -> pd.DataFrame:
    """Pull the main sheet and return only feature-request rows."""
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

        # Parse timestamp
        ts_col = COLUMNS.get("timestamp", "")
        if ts_col and ts_col in df.columns:
            df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")

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
# CHATBOT HELPERS
# ============================================================


def _get(row: pd.Series, key: str, default: str = "") -> str:
    """Safely retrieve a column value by logical key."""
    col = COLUMNS.get(key, "")
    if col and col in row.index:
        val = row.get(col, "")
        return str(val).strip() if pd.notna(val) and str(val).strip() else default
    return default


def format_tickets_for_context(df: pd.DataFrame) -> str:
    """Condense feature-request rows into a compact string for Claude."""
    if df.empty:
        return "No feature request tickets available."

    sample = df.head(CHATBOT_MAX_TICKETS)
    lines = []

    for i, (_, row) in enumerate(sample.iterrows(), start=1):
        title = _get(row, "title") or f"Ticket #{i}"
        ticket_id = _get(row, "id")
        area = _get(row, "product_area")
        priority = _get(row, "priority")
        severity = _get(row, "severity")
        submitter = _get(row, "submitter")
        owners = _get(row, "owners")
        ts = _get(row, "timestamp")
        description = _get(row, "description")
        labels = _get(row, "labels")
        epic = _get(row, "epic")
        team = _get(row, "team")
        status = _get(row, "status")

        id_part = f"sc-{ticket_id}" if ticket_id else f"#{i}"
        header = f"[{id_part}] {title}"
        if area:
            header += f"  |  Area: {area}"
        if priority:
            header += f"  |  Priority: {priority}"
        if severity:
            header += f"  |  Severity: {severity}"
        if status:
            header += f"  |  State: {status}"
        if submitter:
            header += f"  |  Requester: {submitter}"
        if ts:
            header += f"  |  Created: {str(ts)[:10]}"

        body_lines = [header]
        if description:
            body_lines.append(f"   Description: {description[:400]}{'...' if len(description) > 400 else ''}")
        if labels:
            body_lines.append(f"   Labels: {labels}")
        if epic:
            body_lines.append(f"   Epic: {epic}")
        if team:
            body_lines.append(f"   Team: {team}")
        if owners:
            body_lines.append(f"   Owners: {owners}")

        lines.append("\n".join(body_lines))

    total = len(df)
    suffix = ""
    if total > CHATBOT_MAX_TICKETS:
        suffix = f"\n\n[Note: Showing {CHATBOT_MAX_TICKETS} of {total} total tickets due to context limits.]"

    return "\n\n".join(lines) + suffix


def build_system_prompt(df: pd.DataFrame) -> str:
    ticket_count = min(len(df), CHATBOT_MAX_TICKETS)
    tickets_text = format_tickets_for_context(df)

    return f"""You are a product analyst specializing in NPI (New Product Introduction) impact assessment.

You have access to {ticket_count} customer feature request ticket(s). When the user describes an NPI change — a new feature, product update, architectural change, or capability — analyze ALL tickets and identify which ones are relevant or impacted.

## Feature Request Tickets

{tickets_text}

---

## Your Job

When the user describes an NPI change, respond with:

1. **Brief restatement** of the NPI change as you understand it (one sentence).

2. **Directly Addressed (N tickets):** — tickets the NPI change fully or substantially fulfills.
   - #[number] **[Title]** — [one sentence explaining why this NPI addresses it]

3. **Potentially Impacted (N tickets):** — tickets in the same area that may be partially addressed, affected, or made obsolete.
   - #[number] **[Title]** — [one sentence on the impact]

4. **Related Context (N tickets):** — tickets in adjacent areas worth considering together.
   - #[number] **[Title]** — [one sentence on the connection]

5. **Summary** — 2–3 sentences: how much existing customer demand does this NPI cover? Are there major unaddressed themes?

**Be thorough.** Include borderline tickets and note your uncertainty. Err on the side of inclusion — a false positive is better than a missed relevant ticket.

For follow-up questions (e.g. "tell me more about #7" or "which of those can be closed?"), answer conversationally using the ticket data above."""


# ============================================================
# UTILITY
# ============================================================


def resolve_col(key: str, df: pd.DataFrame):
    """Return the actual column name if it exists in df, else None."""
    col = COLUMNS.get(key, "")
    return col if col and col in df.columns else None


# ============================================================
# MAIN APP
# ============================================================


def main():
    # ── Sidebar ─────────────────────────────────────────────
    with st.sidebar:
        try:
            st.image("logo.svg", width=160)
        except Exception:
            st.markdown("## 🚀 FR Dashboard")

        st.markdown("---")
        st.markdown("### Data")
        if st.button("🔄 Refresh", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        st.caption(f"Last loaded: {datetime.now().strftime('%H:%M:%S')}")

    # ── Load data ────────────────────────────────────────────
    with st.spinner("Loading feature requests…"):
        df = load_feature_requests()

    # ── Tabs ─────────────────────────────────────────────────
    tab1, tab2, tab3 = st.tabs(["📋 Feature Requests", "💬 NPI Chatbot", "📊 Google Sheet"])

    # ════════════════════════════════════════════════════════
    # TAB 1 — FEATURE REQUESTS DASHBOARD
    # ════════════════════════════════════════════════════════
    with tab1:
        if df.empty:
            st.warning("No feature request tickets found. Check your sheet ID, tab name, and column configuration.")
            st.stop()

        st.subheader(f"Feature Requests — {len(df):,} total")

        # ── Metrics row ──────────────────────────────────────
        m1, m2, m3, m4 = st.columns(4)

        with m1:
            st.metric("Total Requests", f"{len(df):,}")

        priority_col = resolve_col("priority", df)
        with m2:
            if priority_col:
                high = df[priority_col].astype(str).str.lower().isin(["high", "critical"]).sum()
                st.metric("High Priority", high)
            else:
                st.metric("High Priority", "—")

        status_col = resolve_col("status", df)
        with m3:
            if status_col:
                open_count = (~df[status_col].astype(str).str.lower().isin(
                    ["completed", "done", "cancelled", "canceled", "archived"]
                )).sum()
                st.metric("Open", open_count)
            else:
                st.metric("Open", "—")

        area_col = resolve_col("product_area", df)
        with m4:
            if area_col:
                st.metric("Product Areas", df[area_col].nunique())
            else:
                sub_col = resolve_col("submitter", df)
                st.metric("Submitters", df[sub_col].nunique() if sub_col else "—")

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
        desc_col = resolve_col("description", fdf)

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
        display_cols = [
            COLUMNS[k]
            for k in DISPLAY_KEYS
            if COLUMNS.get(k) and COLUMNS[k] in fdf.columns
        ]
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
                    uc_col = resolve_col("use_case", fdf)
                    if uc_col and pd.notna(row.get(uc_col)):
                        st.markdown("**Use Case**")
                        st.markdown(str(row.get(uc_col, "—")))
                    imp_col = resolve_col("impact", fdf)
                    if imp_col and pd.notna(row.get(imp_col)):
                        st.markdown("**Business Impact**")
                        st.markdown(str(row.get(imp_col, "—")))

                with dcol2:
                    for key in ["submitter", "company", "product_area", "priority", "status", "timestamp"]:
                        col_name = COLUMNS.get(key, "")
                        if col_name and col_name in row.index and pd.notna(row.get(col_name)):
                            val = row.get(col_name)
                            if hasattr(val, "strftime"):
                                val = val.strftime("%Y-%m-%d")
                            st.markdown(f"**{col_name}:** {val}")

        # ── Charts ────────────────────────────────────────────
        st.markdown("---")
        cc1, cc2 = st.columns(2)

        with cc1:
            if area_col and area_col in df.columns:
                st.subheader("By Product Area")
                area_counts = (
                    df[area_col].astype(str).value_counts().reset_index()
                )
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
            "impacted or addressed."
        )

        if df.empty:
            st.warning("No feature request tickets loaded. Please fix your data source first.")
            st.stop()

        # Session state
        if "chat_messages" not in st.session_state:
            st.session_state.chat_messages = []

        # Controls bar
        bar1, bar2 = st.columns([5, 1])
        with bar1:
            ticket_count = min(len(df), CHATBOT_MAX_TICKETS)
            pct = ticket_count / len(df) * 100 if len(df) > 0 else 0
            label = f"Analyzing {ticket_count:,} feature request tickets"
            if ticket_count < len(df):
                label += f" ({pct:.0f}% of total — increase CHATBOT_MAX_TICKETS to see more)"
            st.caption(label)
        with bar2:
            if st.button("🗑️ Clear Chat", use_container_width=True):
                st.session_state.chat_messages = []
                st.rerun()

        # Render existing messages
        for msg in st.session_state.chat_messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

        # Chat input
        if user_input := st.chat_input(
            "e.g. 'We're adding bulk PDF export to the reporting module'…"
        ):
            # Show user message
            st.session_state.chat_messages.append({"role": "user", "content": user_input})
            with st.chat_message("user"):
                st.markdown(user_input)

            # Stream assistant response
            with st.chat_message("assistant"):
                placeholder = st.empty()
                full_response = ""

                ai = get_anthropic_client()
                if ai is None:
                    full_response = "❌ ANTHROPIC_API_KEY is not configured."
                    placeholder.markdown(full_response)
                else:
                    system_prompt = build_system_prompt(df)
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
