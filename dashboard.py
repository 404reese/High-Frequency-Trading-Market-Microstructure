import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# 1. Page Configuration
st.set_page_config(layout="wide", page_title="HFT Microstructure", page_icon="📈")

# 2. Dynamic CSS Injection (Adapts to Dark/Light Mode automatically)
st.markdown(
    """
    <style>
        /* Base typography */
        .stApp {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
        }

        /* Header Styling */
        .header-container {
            border-bottom: 1px solid rgba(128, 128, 128, 0.2);
            padding-bottom: 1rem;
            margin-bottom: 2rem;
        }
        
        .header-title {
            font-size: 1.8rem;
            font-weight: 300;
            color: var(--text-color); /* Adapts to theme */
            margin: 0;
            letter-spacing: -0.5px;
        }
        
        .header-subtitle {
            font-size: 0.85rem;
            color: #888888;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 500;
        }

        /* Panel Cards (Uses Streamlit's native secondary background) */
        .panel {
            background-color: var(--secondary-background-color);
            border: 1px solid rgba(128, 128, 128, 0.2);
            border-radius: 6px;
            padding: 1.2rem;
            margin-bottom: 1rem;
        }

        .section-title {
            font-size: 0.75rem;
            font-weight: 600;
            color: #888888;
            text-transform: uppercase;
            letter-spacing: 1.2px;
            margin-bottom: 1rem;
        }

        /* Metric Widgets */
        div[data-testid="stMetric"] {
            background-color: var(--secondary-background-color);
            border: 1px solid rgba(128, 128, 128, 0.2);
            border-radius: 6px;
            padding: 1rem;
            border-left: 3px solid #3b82f6; /* Financial Blue Accent */
        }

        /* Hide Streamlit Branding */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
    </style>
    """,
    unsafe_allow_html=True,
)

# 3. Clean Header
st.markdown(
    """
    <div class="header-container">
        <h1 class="header-title">NASDAQ HFT Analytics</h1>
        <div class="header-subtitle">ITCH 5.0 Market Microstructure • Spark Processing Engine</div>
    </div>
    """,
    unsafe_allow_html=True,
)

# 4. Data Loading Logic
summary_path = Path("data/dashboard_summary.csv")
stock_summary_path = Path("data/stock_summary.csv")
legacy_path = Path("dashboard_data_aapl.csv")

@st.cache_data
def load_data():
    if summary_path.exists():
        stock_summary = pd.read_csv(stock_summary_path) if stock_summary_path.exists() else pd.DataFrame()
        return pd.read_csv(summary_path), stock_summary

    if legacy_path.exists():
        legacy_df = pd.read_csv(legacy_path).copy()
        if "volume" not in legacy_df.columns and "shares" in legacy_df.columns:
            legacy_df["volume"] = legacy_df["shares"]
        if "minute_bucket" not in legacy_df.columns and "time" in legacy_df.columns:
            legacy_df["minute_bucket"] = range(len(legacy_df))
        if "market_cat" not in legacy_df.columns:
            legacy_df["market_cat"] = "Unknown"
        stock_df = legacy_df.groupby(["stock", "market_cat"], as_index=False).agg(
            total_volume=("volume", "sum"),
            avg_price=("price", "mean"),
            trade_count=("price", "size"),
        )
        return legacy_df, stock_df

    return pd.DataFrame(), pd.DataFrame()

df, stock_df = load_data()

if df.empty:
    st.warning("System Notice: Run parser-new.py to generate data/dashboard_summary.csv, then reload.")
    st.stop()

# 5. Sidebar
with st.sidebar:
    st.markdown("<h3 style='font-size: 0.9rem; text-transform: uppercase; color: #888888;'>Controls</h3>", unsafe_allow_html=True)
    stocks = sorted(df["stock"].dropna().unique().tolist())
    selected_stock = st.selectbox("Ticker Symbol", stocks)
    
    st.markdown("---")
    st.markdown(
        "<div style='font-size: 0.8rem; color: #888888;'><b>Notice:</b> Dashboard utilizes compacted Spark exports to maintain responsiveness on standard hardware.</div>", 
        unsafe_allow_html=True
    )

filtered = df[df["stock"] == selected_stock].copy().sort_values("minute_bucket")

# 6. Top Metrics Row
metric_cols = st.columns(4)
with metric_cols[0]:
    st.metric("Total Volume", f"{filtered['volume'].sum():,.0f}")
with metric_cols[1]:
    st.metric("Average VWAP", f"${filtered['vwap'].mean():.2f}")
with metric_cols[2]:
    st.metric("Trade Count", f"{int(filtered['trade_count'].sum()):,}")
with metric_cols[3]:
    st.metric("Market Category", str(filtered["market_cat"].iloc[0]))

st.markdown("<br>", unsafe_allow_html=True)

# 7. Main Price/VWAP Chart
st.markdown('<div class="panel"><div class="section-title">Price vs. VWAP Dynamics</div>', unsafe_allow_html=True)
price_fig = go.Figure()

# Bright blue for price, dynamic neutral for VWAP
price_fig.add_trace(go.Scatter(x=filtered["minute_bucket"], y=filtered["avg_price"], name="Avg Price", line=dict(color="#3b82f6", width=1.5)))
price_fig.add_trace(go.Scatter(x=filtered["minute_bucket"], y=filtered["vwap"], name="VWAP", line=dict(color="#888888", dash="dot", width=1.5)))

price_fig.update_layout(
    hovermode="x unified", 
    margin=dict(l=0, r=0, t=10, b=0),
    height=350,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=10)),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    xaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.2)", zeroline=False, title=""),
    yaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.2)", zeroline=False, tickprefix="$", title="")
)
# By omitting theme=None, Streamlit seamlessly injects Dark Mode font styling
st.plotly_chart(price_fig, use_container_width=True, config={'displayModeBar': False})
st.markdown('</div>', unsafe_allow_html=True)

# 8. Volume and Top Stocks Row
left_col, right_col = st.columns([1, 1])

# Neutral steel color that looks good on both pure black and bright white
chart_bar_color = "#5d6d7e" 

with left_col:
    st.markdown('<div class="panel"><div class="section-title">Trading Volume Distribution</div>', unsafe_allow_html=True)
    vol_fig = px.bar(filtered, x="minute_bucket", y="volume")
    
    vol_fig.update_traces(marker_color=chart_bar_color, marker_line_width=0)
    vol_fig.update_layout(
        hovermode="x unified",
        margin=dict(l=0, r=0, t=10, b=0),
        height=280,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=False, zeroline=False, title=""),
        yaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.2)", zeroline=False, title="")
    )
    st.plotly_chart(vol_fig, use_container_width=True, config={'displayModeBar': False})
    st.markdown('</div>', unsafe_allow_html=True)

with right_col:
    st.markdown('<div class="panel"><div class="section-title">Liquidity by Ticker (Top 10)</div>', unsafe_allow_html=True)
    if not stock_df.empty:
        top_stocks = stock_df.nlargest(10, "total_volume")
        top_stock_fig = px.bar(top_stocks, x="stock", y="total_volume")
        
        top_stock_fig.update_traces(marker_color=chart_bar_color, marker_line_width=0)
        top_stock_fig.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=280,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(showgrid=False, zeroline=False, title="", tickangle=0),
            yaxis=dict(showgrid=True, gridcolor="rgba(128,128,128,0.2)", zeroline=False, title="")
        )
        st.plotly_chart(top_stock_fig, use_container_width=True, config={'displayModeBar': False})
    else:
        st.info("Awaiting stock summary data from parser-new.py")
    st.markdown('</div>', unsafe_allow_html=True)

# 9. Data Table
st.markdown('<div class="panel"><div class="section-title">Aggregated Trade Tape (Recent)</div>', unsafe_allow_html=True)
formatted_df = filtered.tail(10).copy()
if 'avg_price' in formatted_df.columns:
    formatted_df['avg_price'] = formatted_df['avg_price'].apply(lambda x: f"${x:.4f}")
if 'vwap' in formatted_df.columns:
    formatted_df['vwap'] = formatted_df['vwap'].apply(lambda x: f"${x:.4f}")
if 'volume' in formatted_df.columns:
    formatted_df['volume'] = formatted_df['volume'].apply(lambda x: f"{x:,.0f}")

st.dataframe(formatted_df, use_container_width=True, hide_index=True)
st.markdown('</div>', unsafe_allow_html=True)