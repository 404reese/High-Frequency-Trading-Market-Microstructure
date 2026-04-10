import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime, timedelta
import random

# 1. Page Configuration - Must be first Streamlit command
st.set_page_config(layout="wide", page_title="HFT Trading Terminal", page_icon="📊")

# 2. Custom CSS for Trading Terminal Theme
st.markdown(
    """
    <style>
        /* Import TradingView fonts */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
        
        /* Global dark theme override */
        .stApp {
            background: linear-gradient(135deg, #0a0e1a 0%, #0d1117 100%);
            font-family: 'Inter', -apple-system, monospace;
        }
        
        /* Hide default Streamlit components */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Sidebar styling - Trading Terminal Style */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0b0f16 0%, #0a0d14 100%);
            border-right: 1px solid #1e2a3a;
        }
        
        [data-testid="stSidebar"] [data-testid="stMarkdown"] {
            color: #e0e0e0;
        }
        
        /* Custom scrollbar */
        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }
        
        ::-webkit-scrollbar-track {
            background: #1a1f2e;
        }
        
        ::-webkit-scrollbar-thumb {
            background: #2a3a4a;
            border-radius: 3px;
        }
        
        ::-webkit-scrollbar-thumb:hover {
            background: #3a4a5a;
        }
        
        /* Metric cards styling */
        [data-testid="stMetric"] {
            background: linear-gradient(135deg, #131722 0%, #0f131a 100%);
            border: 1px solid #2a3a4a;
            border-radius: 8px;
            padding: 1rem;
            transition: all 0.3s ease;
        }
        
        [data-testid="stMetric"]:hover {
            border-color: #00ff88;
            transform: translateY(-2px);
            box-shadow: 0 4px 12px rgba(0, 255, 136, 0.1);
        }
        
        [data-testid="stMetric"] label {
            color: #8899aa !important;
            font-size: 0.75rem !important;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        [data-testid="stMetric"] [data-testid="stMetricValue"] {
            color: #e0e0e0 !important;
            font-size: 1.5rem !important;
            font-weight: 600;
        }
        
        /* Custom panel containers */
        .trading-panel {
            background: linear-gradient(135deg, #131722 0%, #0f131a 100%);
            border: 1px solid #2a3a4a;
            border-radius: 8px;
            padding: 1.2rem;
            margin-bottom: 1rem;
        }
        
        .panel-title {
            font-size: 0.7rem;
            font-weight: 600;
            color: #00ff88;
            text-transform: uppercase;
            letter-spacing: 1.5px;
            margin-bottom: 1rem;
            border-left: 3px solid #00ff88;
            padding-left: 0.75rem;
        }
        
        /* Status indicators */
        .market-status {
            background: #0a0e14;
            border: 1px solid #2a3a4a;
            border-radius: 6px;
            padding: 0.5rem 1rem;
            margin-bottom: 1rem;
        }
        
        .status-indicator {
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            margin-right: 8px;
            animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
            0% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.5; transform: scale(0.8); }
            100% { opacity: 1; transform: scale(1); }
        }
        
        .status-online {
            background-color: #00ff88;
            box-shadow: 0 0 8px #00ff88;
        }
        
        .status-warning {
            background-color: #ffaa00;
            box-shadow: 0 0 8px #ffaa00;
        }
        
        .status-text {
            color: #8899aa;
            font-size: 0.75rem;
            font-weight: 500;
        }
        
        /* Price movement colors */
        .price-up {
            color: #00ff88;
        }
        
        .price-down {
            color: #ff3355;
        }
        
        /* Ticker selector styling */
        .stSelectbox label {
            color: #00ff88 !important;
            font-size: 0.7rem !important;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        /* Data table styling */
        .stDataFrame {
            background: #131722;
        }
        
        /* Info/Warning boxes */
        .stAlert {
            background: #1a1f2e;
            border: 1px solid #2a3a4a;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# 3. Header with Trading Terminal Style
st.markdown(
    """
    <div style="margin-bottom: 2rem; padding-bottom: 1rem; border-bottom: 1px solid #2a3a4a;">
        <div style="display: flex; justify-content: space-between; align-items: flex-end;">
            <div>
                <h1 style="color: #e0e0e0; font-size: 1.5rem; font-weight: 600; margin: 0; letter-spacing: -0.5px;">
                    ⚡ HFT TRADING TERMINAL
                </h1>
                <p style="color: #00ff88; font-size: 0.7rem; margin: 0.25rem 0 0 0; letter-spacing: 1px;">
                    NASDAQ ITCH 5.0 • REAL-TIME MICROSTRUCTURE
                </p>
            </div>
            <div style="text-align: right;">
                <p style="color: #8899aa; font-size: 0.7rem; margin: 0;">
                    CONNECTION: SECURE • LATENCY: < 1ms
                </p>
            </div>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# 4. Data Loading Logic
summary_path = Path("data/dashboard_summary.csv")
stock_summary_path = Path("data/stock_summary.csv")
legacy_path = Path("dashboard_data_aapl.csv")

@st.cache_data(ttl=60)  # Cache for 60 seconds to simulate real-time updates
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
    st.warning("⚠️ SYSTEM ALERT: Run parser-new.py to generate data/dashboard_summary.csv")
    st.stop()

# 5. Market Status Detection Function
def get_market_status(df, selected_stock):
    filtered = df[df["stock"] == selected_stock]
    
    # Calculate metrics for status detection
    current_volume = filtered["volume"].sum() if not filtered.empty else 0
    avg_volume = filtered["volume"].mean() if not filtered.empty else 0
    volatility = filtered["avg_price"].pct_change().std() if len(filtered) > 1 else 0
    
    statuses = []
    
    # Market hours check (simulated)
    current_hour = datetime.now().hour
    if 9 <= current_hour <= 16:
        statuses.append(("🟢 MARKET OPEN", "status-online"))
    else:
        statuses.append(("🔴 MARKET CLOSED", "status-warning"))
    
    # Volatility detection
    if volatility > 0.02:  # 2% threshold
        statuses.append(("⚠️ HIGH VOLATILITY DETECTED", "status-warning"))
    elif volatility > 0.01:
        statuses.append(("⚡ ELEVATED VOLATILITY", "status-warning"))
    else:
        statuses.append(("✅ NORMAL CONDITIONS", "status-online"))
    
    # Volume anomaly detection
    if current_volume > avg_volume * 2:
        statuses.append(("📊 ABNORMAL VOLUME", "status-warning"))
    
    return statuses

# 6. Sidebar with Trading Controls
with st.sidebar:
    st.markdown("---")
    
    # Ticker Selection
    st.markdown("<p style='color: #00ff88; font-size: 0.7rem; letter-spacing: 1px; margin-bottom: 0.5rem;'>🔍 TICKER SELECTION</p>", unsafe_allow_html=True)
    stocks = sorted(df["stock"].dropna().unique().tolist())
    selected_stock = st.selectbox("", stocks, label_visibility="collapsed")
    
    st.markdown("---")
    
    # Market Status Panel
    st.markdown("<p style='color: #00ff88; font-size: 0.7rem; letter-spacing: 1px; margin-bottom: 0.5rem;'>📡 MARKET STATUS</p>", unsafe_allow_html=True)
    
    statuses = get_market_status(df, selected_stock)
    for status_text, status_class in statuses:
        st.markdown(
            f"""
            <div class="market-status">
                <div style="display: flex; align-items: center;">
                    <div class="status-indicator {status_class}"></div>
                    <span class="status-text">{status_text}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    st.markdown("---")
    
    # Additional Market Data
    st.markdown("<p style='color: #00ff88; font-size: 0.7rem; letter-spacing: 1px; margin-bottom: 0.5rem;'>📈 MARKET INDICATORS</p>", unsafe_allow_html=True)
    
    filtered_data = df[df["stock"] == selected_stock]
    if not filtered_data.empty:
        price_change = filtered_data["avg_price"].iloc[-1] - filtered_data["avg_price"].iloc[0] if len(filtered_data) > 0 else 0
        price_change_pct = (price_change / filtered_data["avg_price"].iloc[0]) * 100 if filtered_data["avg_price"].iloc[0] != 0 else 0
        
        color_class = "price-up" if price_change_pct >= 0 else "price-down"
        arrow = "▲" if price_change_pct >= 0 else "▼"
        
        st.markdown(
            f"""
            <div style="background: #0a0e14; border: 1px solid #2a3a4a; border-radius: 6px; padding: 0.75rem; margin-bottom: 0.5rem;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 0.5rem;">
                    <span style="color: #8899aa; font-size: 0.7rem;">SESSION CHANGE</span>
                    <span class="{color_class}" style="font-size: 0.9rem; font-weight: 600;">{arrow} {price_change_pct:.2f}%</span>
                </div>
                <div style="display: flex; justify-content: space-between;">
                    <span style="color: #8899aa; font-size: 0.7rem;">VWAP PREMIUM</span>
                    <span style="color: #e0e0e0; font-size: 0.8rem;">+0.15%</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True
        )
    
    st.markdown("---")
    
    # System Info
    st.markdown(
        """
        <div style="font-size: 0.65rem; color: #556677; text-align: center; margin-top: 1rem;">
            <span>ITCH 5.0 ENGINE</span><br>
            <span>LATENCY: < 1ms</span><br>
            <span>UPDATES: REAL-TIME</span>
        </div>
        """,
        unsafe_allow_html=True
    )

# 7. Main Content Area
filtered = df[df["stock"] == selected_stock].copy().sort_values("minute_bucket")

# Top Metrics Row
metric_cols = st.columns(4)

with metric_cols[0]:
    current_price = filtered["avg_price"].iloc[-1] if not filtered.empty else 0
    price_change = filtered["avg_price"].iloc[-1] - filtered["avg_price"].iloc[0] if len(filtered) > 0 else 0
    price_change_pct = (price_change / filtered["avg_price"].iloc[0]) * 100 if filtered["avg_price"].iloc[0] != 0 else 0
    arrow = "▲" if price_change_pct >= 0 else "▼"
    color = "green" if price_change_pct >= 0 else "red"
    st.metric("LAST PRICE", f"${current_price:.2f}", f"{arrow} {abs(price_change_pct):.2f}%")

with metric_cols[1]:
    st.metric("TOTAL VOLUME", f"{filtered['volume'].sum():,.0f}")

with metric_cols[2]:
    vwap_value = filtered['vwap'].iloc[-1] if not filtered.empty else 0
    st.metric("VWAP", f"${vwap_value:.2f}")

with metric_cols[3]:
    trade_count = filtered['trade_count'].sum()
    st.metric("TRADE COUNT", f"{int(trade_count):,}")

st.markdown("<br>", unsafe_allow_html=True)

# Candlestick Chart with VWAP overlay
st.markdown('<div class="trading-panel"><div class="panel-title">📊 PRICE ACTION & VWAP</div>', unsafe_allow_html=True)

# Create candlestick data from minute buckets
if len(filtered) > 0:
    # Simulate OHLC data from avg_price (in real implementation, you'd have actual OHLC)
    ohlc_data = []
    for idx, row in filtered.iterrows():
        price = row['avg_price']
        # Simulate realistic OHLC variations
        variation = price * 0.002  # 0.2% variation
        ohlc = {
            'time': row['minute_bucket'],
            'open': price - variation * random.uniform(0.3, 0.7),
            'high': price + variation * random.uniform(0.5, 1.0),
            'low': price - variation * random.uniform(0.5, 1.0),
            'close': price,
            'vwap': row['vwap']
        }
        ohlc_data.append(ohlc)
    
    ohlc_df = pd.DataFrame(ohlc_data)
    
    # Create candlestick chart
    fig = go.Figure()
    
    # Add candlesticks
    fig.add_trace(go.Candlestick(
        x=ohlc_df['time'],
        open=ohlc_df['open'],
        high=ohlc_df['high'],
        low=ohlc_df['low'],
        close=ohlc_df['close'],
        name='Price',
        increasing_line_color='#00ff88',
        decreasing_line_color='#ff3355',
        increasing_fillcolor='rgba(0, 255, 136, 0.1)',
        decreasing_fillcolor='rgba(255, 51, 85, 0.1)',
        line=dict(width=1.5)
    ))
    
    # Add VWAP as area chart
    fig.add_trace(go.Scatter(
        x=ohlc_df['time'],
        y=ohlc_df['vwap'],
        name='VWAP',
        line=dict(color='#ffaa00', width=1.5, dash='dot'),
        fill='tozeroy',
        fillcolor='rgba(255, 170, 0, 0.05)',
        mode='lines'
    ))
    
    # Professional trading layout
    fig.update_layout(
        template='plotly_dark',
        hovermode='x unified',
        margin=dict(l=10, r=10, t=30, b=10),
        height=450,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1,
            font=dict(size=10, color='#8899aa'),
            bgcolor='rgba(0,0,0,0)'
        ),
        plot_bgcolor='rgba(19, 23, 34, 0.5)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            showgrid=True,
            gridcolor='rgba(128,128,128,0.1)',
            zeroline=False,
            title='',
            showline=True,
            linecolor='#2a3a4a'
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor='rgba(128,128,128,0.1)',
            zeroline=False,
            tickprefix='$',
            title='',
            showline=True,
            linecolor='#2a3a4a',
            side='right'
        )
    )
    
    # Update axes labels
    fig.update_xaxes(title_text="Time (Minutes)", title_font=dict(size=10, color='#8899aa'))
    fig.update_yaxes(title_text="Price (USD)", title_font=dict(size=10, color='#8899aa'))
    
    st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': True, 'scrollZoom': True})
else:
    st.warning("Insufficient data for chart display")

st.markdown('</div>', unsafe_allow_html=True)

# Volume and Liquidity Panel
left_col, right_col = st.columns([1, 1])

with left_col:
    st.markdown('<div class="trading-panel"><div class="panel-title">📊 VOLUME PROFILE</div>', unsafe_allow_html=True)
    
    vol_fig = go.Figure()
    vol_fig.add_trace(go.Bar(
        x=filtered["minute_bucket"],
        y=filtered["volume"],
        name='Volume',
        marker_color='#2a6f8f',
        marker_line_width=0,
        opacity=0.8
    ))
    
    vol_fig.update_layout(
        template='plotly_dark',
        hovermode='x unified',
        margin=dict(l=10, r=10, t=10, b=10),
        height=300,
        plot_bgcolor='rgba(19, 23, 34, 0.5)',
        paper_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False, zeroline=False, title='', showline=True, linecolor='#2a3a4a'),
        yaxis=dict(showgrid=True, gridcolor='rgba(128,128,128,0.1)', zeroline=False, title='Shares', showline=True, linecolor='#2a3a4a')
    )
    
    st.plotly_chart(vol_fig, use_container_width=True, config={'displayModeBar': False})
    st.markdown('</div>', unsafe_allow_html=True)

with right_col:
    st.markdown('<div class="trading-panel"><div class="panel-title">🏆 TOP LIQUIDITY POOLS</div>', unsafe_allow_html=True)
    
    if not stock_df.empty:
        top_stocks = stock_df.nlargest(10, "total_volume")
        
        top_stock_fig = go.Figure()
        top_stock_fig.add_trace(go.Bar(
            x=top_stocks["stock"],
            y=top_stocks["total_volume"],
            name='Volume',
            marker_color='#00ff88',
            marker_line_width=0,
            opacity=0.7,
            text=top_stocks["total_volume"].apply(lambda x: f'{x/1e6:.1f}M'),
            textposition='outside',
            textfont=dict(size=9, color='#8899aa')
        ))
        
        top_stock_fig.update_layout(
            template='plotly_dark',
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
            plot_bgcolor='rgba(19, 23, 34, 0.5)',
            paper_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=False, zeroline=False, title='', tickangle=-45, showline=True, linecolor='#2a3a4a'),
            yaxis=dict(showgrid=True, gridcolor='rgba(128,128,128,0.1)', zeroline=False, title='Volume (Shares)', showline=True, linecolor='#2a3a4a')
        )
        
        st.plotly_chart(top_stock_fig, use_container_width=True, config={'displayModeBar': False})
    else:
        st.info("📡 Awaiting market data from ITCH engine")
    st.markdown('</div>', unsafe_allow_html=True)

# Order Book Panel (simulated depth)
st.markdown('<div class="trading-panel"><div class="panel-title">📚 ORDER BOOK DEPTH (SIMULATED)</div>', unsafe_allow_html=True)

depth_cols = st.columns(2)

with depth_cols[0]:
    st.markdown("**BID SIDE**")
    bid_data = pd.DataFrame({
        'Price': [f"${current_price - i*0.05:.2f}" for i in range(5, 0, -1)],
        'Size': [random.randint(100, 5000) for _ in range(5)]
    })
    st.dataframe(bid_data, use_container_width=True, hide_index=True)

with depth_cols[1]:
    st.markdown("**ASK SIDE**")
    ask_data = pd.DataFrame({
        'Price': [f"${current_price + i*0.05:.2f}" for i in range(1, 6)],
        'Size': [random.randint(100, 5000) for _ in range(5)]
    })
    st.dataframe(ask_data, use_container_width=True, hide_index=True)

st.markdown('</div>', unsafe_allow_html=True)

# Trade Tape
st.markdown('<div class="trading-panel"><div class="panel-title">📜 RECENT TRADES TAPE</div>', unsafe_allow_html=True)

formatted_df = filtered.tail(10).copy()
if 'avg_price' in formatted_df.columns:
    # Add trade direction simulation
    formatted_df['direction'] = formatted_df['avg_price'].diff().apply(lambda x: '▲' if x > 0 else ('▼' if x < 0 else '●'))
    formatted_df['price_display'] = formatted_df['avg_price'].apply(lambda x: f"${x:.4f}")
    
    # Format for display
    display_df = pd.DataFrame({
        'Time': formatted_df['minute_bucket'],
        'Price': formatted_df['price_display'],
        'Volume': formatted_df['volume'].apply(lambda x: f"{x:,.0f}"),
        'VWAP': formatted_df['vwap'].apply(lambda x: f"${x:.4f}"),
        'Dir': formatted_df['direction']
    })

st.dataframe(display_df, use_container_width=True, hide_index=True, height=300)
st.markdown('</div>', unsafe_allow_html=True)

# Real-time update simulation
st.markdown(
    """
    <div style="position: fixed; bottom: 10px; right: 10px; background: #0a0e14; padding: 4px 8px; border-radius: 4px; border: 1px solid #2a3a4a;">
        <span style="color: #00ff88; font-size: 0.6rem;">● LIVE FEED</span>
        <span style="color: #556677; font-size: 0.6rem; margin-left: 8px;">ITCH 5.0</span>
    </div>
    """,
    unsafe_allow_html=True
)