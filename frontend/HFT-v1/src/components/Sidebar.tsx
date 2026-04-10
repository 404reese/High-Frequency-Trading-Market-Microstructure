import { Activity, TrendingUp, TrendingDown, BookOpen, Clock } from 'lucide-react';
import './components.css';

interface SidebarProps {
  sentiment: any;
  stock: string;
}

export const Sidebar = ({ sentiment, stock }: SidebarProps) => {
  const isBullish = sentiment?.status === 'BULLISH';
  const isBearish = sentiment?.status === 'BEARISH';
  
  return (
    <div className="sidebar glass-panel">
      <div className="sidebar-header">
        <h2>{stock} Overview</h2>
        <span className="mono-text live-indicator">● LIVE</span>
      </div>

      <div className="metric-card">
        <div className="metric-title">
          <Activity size={14} /> Machine Learning Sentiment
        </div>
        <div className="sentiment-display">
          <div className="score-dial">
            <svg viewBox="0 0 36 36" className="circular-chart">
              <path className="circle-bg"
                d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
              />
              <path className={`circle ${isBullish ? 'bullish' : isBearish ? 'bearish' : 'neutral'}`}
                strokeDasharray={`${sentiment?.sentiment_score || 0}, 100`}
                d="M18 2.0845 a 15.9155 15.9155 0 0 1 0 31.831 a 15.9155 15.9155 0 0 1 0 -31.831"
              />
            </svg>
            <div className={`score-value mono-text ${isBullish ? 'text-up' : isBearish ? 'text-down' : 'text-neutral'}`}>
              {sentiment?.sentiment_score?.toFixed(0)}
            </div>
          </div>
          <div className="status-badge" style={{
            background: isBullish ? 'rgba(0, 230, 118, 0.1)' : isBearish ? 'rgba(255, 59, 48, 0.1)' : 'rgba(255, 214, 0, 0.1)',
            color: isBullish ? '#00E676' : isBearish ? '#FF3B30' : '#FFD600',
            border: `1px solid ${isBullish ? '#00E676' : isBearish ? '#FF3B30' : '#FFD600'}`
          }}>
            {isBullish ? <TrendingUp size={14} /> : isBearish ? <TrendingDown size={14} /> : <Activity size={14} />}
            {sentiment?.status || 'CALCULATING'}
          </div>
        </div>
        <div className="metric-desc">
          Sprint 3 RF Model Proxy based on Volatility & OBI
        </div>
      </div>

      <div className="metric-card animate-slide-in" style={{ animationDelay: '0.1s' }}>
        <div className="metric-title">
          <BookOpen size={14} /> Order Book Imbalance (OBI)
        </div>
        <div className="metric-value mono-text">
          {sentiment?.obi > 0 ? '+' : ''}{sentiment?.obi?.toFixed(4) || '0.0000'}
        </div>
        <div className="metric-desc">
          Instantaneous bid/ask volume ratio
        </div>
      </div>

      <div className="system-info">
        <Clock size={12} /> ITCH 5.0 ENGINE • LATENCY: &lt; 2ms
      </div>
    </div>
  );
};
