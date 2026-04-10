import { useState, useEffect } from 'react'
import { Zap } from 'lucide-react'
import './App.css'
import { TradingChart } from './components/TradingChart'
import { Sidebar } from './components/Sidebar'
import { getTickers, getHistoricalPrices, getObiData, getSentiment } from './api'

function App() {
  const [tickers, setTickers] = useState<string[]>([])
  const [selectedStock, setSelectedStock] = useState<string>('')
  
  const [prices, setPrices] = useState<any[]>([])
  const [obi, setObi] = useState<any[]>([])
  const [sentiment, setSentiment] = useState<any>(null)
  
  const [loading, setLoading] = useState<boolean>(true)

  // 1. Initial Load of Tickers
  useEffect(() => {
    const fetchInit = async () => {
      try {
        const symbolList = await getTickers()
        setTickers(symbolList)
        if (symbolList.length > 0) {
          setSelectedStock(symbolList[0])
        }
      } catch (err) {
        console.error("Error fetching tickers:", err)
      }
    }
    fetchInit()
  }, [])

  // 2. Load Ticker-specific Data
  useEffect(() => {
    if (!selectedStock) return
    
    const fetchStockData = async () => {
      setLoading(true)
      try {
        const [priceData, obiData, sentimentData] = await Promise.all([
          getHistoricalPrices(selectedStock),
          getObiData(selectedStock),
          getSentiment(selectedStock)
        ])
        
        setPrices(priceData)
        setObi(obiData)
        setSentiment(sentimentData)
        
      } catch (err) {
        console.error("Error fetching stock data:", err)
      } finally {
        setLoading(false)
      }
    }
    
    fetchStockData()
  }, [selectedStock])

  return (
    <div className="app-container">
      {/* Top Navbar */}
      <header className="top-navbar glass-panel">
        <div className="brand">
          <div className="brand-icon">
            <Zap size={20} />
          </div>
          <div>
            <h1>HFT Terminal</h1>
            <span>ULTRA-LOW LATENCY MARKET MICROSTRUCTURE</span>
          </div>
        </div>
        
        <div className="nav-controls animate-slide-in">
          <select 
            className="ticker-select" 
            value={selectedStock} 
            onChange={(e) => setSelectedStock(e.target.value)}
          >
            {tickers.map(ticker => (
              <option key={ticker} value={ticker}>{ticker.toUpperCase()}</option>
            ))}
          </select>
        </div>
      </header>

      {/* Main Dashboard Grid */}
      <main className="dashboard-grid">
        {/* Left Sidebar */}
        <Sidebar sentiment={sentiment} stock={selectedStock} />
        
        {/* Charting Area */}
        <div className="glass-panel" style={{ height: '100%', overflow: 'hidden' }}>
          {loading ? (
            <div style={{ display: 'flex', height: '100%', alignItems: 'center', justifyContent: 'center', color: 'var(--text-secondary)' }}>
              INITIALIZING ENGINE...
            </div>
          ) : (
            <TradingChart data={prices} obiData={obi} />
          )}
        </div>
      </main>
    </div>
  )
}

export default App
