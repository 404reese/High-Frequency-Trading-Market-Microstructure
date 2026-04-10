import { useEffect, useRef } from 'react';
import { createChart, ColorType, CrosshairMode } from 'lightweight-charts';

interface ChartProps {
  data: any[];
  obiData: any[];
}

export const TradingChart = ({ data, obiData }: ChartProps) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!chartContainerRef.current || data.length === 0) return;

    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: 'transparent' },
        textColor: '#8899aa',
      },
      grid: {
        vertLines: { color: 'rgba(42, 58, 74, 0.5)' },
        horzLines: { color: 'rgba(42, 58, 74, 0.5)' },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
      },
      rightPriceScale: {
        borderColor: 'rgba(42, 58, 74, 0.8)',
      },
      timeScale: {
        borderColor: 'rgba(42, 58, 74, 0.8)',
        timeVisible: true,
        secondsVisible: false,
      },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: '#00E676',
      downColor: '#FF3B30',
      borderVisible: false,
      wickUpColor: '#00E676',
      wickDownColor: '#FF3B30',
    });

    const formattedData = data.map((d: any) => ({
      time: (d.minute_bucket * 60) as any, // DuckDB bucket might be in minutes, adjust if seconds
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
    }));
    candleSeries.setData(formattedData);

    const vwapSeries = chart.addLineSeries({
      color: '#FFD600',
      lineWidth: 2,
      lineStyle: 2, // Dashed
      crosshairMarkerVisible: false,
    });

    const vwapData = data.map((d: any) => ({
      time: (d.minute_bucket * 60) as any,
      value: d.vwap,
    }));
    vwapSeries.setData(vwapData);

    // OBI Histogram
    const obiSeries = chart.addHistogramSeries({
      color: '#2962ff',
      priceFormat: { type: 'volume' },
      priceScaleId: '', // set as an overlay
    });

    chart.priceScale('').applyOptions({
      scaleMargins: {
        top: 0.8,
        bottom: 0,
      },
    });

    const formattedObi = obiData.map((d: any) => {
      const isPositive = d.OBI >= 0;
      return {
        time: (d.minute_bucket * 60) as any,
        value: Math.abs(d.OBI),
        color: isPositive ? 'rgba(0, 230, 118, 0.5)' : 'rgba(255, 59, 48, 0.5)',
      };
    });
    
    obiSeries.setData(formattedObi);

    const handleResize = () => {
      chart.applyOptions({ width: chartContainerRef.current?.clientWidth });
    };

    window.addEventListener('resize', handleResize);
    chart.timeScale().fitContent();

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, [data, obiData]);

  return (
    <div style={{ padding: '1rem', height: '100%' }}>
      <h3 style={{ fontSize: '0.8rem', color: '#00E676', letterSpacing: '1px', marginBottom: '1rem', textTransform: 'uppercase' }}>
        📊 Price Action, VWAP & OBI Momentum
      </h3>
      <div
        ref={chartContainerRef}
        style={{ width: '100%', height: 'calc(100% - 30px)' }}
      />
    </div>
  );
};
