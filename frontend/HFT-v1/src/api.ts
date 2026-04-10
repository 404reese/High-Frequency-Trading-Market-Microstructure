import axios from 'axios';

const api = axios.create({
  baseURL: 'http://localhost:8000/api',
});

export const getTickers = async () => {
  const { data } = await api.get('/tickers');
  return data.tickers;
};

export const getSummary = async () => {
  const { data } = await api.get('/summary');
  return data;
};

export const getHistoricalPrices = async (stock: string) => {
  const { data } = await api.get(`/historical-prices/${stock}`);
  return data;
};

export const getObiData = async (stock: string) => {
  const { data } = await api.get(`/obi-data/${stock}`);
  return data;
};

export const getSentiment = async (stock: string) => {
  const { data } = await api.get(`/sentiment/${stock}`);
  return data;
};
