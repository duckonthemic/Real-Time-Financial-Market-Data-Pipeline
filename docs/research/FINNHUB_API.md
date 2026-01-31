# Finnhub API Research

## Overview

Finnhub is a stock API providing real-time data for US stocks, forex, and crypto markets. This document summarizes key findings for our pipeline implementation.

## API Capabilities

### WebSocket Streaming

| Feature | Details |
|---------|---------|
| Endpoint | `wss://ws.finnhub.io?token=API_KEY` |
| Data Types | Trades, Quotes (premium only) |
| Latency | Near real-time (~50-200ms) |
| Format | JSON |

### Message Format

```json
{
  "type": "trade",
  "data": [
    {
      "s": "AAPL",      // Symbol
      "p": 150.25,      // Price
      "v": 100,         // Volume
      "t": 1706684400,  // Unix timestamp (ms)
      "c": ["1", "12"]  // Condition codes
    }
  ]
}
```

## Free Tier Limitations

| Limit | Value | Mitigation |
|-------|-------|------------|
| Connections | 1 per API key | Single producer instance |
| Symbols | 50 maximum | Prioritize high-volume stocks |
| Rate limit | 30 calls/second | Exponential backoff |
| Data quality | Some volume=0 | Filter in Spark silver layer |

## Reconnection Strategy

```
            ┌─────────────────────────────────────┐
            │        Connection Failed            │
            └──────────────┬──────────────────────┘
                           │
                           ▼
            ┌─────────────────────────────────────┐
            │   Wait: min(2^retry_count, 300) sec │
            └──────────────┬──────────────────────┘
                           │
                           ▼
            ┌─────────────────────────────────────┐
            │        Reconnect Attempt            │
            └──────────────┬──────────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
              ▼                         ▼
         [Success]                  [Failure]
         Reset retry=0         retry++ → Loop back
```

## Trade Condition Codes

| Code | Meaning |
|------|---------|
| 1 | Regular Sale |
| 2 | Cash Trade |
| 3 | Next Day Trade |
| 4 | Seller's Option |
| 12 | Form T (extended hours) |

## Market Hours (US/Eastern)

| Session | Hours |
|---------|-------|
| Pre-market | 4:00 AM - 9:30 AM |
| Regular | 9:30 AM - 4:00 PM |
| After-hours | 4:00 PM - 8:00 PM |

## Recommendations

1. **Symbol Selection**: Focus on high-volume stocks (AAPL, GOOGL, MSFT, etc.)
2. **Error Handling**: Implement circuit breaker for repeated failures
3. **Volume Filtering**: Filter volume=0 trades in silver layer
4. **Backup Sources**: Consider Alpha Vantage, Polygon.io as fallbacks
