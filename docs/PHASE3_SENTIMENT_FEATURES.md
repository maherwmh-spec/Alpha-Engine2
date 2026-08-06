# Phase 3 — Sentiment + Feature Engineering (implemented)

## Delivered

| Item | Path |
|------|------|
| Migration | `migrations/008_sentiment_and_features.sql` |
| Feature bot | `bots/feature_engineer/` |
| Sentiment bot | `bots/sentiment_analyzer/` |
| Schedules | feature 16:45, sentiment 17:00 (Asia/Riyadh) |
| Config | `bots.feature_engineer`, `bots.sentiment_analyzer` |

## Nightly order

1. 16:45 feature_engineer
2. 17:00 sentiment_analyzer
3. 17:30 stock_personality (Phase 2)
4. 18:00 genetic-engine-run (Phase 1)

## Scope v1

- Compute + store only
- No Scientist / Watchlist / FreqAI consumption yet
- No Featuretools

## Sentiment behavior

- Reads `market_data.news` when available
- FinBERT (`ProsusAI/finbert`) when transformers/torch available
- Statuses: `ok`, `no_news`, `model_unavailable`, `error`
