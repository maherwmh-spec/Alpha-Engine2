# Phase 2 — Stock Personality (implemented)

## Delivered

| Item | Path |
|------|------|
| Migration | `migrations/007_stock_personalities.sql` |
| Bot | `bots/stock_personality/bot.py` |
| Celery task | `bots/stock_personality/tasks.py` |
| Schedule | 17:30 Asia/Riyadh (`stock-personality-run`) before genetic 18:00 |
| Config | `bots.stock_personality` in `config/config.yaml` |

## Deploy

```bash
git pull origin master
# apply migration
psql "$DATABASE_URL" -f migrations/007_stock_personalities.sql
# restart celery worker + beat
```

## Manual test

```bash
celery -A scripts.celery_app call bots.stock_personality.tasks.run_stock_personality --kwargs='{"max_symbols":5}'
# or inside app container
python -c "from bots.stock_personality.bot import StockPersonalityBot; print(StockPersonalityBot().run(max_symbols=5))"
```

```sql
SELECT symbol, phase, phase_confidence, beta_90d, accumulation_score, explosiveness_score, distribution_style, status
FROM market_data.stock_personalities
ORDER BY updated_at DESC
LIMIT 20;
```

## Scope

- **v1:** compute + store only (no Scientist consumption).
- **v1.1 (deferred):** DNA features + soft fitness weights.

## Benchmark

Default `bots.stock_personality.benchmark_symbol: '90001'` (TASI-style index in `market_data.indices` / ohlcv). Change if your index symbol differs.
