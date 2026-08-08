# Phase 4 — Watchlist + Morning Telegram (implemented)

## Delivered

| Item | Path |
|------|------|
| Migration | `migrations/009_daily_watchlist.sql` |
| Bot | `bots/watchlist_generator/` |
| Schedule | 07:30 Asia/Riyadh |
| Config | `bots.watchlist_generator` |

## Score inputs

- Genetic fitness (optional, 35%)
- Personality phase (25%)
- Features momentum (25%)
- Sentiment soft weight (15%; no_news does not punish)

## Deploy

```bash
git pull origin master
docker compose exec -T postgres psql -U alpha_user -d alpha_engine < migrations/009_daily_watchlist.sql
docker compose restart celery_worker celery_beat
```

## Manual run

```bash
docker compose exec celery_worker celery -A scripts.celery_app call bots.watchlist_generator.tasks.run_watchlist_generator --kwargs='{"send_telegram":true}'
```
