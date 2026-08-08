# Phase 5 — /analyze + Active Monitoring + Paper

## Delivered

| Item | Path |
|------|------|
| Migration | `migrations/010_active_monitoring_paper.sql` |
| Service | `scripts/analyze_service.py` |
| Telegram handlers | `scripts/telegram_phase5.py` |
| Monitor hook | `bots/monitor/bot.py` → `check_phase5_active` |

## Commands

- `/analyze SYMBOL`
- `/watchlist` `/watching` `/unwatch SYMBOL`
- `/paper SYMBOL` `/paper_close SYMBOL` `/papers`

## Deploy

```bash
git pull origin master
docker compose exec -T postgres psql -U alpha_user -d alpha_engine < migrations/010_active_monitoring_paper.sql
docker compose restart celery_worker celery_beat
# restart telegram bot process/container if separate
```

## Note

Paper trades are simulated only — no live broker orders.
