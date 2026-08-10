# Phase 7 — Weekly Review + Self Trainer

## Delivered

| Item | Path |
|------|------|
| Migration | `migrations/012_weekly_reviews.sql` |
| Reviewer | `bots/weekly_reviewer/bot.py` |
| Trainer v1 | `bots/self_trainer/bot.py` |
| Schedule | Sunday 18:00 Asia/Riyadh |

## Behavior

1. Load closed `paper_trades` from last 7 days.
2. Compute win_rate, avg_pnl, best/worst symbols.
3. Build recommendations for weak symbols.
4. Call SelfTrainer v1 (`auto_apply=false` by default → suggestions only).
5. Save `analytics.weekly_reviews` and send Telegram once per `week_id`.

## Deploy

```bash
git pull origin master
docker compose exec -T postgres psql -U alpha_user -d alpha_engine < migrations/012_weekly_reviews.sql
docker compose restart celery_worker celery_beat
```

## Manual run

```bash
docker compose exec celery_worker python -c "
from bots.weekly_reviewer.bot import WeeklyReviewerBot
print(WeeklyReviewerBot().run(send_telegram=True, force=True))
"
```

## Config

```yaml
bots:
  weekly_reviewer:
    enabled: true
    min_trades_for_symbol: 3
    weak_win_rate: 0.40
  self_trainer:
    enabled: true
    auto_apply: false
```
