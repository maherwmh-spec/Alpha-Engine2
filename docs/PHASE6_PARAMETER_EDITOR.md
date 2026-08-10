# Phase 6 — Parameter Editor

## Delivered

| Item | Path |
|------|------|
| Migration | `migrations/011_parameter_overrides.sql` |
| Service | `scripts/parameter_editor.py` |
| Telegram | `scripts/telegram_phase6.py` |
| Integration | `scripts/analyze_service.py` (paper + report) |

## Commands

```
/params
/strategy 1120
/update_strategy 1120 stop_loss_pct=0.02 take_profit_pct=0.05
/update_strategy 1120 objective=short_swings
/update_strategy 1120 reset
/strategies
```

## Allowed keys

- `stop_loss_pct` 0.005–0.15
- `take_profit_pct` 0.005–0.30
- `min_confidence` 0.3–0.99
- `alert_cooldown_minutes` 5–240
- `enabled` bool
- `objective` scalping|short_swings|medium_trends|momentum
- `trailing` bool

## Priority

`symbol override` → `global override` → `config defaults`

## Deploy

```bash
git pull origin master
docker compose exec -T postgres psql -U alpha_user -d alpha_engine < migrations/011_parameter_overrides.sql
docker compose restart celery_worker celery_beat
# restart telegram process/container
```
