# GAMMAX Pro V6.1 — 30-second OI Sniper compatible backend

This source is the supplied GAMMAX Pro V6.1 backend with a multi-timeframe live tick candle aggregator.

## Deployment
1. Upload the files at the project root to GitHub.
2. Render: Build command `npm install`; Start command `npm start`.
3. Copy `.env.example` to Render Environment and provide the Dhan credentials.
4. Verify `/api/health`, `/api/option-chain`, `/api/instruments`, and `/api/history?tf=30s`.

## Candle API
- `GET /api/history?segment=NSE_FNO&securityId=<future-security-id>&tf=30s`
- `GET /api/candles?segment=NSE_FNO&securityId=<future-security-id>&tf=30s&limit=2000`
- Supported: `30s`, `1m`, `3m`, `5m`, `15m`, `30m`, `60m`.
- Candles are aggregated from received Dhan live ticks. 30-second history starts after the backend receives ticks following deployment/restart.
- This V6.1 source does not include V5.2 persistent one-second snapshot history, so it cannot backfill old 30-second candles after restart.

## Option-chain data
The existing `/api/option-chain` endpoint returns CE/PE rows with strike, LTP, OI, Change in OI, volume and related fields. The PWA aggregates the nearest 10 strikes above and below ATM.

## Environment
Set `CANDLE_TIMEFRAME_MS=30000` and include `30000` in `SUPPORTED_TIMEFRAMES`. This controls the default candle series; all supported series are built in parallel from the tick feed.

## Important
This is a market-data and signal backend. It does not place orders. Keep Dhan credentials server-side and never commit `.env`.

## Signal push notification
- Registered PWA subscriptions receive a server push when the live page confirms a new entry and posts it to `/api/push/signal`. This is a notification path, not independent backend-side strategy calculation: the PWA must be running to detect the entry. The in-page three-pulse alarm also requires the PWA to be foregrounded and sound enabled.


## Backup role (GAMMAX V6.1)

This package is configured as the standby backend for the OI Sniper dual-backend PWA.

- Keep `FAILOVER_STANDBY=true` on this Render service.
- The PWA calls `POST /api/failover/activate` when V5.2 is unavailable or its Dhan connection fails during market hours.
- To run V6.1 as a standalone primary, explicitly set `FAILOVER_STANDBY=false`.
- `/api/health` reports auth, WebSocket, packet, tick and subscription diagnostics.
- Keep V5.2 as the primary and avoid two simultaneous Dhan feed connections unless you have planned the Dhan connection quota.

Both deployments must have the same Dhan account credentials configured as Render environment secrets (`DHAN_CLIENT_ID`, `DHAN_PIN`, `DHAN_TOTP_SECRET`) and the required API settings. The backup should remain in standby and will generate/refresh its own access token only after activation. Never put these credentials in the PWA or commit them to Git.
