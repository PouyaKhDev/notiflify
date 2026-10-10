# notiflify 🚨

Automated multi-timeframe **Binance USDT-M Perpetuals Blow-Off Top & Parabolic Short Scanner**.

## Overview
Scans the Binance Futures market every 5 minutes to detect overextended coins exhibiting climactic volume blow-offs and structural exhaustion:
- **Universe Filter**: 24h pump $\ge +15.0\%$ with liquid turnover $\ge \$10,000,000\text{ USDT}$ (memes included, stablecoins excluded).
- **Volume Climax**: Identifies volume spikes $\ge 3.0\times$ 20-period volume SMA on 1H or 15M candles.
- **RSI Overextension**: 1H or 15M RSI $\ge 70.0$.
- **Rejection Analysis**: Computes 1H and 15M upper rejection wick ratios and displays them for confirmation.
- **Funding Context**: Tracks real-time funding rates and exact countdown to the next settlement snapshot.
- **Telegram Alerts**: Instantly broadcasts detected setups to Telegram.

## GitHub Actions Automation
This repository runs automatically every 5 minutes via GitHub Actions (`.github/workflows/scanner.yml`).

### Setup GitHub Secrets (Optional if blowoff.conf.env is configured)
Go to **Settings** $\rightarrow$ **Secrets and variables** $\rightarrow$ **Actions**:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## Local / Manual Run
```bash
pip install -r requirements.txt
python main.py --once
```
