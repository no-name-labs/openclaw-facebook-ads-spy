# JOIN OUR COMMUNITY ON SKOOL: [https://www.skool.com/ai-agents-openclaw](https://www.skool.com/ai-agents-openclaw)

OpenClaw deployment help lives in the Skool community. This repository only explains how to install the Facebook Ads plugin into an existing OpenClaw runtime.

# OpenClaw Facebook Ads Spy

OpenClaw Facebook Ads Spy is a Telegram-first investigative plugin for Facebook Ads Library research. It helps affiliate marketers, operators, and growth teams move from a keyword to a live advertiser, landing page, redirect path, and funnel clues in one thread.

It is designed to fit naturally into OpenClaw as a plugin, not as a separate bot.

## Why people use it

- Search the US Facebook Ads Library from Telegram
- Collapse duplicate-heavy ad runs into grouped cards
- See native media directly in the thread when Telegram accepts it
- Pivot fast with `/ads page ...`, `/ads domain ...`, and `/ads inspect ...`
- Reply `page`, `domain`, `inspect`, or `next 10` to keep digging without restarting
- Inspect landing, final, and browser-observed URLs
- Spot redirect behavior and delivery divergence hints
- Capture landing page screenshots when feasible
- Surface technology and tracker hints
- Compare the current card against the current pivot bucket for LP, overlap, stack, delivery, and redirect patterns

## Runtime economics

This plugin is tested inside OpenClaw with the OpenRouter model `openrouter/google/gemini-3.1-flash-lite-preview`.

That gives you a low-cost, high-throughput agent shell around the plugin, while the Facebook Ads acquisition and inspect path stays deterministic instead of guessing its way through the data.

## Requirements

- An existing OpenClaw installation
- A Telegram bot already connected to OpenClaw
- An OpenRouter key wired into OpenClaw
- Python 3 and Node already available on the OpenClaw host
- Playwright Chromium installed on the host if you want screenshot-backed `/ads inspect ...`

## Fastest install for existing OpenClaw users

```bash
git clone https://github.com/no-name-labs/openclaw-facebook-ads-spy.git
cd openclaw-facebook-ads-spy
./scripts/install-inspect-deps.sh
openclaw plugins install "$(pwd)"
openclaw plugins enable facebook-ads-us
```

Then:

1. Merge [`examples/openclaw-plugin-config.json`](examples/openclaw-plugin-config.json) into your `openclaw.json`.
2. Restart OpenClaw.
3. Send `/ads auto insurance` in your Telegram ads topic.

## What this plugin adds to OpenClaw

- Plugin id: `facebook-ads-us`
- Telegram-first workflow for `/ads ...` search, pivots, and inspect
- No separate poller, no separate bot runtime, no ad warehouse
- Ephemeral SQLite session state only

## Telegram setup

Read [docs/TELEGRAM_SETUP.md](docs/TELEGRAM_SETUP.md).

You will need:

- a Telegram bot token
- a supergroup with Topics enabled
- the group `telegramChatId`
- the ads topic `telegramThreadId`
- BotFather privacy disabled
- admin rights for the bot on the forum if you run this in a topic

## OpenRouter setup

Read [docs/OPENROUTER_SETUP.md](docs/OPENROUTER_SETUP.md).

Recommended model:

- `openrouter/google/gemini-3.1-flash-lite-preview`

## Inspect screenshots

`/ads inspect ...` can capture landing page screenshots, but only if Playwright Chromium is available on the host.

Install that dependency with:

```bash
./scripts/install-inspect-deps.sh
```

## Example plugin config

See [examples/openclaw-plugin-config.json](examples/openclaw-plugin-config.json).

The example covers the normal install path. Legacy benchmark/reference fields still exist in the plugin schema, but they are optional and not required for normal operator use.

## Scope

This public repository is the installable distribution package.

The private development source of truth stays separate. OpenClaw deployment tutorials, host bootstrap walkthroughs, and advanced operator support live in the Skool community:

- [https://www.skool.com/ai-agents-openclaw](https://www.skool.com/ai-agents-openclaw)
