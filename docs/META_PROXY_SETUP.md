# Residential Meta Proxy Setup

This plugin can route **Meta-facing acquisition requests only** through a residential HTTP or HTTPS proxy.

That means:

- Facebook/Meta requests can use the proxy
- Telegram transport stays direct
- OpenRouter traffic stays direct
- normal OpenClaw traffic stays direct

This is useful when Facebook challenge behavior or rate limits make direct acquisition unreliable.

## Supported proxy types

Use:

- `http`
- `https`

Do not use `socks5` in this build.

## Required environment variables

```bash
export FACEBOOK_ADS_META_PROXY_ENABLED=1
export FACEBOOK_ADS_META_PROXY_SCHEME=http
export FACEBOOK_ADS_META_PROXY_HOST=your.proxy.host
export FACEBOOK_ADS_META_PROXY_PORT=12345
export FACEBOOK_ADS_META_PROXY_USERNAME=your_username
export FACEBOOK_ADS_META_PROXY_PASSWORD=your_password
```

Username and password are optional only if your provider truly gives you an open proxy endpoint.

## What to set in OpenClaw

Nothing goes into the plugin config for proxy credentials.

These values are host environment variables for the deterministic Python runtime.

## Restart after setting them

After exporting the variables, restart OpenClaw so the plugin picks them up on the next run.

## Recommended use

Start without the proxy if your direct acquisition is stable.

Turn it on if you see:

- repeated Facebook challenge behavior
- acquisition instability on normal searches
- better results with residential routing in your own environment
