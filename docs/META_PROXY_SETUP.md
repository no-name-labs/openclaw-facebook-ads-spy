# Residential Meta Proxy Setup

This is an **optional advanced reliability step**.

Use it only if your Meta acquisition is unstable or challenge-heavy.

## What this proxy does

It routes **Meta-facing acquisition requests only** through a residential HTTP or HTTPS proxy.

It does **not** proxy:

- Telegram transport
- OpenRouter traffic
- normal OpenClaw traffic

## Step 1. Use the right proxy type

Supported:

- `http`
- `https`

Do **not** use `socks5` in this build.

## Step 2. Export the proxy environment variables

```bash
export FACEBOOK_ADS_META_PROXY_ENABLED=1
export FACEBOOK_ADS_META_PROXY_SCHEME=http
export FACEBOOK_ADS_META_PROXY_HOST=your.proxy.host
export FACEBOOK_ADS_META_PROXY_PORT=12345
export FACEBOOK_ADS_META_PROXY_USERNAME=your_username
export FACEBOOK_ADS_META_PROXY_PASSWORD=your_password
```

`FACEBOOK_ADS_META_PROXY_USERNAME` and `FACEBOOK_ADS_META_PROXY_PASSWORD` are optional only if your provider truly gives you an open proxy endpoint.

## Step 3. Keep credentials out of plugin config

Do **not** put proxy credentials into the plugin config JSON.

These are host environment variables for the deterministic Python runtime.

## Step 4. Restart OpenClaw

Restart OpenClaw after exporting the variables so the plugin picks them up.

## When to turn it on

Use it when you see:

- repeated Facebook challenge behavior
- unstable acquisition on normal searches
- better results with residential routing in your own environment
