# OpenRouter Setup

This plugin runs inside OpenClaw, so the model is configured in OpenClaw rather than in the plugin itself.

## Recommended model

Use:

- `openrouter/google/gemini-3.1-flash-lite-preview`

That is the model this plugin has been tested with in the current OpenClaw runtime.

## 1. Create an OpenRouter API key

1. Open your OpenRouter account.
2. Create an API key.
3. Store it on the OpenClaw host.

If your OpenClaw deployment reads provider credentials from environment variables, use:

```bash
export OPENROUTER_API_KEY="sk-or-..."
```

## 2. Point OpenClaw at OpenRouter

Your OpenClaw model config should use:

- base URL: `https://openrouter.ai/api/v1`
- primary model: `openrouter/google/gemini-3.1-flash-lite-preview`

Fallback models are optional. They are not required for this plugin package.

## 3. Restart OpenClaw

After wiring the key and model, restart OpenClaw so the Telegram route and the plugin share the same live runtime.

## Important note

The OpenClaw agent shell can use OpenRouter, but the Facebook Ads acquisition and inspect path in this plugin stays deterministic.

That is intentional: the model handles the surrounding OpenClaw shell, while the ad search/pivot/inspect path avoids LLM guesswork.
