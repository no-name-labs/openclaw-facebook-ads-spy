# OpenRouter Setup

This plugin runs inside OpenClaw, so the model is configured in OpenClaw rather than inside the plugin.

## Step 1. Create an OpenRouter API key

1. Open your OpenRouter account.
2. Create an API key.
3. Store it on the OpenClaw host where your OpenClaw runtime can read it.

If your OpenClaw deployment reads provider credentials from environment variables, use:

```bash
export OPENROUTER_API_KEY="sk-or-..."
```

## Step 2. Point OpenClaw at OpenRouter

Use:

- **Base URL:** `https://openrouter.ai/api/v1`
- **Primary model:** `openrouter/google/gemini-3.1-flash-lite-preview`

Fallback models are optional. They are not required for this plugin package.

## Step 3. Restart OpenClaw

Restart OpenClaw after wiring the key and model so the Telegram route and plugin share the same live runtime.

If the plugin does not answer after setup, confirm OpenClaw itself can see the OpenRouter key before you debug the plugin.

## Why this model

This plugin has been tested with **`openrouter/google/gemini-3.1-flash-lite-preview`** as the surrounding OpenClaw shell.

The Facebook Ads acquisition and inspect path still stays deterministic. That is intentional: the model handles the OpenClaw shell, while the ad research path avoids LLM guesswork.
