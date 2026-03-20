# Telegram Setup

This plugin expects an existing OpenClaw Telegram account and a dedicated Telegram place to run the ads workflow.

## 1. Create a bot in BotFather

1. Open BotFather.
2. Create a new bot.
3. Copy the bot token.
4. Disable privacy mode for that bot.

Privacy mode must be disabled or ordinary topic-root text will not reliably reach the ads workflow.

## 2. Create the Telegram destination

Recommended setup:

1. Create a Telegram **supergroup**.
2. Enable **Topics** in that supergroup.
3. Create one topic for the Facebook Ads workflow.
4. Add your bot to the group.
5. Promote the bot to admin.

If you run the workflow inside a forum topic, give the bot rights that include topic management on that forum.

## 3. Send one message in the ads topic

Post a test message in the exact topic where you want the plugin to run.

That gives Telegram a fresh update containing the values you need.

## 4. Get `telegramChatId` and `telegramThreadId`

Use your bot token and call:

```bash
curl "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates"
```

Find the message you just sent in the ads topic and note:

- `message.chat.id` -> this becomes `telegramChatId`
- `message.message_thread_id` -> this becomes `telegramThreadId`

If you run the workflow in a normal group without topics, you only need `message.chat.id`.

## 5. Put the values into the plugin config

Use:

- `telegramAccountId`: the OpenClaw Telegram account name, usually `default`
- `telegramChatId`: the group id
- `telegramThreadId`: the topic id

See [../examples/openclaw-plugin-config.json](../examples/openclaw-plugin-config.json).

## 6. Keep the route plugin-owned

This plugin is meant to own its Telegram route.

Use one dedicated group or one dedicated topic for ads work so your OpenClaw general assistant does not compete with it for the same messages.
