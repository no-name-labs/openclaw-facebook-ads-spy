# Telegram Setup

Need OpenClaw help first? Start with the [OpenClaw setup guide in Skool Classroom](https://www.skool.com/ai-agents-openclaw/classroom). If you need live help while wiring Telegram, use the [Skool community](https://www.skool.com/ai-agents-openclaw).

This plugin works best when it owns one dedicated Telegram route.

## Step 1. Create the bot in BotFather

1. Open BotFather.
2. Create a new bot.
3. Copy the bot token.
4. **Disable privacy mode** for that bot.

**Important:** privacy mode must be disabled or ordinary topic-root text will not reliably reach the ads workflow.

## Step 2. Create the Telegram destination

Recommended setup:

1. Create a Telegram **supergroup**.
2. Enable **Topics**.
3. Create one topic only for the ads workflow.
4. Add the bot to the group.
5. Promote the bot to admin.

If you run this inside a forum topic, give the bot **admin rights with topic-management permissions** on that forum.

## Step 3. Send one test message in the ads topic

Send one manual message inside the exact topic you want this plugin to own.

That creates a fresh Telegram update containing the IDs you need.

## Step 4. Read the two values you must keep

Call:

```bash
curl "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates"
```

Find the message you just sent and copy:

- **`message.chat.id`** -> this becomes **`telegramChatId`**
- **`message.message_thread_id`** -> this becomes **`telegramThreadId`**

If you run the workflow in a normal group without topics, you only need **`message.chat.id`**.

## Step 5. Put those values into the plugin config

The normal values are:

- **`telegramAccountId`** = `default`
- **`telegramChatId`** = your group id
- **`telegramThreadId`** = your topic id

See [../examples/openclaw-plugin-config.json](../examples/openclaw-plugin-config.json).

## Step 6. Keep this route plugin-owned

Use one dedicated group or one dedicated topic for ads work.

That keeps your OpenClaw general assistant from competing with this plugin for the same messages.

## Step 7. Restart and test

Restart OpenClaw after the config change, then send:

```text
/ads auto insurance
```

inside the dedicated ads topic.
