# Telegram Setup

Need OpenClaw help first? Start with the [OpenClaw setup guide in Skool Classroom](https://www.skool.com/ai-agents-openclaw/classroom). If you need live help while wiring Telegram, use the [Skool community](https://www.skool.com/ai-agents-openclaw).

This plugin works best when it owns one dedicated Telegram route.

## Step 1. Create the bot in BotFather

1. Open BotFather.
2. Create a new bot.
3. Copy the bot token and keep it somewhere safe on the OpenClaw host.
4. **Disable privacy mode** for that bot.
5. In BotFather, run:

```text
/setprivacy
```

Then:

- choose your bot
- choose **`Disable`**

**Important:** privacy mode must be disabled or ordinary topic-root text will not reliably reach the ads workflow.

## Step 2. Create the Telegram destination

Recommended setup:

1. Create a Telegram **supergroup**.
2. Enable **Topics**.
3. Create one topic only for the ads workflow.
4. Add the bot to the group.
5. Promote the bot to admin.

If you run this inside a forum topic, give the bot **admin rights with topic-management permissions** on that forum.

This is the most reliable shape for this plugin:

- one dedicated supergroup
- one dedicated ads topic
- one bot with privacy disabled
- one bot admin that can manage topics

## Step 3. Send one plain test message in the ads topic

Send one simple manual message like `hello` inside the exact topic you want this plugin to own.

That creates a fresh Telegram update containing the IDs you need.

## Step 4. Read the two values you must keep

Call:

```bash
curl "https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates"
```

Find the latest message you just sent in that ads topic and copy:

- **`message.chat.id`** -> this becomes **`telegramChatId`**
- **`message.message_thread_id`** -> this becomes **`telegramThreadId`**

If you run the workflow in a normal group without topics, you only need **`message.chat.id`**.

If you do not see your latest topic message yet, send one more plain message in the topic and call `getUpdates` again.

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

If the bot does not answer, check these in order:

1. **Privacy is really disabled** for that bot
2. **The bot is really an admin** in the same group/topic you are testing
3. **`telegramChatId`** matches the same group
4. **`telegramThreadId`** matches the same topic
5. OpenClaw was restarted after the config change
