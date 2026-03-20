import { spawn } from "node:child_process";
import os from "node:os";
import { mkdir, mkdtemp, readFile, rm, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const PYTHON = process.env.PYTHON_BIN || "python3";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const RUNTIME_PATH = path.join(__dirname, "backend", "facebook_ads_runtime.py");
const DEFAULT_ACTION_TIMEOUT_MS = 180000;
const SEARCH_TIMEOUT_MS = 600000;
const TELEGRAM_SEND_DELAY_MS = 750;
const TELEGRAM_SEND_MAX_RETRIES = 2;
const TELEGRAM_SEND_MAX_RETRY_DELAY_MS = 20000;
const ADS_WORKING_STATUS_MAX_RETRY_DELAY_MS = 30000;
const MEDIA_PROBE_TIMEOUT_MS = 15000;
const MEDIA_DOWNLOAD_TIMEOUT_MS = 45000;
const TELEGRAM_BOT_VIDEO_MAX_BYTES = 50 * 1024 * 1024;
const SILENT_REPLY_TOKEN = "NO_REPLY";
const ADS_QUEUED_STATUS_AGE_MS = 8000;
const ADS_TOPIC_HELP_TEXT = [
  "Facebook Ads topic commands:",
  '- Search: `/ads auto insurance` or `auto insurance for past 3 days`',
  '- Pivots: `/ads page 123456789`, `/ads domain example.com`',
  '- Inspect: `/ads inspect https://example.com` or reply `inspect` to a current grouped card',
  '- Reply `page`, `domain`, or `next 10` to the current bot messages',
].join("\n");
const ADS_TOPIC_HELP_MARKERS = new Set([
  "help",
  "usage",
  "what can you do",
  "what can i search",
  "how do i use this",
  "how to use this",
]);
const ADS_TOPIC_GREETINGS = new Set([
  "hi",
  "hello",
  "hey",
  "yo",
  "good morning",
  "good afternoon",
  "good evening",
  "thanks",
  "thank you",
  "ok",
  "okay",
  "cool",
]);
const ADS_TOPIC_QUESTION_PREFIXES = [
  "what ",
  "how ",
  "why ",
  "who ",
  "when ",
  "where ",
  "can you ",
  "could you ",
  "would you ",
  "do you ",
  "are you ",
  "should i ",
];
const ADS_TOPIC_RESERVED_PREFIXES = ["page ", "domain ", "inspect ", "advertiser "];
const URL_ONLY_PATTERN = /^(?:https?:\/\/|www\.)\S+$/i;
const ADS_WORKING_STATUS_TEXT = {
  search: "Usually around 1-3 minutes. I’ll reply in this thread when it’s ready.",
  pivot: "Usually around 30-90 seconds. I’ll reply in this thread when it’s ready.",
  inspect: "Usually around 30-90 seconds. I’ll reply in this thread when it’s ready.",
  next_10: "Usually around 20-60 seconds. I’ll reply in this thread when it’s ready.",
} as const;

type JsonMap = Record<string, unknown>;
type ActionResult = JsonMap & {
  ok?: boolean;
  status?: string;
  summary?: string;
  messages?: Array<Record<string, unknown>>;
};

type TelegramRoute = {
  accountId: string;
  chatId: string;
  threadId?: number;
  scopedChatId: string;
};

type TelegramSendOptions = {
  accountId: string;
  replyToMessageId?: number;
  messageThreadId?: number;
};

type TelegramBotApiRequest = {
  method: "sendMessage" | "sendPhoto" | "sendVideo" | "sendDocument";
  payload: Record<string, unknown>;
};

type MediaProbeStatus = "ok" | "expired_or_unfetchable" | "fetch_failed";

type MediaUrlClass = "signed_meta_o1_v_t2" | "signed_meta_v_t42_1790_2" | "other";

type AdsWorkingTaskClass = keyof typeof ADS_WORKING_STATUS_TEXT;

type AdsTaskPreview = {
  taskClass: AdsWorkingTaskClass;
  taskLabel: string;
};

type AdsInFlightTask = {
  token: symbol;
  taskClass: AdsWorkingTaskClass;
  taskLabel: string;
};

type MediaProbeDetails = {
  status: MediaProbeStatus;
  contentType: string;
  sizeBytes: number | null;
  urlClass: MediaUrlClass;
  uploadFirstRecommended: boolean;
  tooLargeForTelegram: boolean;
};

let cachedBotUsername: string | null = null;
let botUsernamePromise: Promise<string> | null = null;
const sharedAdsInFlightTasks = new Map<string, AdsInFlightTask>();
const adsInFlightTasksByApi = new WeakMap<object, Map<string, AdsInFlightTask>>();

function safeText(input: unknown): string {
  if (typeof input === "string") {
    return input;
  }
  if (typeof input === "number" && Number.isFinite(input)) {
    return String(input);
  }
  if (typeof input === "bigint") {
    return input.toString();
  }
  return "";
}

function maybeNumber(input: unknown): number | undefined {
  if (typeof input === "number" && Number.isFinite(input)) {
    return input;
  }
  if (typeof input === "string" && input.trim()) {
    const parsed = Number.parseInt(input.trim(), 10);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return undefined;
}

function maybeTimestampMs(input: unknown): number | undefined {
  if (input instanceof Date && Number.isFinite(input.getTime())) {
    return input.getTime();
  }
  if (typeof input === "number" && Number.isFinite(input)) {
    return input < 1_000_000_000_000 ? input * 1000 : input;
  }
  if (typeof input === "string") {
    const trimmed = input.trim();
    if (!trimmed) {
      return undefined;
    }
    const numeric = Number(trimmed);
    if (Number.isFinite(numeric)) {
      return numeric < 1_000_000_000_000 ? numeric * 1000 : numeric;
    }
    const parsed = Date.parse(trimmed);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return undefined;
}

function normalizeTelegramTarget(input: unknown): string {
  const raw = safeText(input).trim();
  if (!raw) {
    return "";
  }
  const stripped = raw.replace(/^telegram:/i, "").trim();
  const numeric = stripped.match(/-?\d{3,}/);
  if (numeric) {
    return numeric[0];
  }
  return stripped;
}

function parseTelegramScope(input: unknown): { chatId: string; threadId?: number } {
  const raw = safeText(input).trim();
  if (!raw) {
    return { chatId: "" };
  }
  const stripped = raw.replace(/^telegram:/i, "").replace(/^group:/i, "").trim();
  const match = stripped.match(/(-?\d{3,})(?::topic:(\d+))?/);
  if (match) {
    return {
      chatId: match[1] || "",
      threadId: maybeNumber(match[2]),
    };
  }
  return { chatId: normalizeTelegramTarget(stripped) };
}

function scopedTelegramChatId(chatId: string, threadId?: number): string {
  return threadId === undefined ? chatId : `${chatId}:topic:${threadId}`;
}

function pluginConfig(api: any): JsonMap {
  if (api?.pluginConfig && typeof api.pluginConfig === "object" && !Array.isArray(api.pluginConfig)) {
    return api.pluginConfig as JsonMap;
  }
  if (api?.config && typeof api.config === "object" && !Array.isArray(api.config)) {
    return api.config as JsonMap;
  }
  return {};
}

function configuredTelegramRoute(api: any): TelegramRoute | null {
  const config = pluginConfig(api);
  const chatId = normalizeTelegramTarget(config.telegramChatId);
  if (!chatId) {
    return null;
  }
  const threadId = maybeNumber(config.telegramThreadId);
  const accountId = safeText(config.telegramAccountId).trim() || "default";
  return {
    accountId,
    chatId,
    threadId,
    scopedChatId: scopedTelegramChatId(chatId, threadId),
  };
}

export function routeMatchesTelegramContext(api: any, ctx: any): TelegramRoute | null {
  const route = configuredTelegramRoute(api);
  if (!route) {
    return null;
  }
  const actualAccountId = safeText(ctx?.accountId).trim();
  if (actualAccountId && actualAccountId !== route.accountId) {
    return null;
  }
  const parsedScopes = [
    ctx?.groupId,
    ctx?.conversationId,
    ctx?.to,
    ctx?.chatId,
    ctx?.chat_id,
    ctx?.channelData?.groupId,
    ctx?.channelData?.conversationId,
    ctx?.channelData?.to,
    ctx?.channelData?.chatId,
    ctx?.channelData?.chat_id,
    ctx?.channelData?.telegram?.groupId,
    ctx?.channelData?.telegram?.conversationId,
    ctx?.channelData?.telegram?.to,
    ctx?.channelData?.telegram?.chatId,
    ctx?.channelData?.telegram?.chat_id,
    ctx?.message?.chat?.id,
    ctx?.message?.chatId,
    ctx?.message?.chat_id,
    ctx?.rawMessage?.chat?.id,
    ctx?.rawMessage?.chatId,
    ctx?.rawMessage?.chat_id,
    ctx?.raw?.message?.chat?.id,
    ctx?.raw?.message?.chatId,
    ctx?.raw?.message?.chat_id,
  ]
    .map((value) => parseTelegramScope(value))
    .filter((scope) => scope.chatId);
  const parsed = parsedScopes.find((scope) => scope.chatId === route.chatId) || parsedScopes[0] || { chatId: "" };
  const actualChatId = parsed.chatId || normalizeTelegramTarget(ctx?.to);
  const actualThreadId =
    maybeNumber(ctx?.messageThreadId)
    ?? maybeNumber(ctx?.message_thread_id)
    ?? maybeNumber(ctx?.channelData?.messageThreadId)
    ?? maybeNumber(ctx?.channelData?.message_thread_id)
    ?? maybeNumber(ctx?.channelData?.telegram?.messageThreadId)
    ?? maybeNumber(ctx?.channelData?.telegram?.message_thread_id)
    ?? maybeNumber(ctx?.message?.messageThreadId)
    ?? maybeNumber(ctx?.message?.message_thread_id)
    ?? maybeNumber(ctx?.rawMessage?.messageThreadId)
    ?? maybeNumber(ctx?.rawMessage?.message_thread_id)
    ?? maybeNumber(ctx?.raw?.message?.messageThreadId)
    ?? maybeNumber(ctx?.raw?.message?.message_thread_id)
    ?? parsed.threadId;
  if (!actualChatId || actualChatId !== route.chatId) {
    return null;
  }
  if (route.threadId !== undefined && actualThreadId !== undefined && actualThreadId !== route.threadId) {
    return null;
  }
  return route;
}

function normalizeText(input: string): string {
  return input.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function actionTimeoutMs(action: string): number {
  if (action === "run_ads_command" || action === "search_ads" || action === "get_next_page" || action === "ads_health_check") {
    return SEARCH_TIMEOUT_MS;
  }
  return DEFAULT_ACTION_TIMEOUT_MS;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function telegramRetryDelayMs(error: unknown): number | undefined {
  const message = String(error || "");
  const match = message.match(/retry after (\d+)/i);
  if (!match) {
    return undefined;
  }
  const seconds = Number.parseInt(match[1] || "", 10);
  if (!Number.isFinite(seconds)) {
    return undefined;
  }
  return Math.max(1, seconds + 1) * 1000;
}

export function allowedTelegramRetryDelayMs(
  retryDelayMs: number | undefined,
  maxRetryDelayMs = TELEGRAM_SEND_MAX_RETRY_DELAY_MS
): number | undefined {
  if (retryDelayMs === undefined) {
    return undefined;
  }
  if (retryDelayMs > maxRetryDelayMs) {
    return undefined;
  }
  return retryDelayMs;
}

export function telegramBotApiRetryDelayMs(status: number, payload: any, fallbackError: unknown): number | undefined {
  const retryAfter = maybeNumber(payload?.parameters?.retry_after);
  if (retryAfter !== undefined) {
    return allowedTelegramRetryDelayMs(Math.max(1, retryAfter + 1) * 1000);
  }
  if (status === 429) {
    return allowedTelegramRetryDelayMs(2000);
  }
  return allowedTelegramRetryDelayMs(telegramRetryDelayMs(fallbackError));
}

function extractTelegramMessageId(result: any): number | undefined {
  return (
    maybeNumber(result?.message_id) ??
    maybeNumber(result?.messageId) ??
    maybeNumber(result?.result?.message_id) ??
    maybeNumber(result?.result?.messageId)
  );
}

function telegramTokenForAccount(api: any, accountId: string): string {
  const resolution = api.runtime.channel.telegram.resolveTelegramToken(api.config, { accountId });
  const token = safeText(resolution?.token).trim();
  if (!token) {
    throw new Error(`Telegram token unavailable for accountId=${accountId}`);
  }
  return token;
}

async function sendTelegramMessageWithRetry(
  api: any,
  chatId: string,
  text: string,
  options: TelegramSendOptions,
  retryOptions?: { maxRetryDelayMs?: number }
): Promise<any> {
  const maxRetryDelayMs = retryOptions?.maxRetryDelayMs ?? TELEGRAM_SEND_MAX_RETRY_DELAY_MS;
  for (let attempt = 0; attempt <= TELEGRAM_SEND_MAX_RETRIES; attempt += 1) {
    try {
      return await api.runtime.channel.telegram.sendMessageTelegram(chatId, text, options);
    } catch (error) {
      const rawRetryDelayMs = telegramRetryDelayMs(error);
      const retryDelayMs = allowedTelegramRetryDelayMs(rawRetryDelayMs, maxRetryDelayMs);
      if (rawRetryDelayMs !== undefined && retryDelayMs === undefined) {
        api.logger.warn(
          `facebook ads telegram send throttled; failing fast because retry-after ${Math.ceil(rawRetryDelayMs / 1000)}s exceeds ${Math.ceil(maxRetryDelayMs / 1000)}s`
        );
      }
      if (retryDelayMs === undefined || attempt >= TELEGRAM_SEND_MAX_RETRIES) {
        throw error;
      }
      api.logger.warn(`facebook ads telegram send throttled; retrying in ${Math.ceil(retryDelayMs / 1000)}s`);
      await sleep(retryDelayMs);
    }
  }
}

async function telegramBotApiRequestWithRetry(
  api: any,
  accountId: string,
  request: TelegramBotApiRequest
): Promise<any> {
  const token = telegramTokenForAccount(api, accountId);
  const url = `https://api.telegram.org/bot${token}/${request.method}`;
  for (let attempt = 0; attempt <= TELEGRAM_SEND_MAX_RETRIES; attempt += 1) {
    let response: Response | undefined;
    let payload: any = null;
    try {
      let requestInit: RequestInit;
      const uploadField =
        request.method === "sendPhoto"
          ? "photo_path"
          : request.method === "sendVideo"
            ? "video_path"
            : request.method === "sendDocument"
              ? "document_path"
              : "";
      const uploadPath = uploadField ? safeText(request.payload[uploadField]).trim() : "";
      if (uploadPath) {
        const mediaField =
          request.method === "sendPhoto" ? "photo" : request.method === "sendVideo" ? "video" : "document";
        const explicitMimeType = inferUploadMimeType(
          uploadPath,
          safeText(request.payload.media_content_type || request.payload.document_mime_type),
          request.method
        );
        const formData = new FormData();
        for (const [key, value] of Object.entries(request.payload)) {
          if (
            key === uploadField ||
            key === "media_content_type" ||
            key === "document_mime_type" ||
            value === undefined ||
            value === null ||
            value === ""
          ) {
            continue;
          }
          formData.set(key, String(value));
        }
        formData.set(
          mediaField,
          new Blob([await readFile(uploadPath)], { type: explicitMimeType || "application/octet-stream" }),
          path.basename(uploadPath)
        );
        requestInit = {
          method: "POST",
          body: formData,
        };
      } else {
        requestInit = {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify(request.payload),
        };
      }
      response = await fetch(url, {
        ...requestInit,
      });
      const bodyText = await response.text();
      payload = bodyText ? JSON.parse(bodyText) : {};
      if (!response.ok || payload?.ok === false) {
        const description = safeText(payload?.description).trim() || `${response.status} ${response.statusText}`.trim();
        const retryDelayMs = telegramBotApiRetryDelayMs(response.status, payload, description);
        const retryAfterSeconds = maybeNumber(payload?.parameters?.retry_after);
        if (retryAfterSeconds !== undefined && retryDelayMs === undefined) {
          api.logger.warn(
            `facebook ads telegram ${request.method} throttled; failing fast because retry-after ${retryAfterSeconds}s exceeds ${Math.ceil(TELEGRAM_SEND_MAX_RETRY_DELAY_MS / 1000)}s`
          );
        }
        if (retryDelayMs !== undefined && attempt < TELEGRAM_SEND_MAX_RETRIES) {
          api.logger.warn(
            `facebook ads telegram ${request.method} throttled; retrying in ${Math.ceil(retryDelayMs / 1000)}s`
          );
          await sleep(retryDelayMs);
          continue;
        }
        throw new Error(`Telegram Bot API ${request.method} failed: ${description}`);
      }
      return payload?.result ?? payload;
    } catch (error) {
      const retryDelayMs = telegramBotApiRetryDelayMs(response?.status ?? 0, payload, error);
      if (retryDelayMs === undefined || attempt >= TELEGRAM_SEND_MAX_RETRIES) {
        throw error;
      }
      api.logger.warn(
        `facebook ads telegram ${request.method} send failed; retrying in ${Math.ceil(retryDelayMs / 1000)}s`
      );
      await sleep(retryDelayMs);
    }
  }
}

async function bindPromptMessage(api: any, searchSessionId: string, promptMessageId: number): Promise<void> {
  const result = await runAdsAction(api, "bind_session_prompt", {
    search_session_id: searchSessionId,
    prompt_message_id: promptMessageId,
  });
  if (result.ok !== true) {
    api.logger.warn(
      `bind_session_prompt failed: session=${searchSessionId} prompt=${promptMessageId} summary=${safeText(result.summary)}`
    );
  }
}

async function bindGroupMessage(api: any, searchSessionId: string, groupKey: string, messageId: number): Promise<void> {
  const result = await runAdsAction(api, "bind_group_message", {
    search_session_id: searchSessionId,
    group_key: groupKey,
    message_id: messageId,
  });
  if (result.ok !== true) {
    api.logger.warn(
      `bind_group_message failed: session=${searchSessionId} group=${groupKey} message=${messageId} summary=${safeText(result.summary)}`
    );
  }
}

async function bindPromptMessageForApi(api: any, searchSessionId: string, promptMessageId: number): Promise<void> {
  const testHook = api?.facebookAdsTestHooks?.bindPromptMessage;
  if (typeof testHook === "function") {
    await testHook(searchSessionId, promptMessageId);
    return;
  }
  await bindPromptMessage(api, searchSessionId, promptMessageId);
}

export function deferredPromptBindingForMessage(
  message: Record<string, unknown>,
  sentMessageId: number | undefined
): { searchSessionId: string; promptMessageId: number } | null {
  const searchSessionId = safeText(message.deferred_prompt_bind_session_id).trim();
  if (!searchSessionId || sentMessageId === undefined) {
    return null;
  }
  return {
    searchSessionId,
    promptMessageId: sentMessageId,
  };
}

export function bindingRequestForMessage(
  message: Record<string, unknown>,
  sentMessageId: number
): { action: "bind_group_message" | "bind_session_prompt"; payload: Record<string, unknown> } | null {
  const searchSessionId = safeText(message.bind_session_id).trim();
  const groupKey = safeText(message.bind_group_key).trim();
  if (searchSessionId && groupKey) {
    return {
      action: "bind_group_message",
      payload: {
        search_session_id: searchSessionId,
        group_key: groupKey,
        message_id: sentMessageId,
      },
    };
  }
  if (searchSessionId) {
    return {
      action: "bind_session_prompt",
      payload: {
        search_session_id: searchSessionId,
        prompt_message_id: sentMessageId,
      },
    };
  }
  return null;
}

async function bindSentMessage(api: any, message: Record<string, unknown>, sentMessageId: number | undefined): Promise<void> {
  if (!sentMessageId) {
    return;
  }
  const binding = bindingRequestForMessage(message, sentMessageId);
  if (!binding) {
    return;
  }
  if (binding.action === "bind_group_message") {
    await bindGroupMessage(
      api,
      safeText(binding.payload.search_session_id).trim(),
      safeText(binding.payload.group_key).trim(),
      maybeNumber(binding.payload.message_id) || sentMessageId
    );
    return;
  }
  await bindPromptMessage(api, safeText(binding.payload.search_session_id).trim(), maybeNumber(binding.payload.prompt_message_id) || sentMessageId);
}

export function telegramMediaMethodForMessage(message: Record<string, unknown>): "sendPhoto" | "sendVideo" | null {
  const mediaKind = safeText(message.media_kind).trim().toLowerCase();
  if (mediaKind === "photo") {
    return "sendPhoto";
  }
  if (mediaKind === "video") {
    return "sendVideo";
  }
  return null;
}

function classifyMediaUrl(mediaUrl: string): MediaUrlClass {
  try {
    const pathname = new URL(mediaUrl).pathname;
    if (pathname.includes("/o1/v/t2/")) {
      return "signed_meta_o1_v_t2";
    }
    if (pathname.includes("/v/t42.1790-2/")) {
      return "signed_meta_v_t42_1790_2";
    }
  } catch {
    return "other";
  }
  return "other";
}

function parseMediaSizeBytes(response: Response): number | null {
  const contentRange = safeText(response.headers.get("content-range")).trim();
  const rangeMatch = contentRange.match(/bytes\s+\d+-\d+\/(\d+)/i);
  if (rangeMatch) {
    const parsed = Number.parseInt(rangeMatch[1] || "", 10);
    if (Number.isFinite(parsed) && parsed >= 0) {
      return parsed;
    }
  }
  const contentLength = safeText(response.headers.get("content-length")).trim();
  if (!contentLength) {
    return null;
  }
  const parsed = Number.parseInt(contentLength, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function inferUploadMimeType(uploadPath: string, explicitMimeType: string, method: TelegramBotApiRequest["method"]): string {
  const normalized = explicitMimeType.trim().toLowerCase();
  const methodExpectsImage = method === "sendPhoto";
  const methodExpectsVideo = method === "sendVideo" || method === "sendDocument";
  if (normalized) {
    if (methodExpectsImage && normalized.startsWith("image/")) {
      return normalized;
    }
    if (methodExpectsVideo && normalized.startsWith("video/")) {
      return normalized;
    }
    if (
      normalized !== "application/octet-stream" &&
      normalized !== "binary/octet-stream" &&
      normalized !== "application/binary"
    ) {
      return normalized;
    }
  }
  const lowerPath = uploadPath.toLowerCase();
  if (lowerPath.endsWith(".mp4")) {
    return "video/mp4";
  }
  if (normalized) {
    return normalized;
  }
  if (lowerPath.endsWith(".png")) {
    return "image/png";
  }
  if (lowerPath.endsWith(".webp")) {
    return "image/webp";
  }
  if (lowerPath.endsWith(".jpg") || lowerPath.endsWith(".jpeg")) {
    return "image/jpeg";
  }
  if (lowerPath.endsWith(".mp4")) {
    return "video/mp4";
  }
  if (method === "sendPhoto") {
    return "image/jpeg";
  }
  if (method === "sendVideo") {
    return "video/mp4";
  }
  return "application/octet-stream";
}

export function buildTelegramNativeMediaRequest(
  chatId: string,
  message: Record<string, unknown>,
  options: TelegramSendOptions
): TelegramBotApiRequest | null {
  const method = telegramMediaMethodForMessage(message);
  const mediaUrl = safeText(message.media_url).trim();
  const mediaPath = safeText(message.media_path).trim();
  if (!method || (!mediaUrl && !mediaPath)) {
    return null;
  }
  const payload: Record<string, unknown> = {
    chat_id: chatId,
    caption: safeText(message.media_caption).trim(),
  };
  if (options.messageThreadId !== undefined) {
    payload.message_thread_id = options.messageThreadId;
  }
  if (options.replyToMessageId !== undefined) {
    payload.reply_to_message_id = options.replyToMessageId;
  }
  if (method === "sendPhoto") {
    if (mediaPath) {
      payload.photo_path = mediaPath;
    } else {
      payload.photo = mediaUrl;
    }
  } else {
    if (mediaPath) {
      payload.video_path = mediaPath;
    } else {
      payload.video = mediaUrl;
    }
    payload.supports_streaming = true;
  }
  return { method, payload };
}

function buildTelegramDocumentRequest(
  chatId: string,
  message: Record<string, unknown>,
  options: TelegramSendOptions
): TelegramBotApiRequest | null {
  const documentPath = safeText(message.media_path).trim();
  if (!documentPath) {
    return null;
  }
  const payload: Record<string, unknown> = {
    chat_id: chatId,
    caption: safeText(message.media_caption).trim(),
    document_path: documentPath,
    document_mime_type: safeText(message.media_content_type).trim(),
  };
  if (options.messageThreadId !== undefined) {
    payload.message_thread_id = options.messageThreadId;
  }
  if (options.replyToMessageId !== undefined) {
    payload.reply_to_message_id = options.replyToMessageId;
  }
  return { method: "sendDocument", payload };
}

export function buildTelegramTextRequest(
  chatId: string,
  message: Record<string, unknown>,
  options: TelegramSendOptions
): TelegramBotApiRequest | null {
  const text = safeText(message.text).trim();
  if (!text || message.disable_web_page_preview !== true) {
    return null;
  }
  const payload: Record<string, unknown> = {
    chat_id: chatId,
    text,
    disable_web_page_preview: true,
  };
  if (options.messageThreadId !== undefined) {
    payload.message_thread_id = options.messageThreadId;
  }
  if (options.replyToMessageId !== undefined) {
    payload.reply_to_message_id = options.replyToMessageId;
  }
  return {
    method: "sendMessage",
    payload,
  };
}

async function sendTelegramTextMessage(
  api: any,
  chatId: string,
  text: string,
  options: TelegramSendOptions,
  message: Record<string, unknown>
): Promise<any> {
  const textRequest = buildTelegramTextRequest(chatId, { ...message, text }, options);
  if (!textRequest) {
    return await sendTelegramMessageWithRetry(api, chatId, text, options);
  }
  return await telegramBotApiRequestWithRetry(api, options.accountId, textRequest);
}

function temporaryMediaRoot(): string {
  return (process.env.FACEBOOK_ADS_TEMP_MEDIA_ROOT || "").trim() || path.join(os.tmpdir(), "facebook-ads-runtime");
}

async function cleanupLocalMediaPath(api: any, message: Record<string, unknown>): Promise<void> {
  const mediaPath = safeText(message.media_path).trim();
  const mediaTempDir = safeText(message.media_temp_dir).trim();
  if (!mediaPath) {
    if (!mediaTempDir) {
      return;
    }
  }
  try {
    if (mediaPath) {
      await unlink(mediaPath);
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException)?.code !== "ENOENT") {
      api.logger.warn(`facebook ads local media cleanup failed for file=${mediaPath}: ${String(error)}`);
    }
  }
  const parentDir = mediaTempDir || path.dirname(mediaPath);
  const tempRoot = temporaryMediaRoot();
  if (!parentDir.startsWith(tempRoot + path.sep)) {
    return;
  }
  try {
    await rm(parentDir, { recursive: true, force: true });
  } catch (error) {
    api.logger.warn(`facebook ads local media cleanup failed for dir=${parentDir}: ${String(error)}`);
  }
}

async function probeMediaUrl(mediaUrl: string): Promise<MediaProbeDetails> {
  const urlClass = classifyMediaUrl(mediaUrl);
  if (!mediaUrl) {
    return {
      status: "fetch_failed",
      contentType: "",
      sizeBytes: null,
      urlClass,
      uploadFirstRecommended: false,
      tooLargeForTelegram: false,
    };
  }
  try {
    const response = await fetch(mediaUrl, {
      method: "GET",
      headers: {
        Range: "bytes=0-0",
        "User-Agent": "facebook-ads-agent/phase1",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(MEDIA_PROBE_TIMEOUT_MS),
    });
    const contentType = safeText(response.headers.get("content-type")).trim().toLowerCase();
    const sizeBytes = parseMediaSizeBytes(response);
    if (response.ok) {
      const tooLargeForTelegram = sizeBytes !== null && sizeBytes > TELEGRAM_BOT_VIDEO_MAX_BYTES;
      const uploadFirstRecommended =
        !tooLargeForTelegram &&
        (urlClass !== "other" || (contentType !== "" && !contentType.startsWith("video/")));
      return {
        status: "ok",
        contentType,
        sizeBytes,
        urlClass,
        uploadFirstRecommended,
        tooLargeForTelegram,
      };
    }
    const status: MediaProbeStatus = [401, 403, 404, 410].includes(response.status)
      ? "expired_or_unfetchable"
      : "fetch_failed";
    return {
      status,
      contentType,
      sizeBytes,
      urlClass,
      uploadFirstRecommended: false,
      tooLargeForTelegram: false,
    };
  } catch {
    return {
      status: "fetch_failed",
      contentType: "",
      sizeBytes: null,
      urlClass,
      uploadFirstRecommended: false,
      tooLargeForTelegram: false,
    };
  }
}

function mediaTempFileExtension(message: Record<string, unknown>, contentType: string, mediaUrl: string): string {
  const normalizedContentType = safeText(contentType).trim().toLowerCase();
  if (normalizedContentType.startsWith("image/")) {
    return normalizedContentType.endsWith("png") ? ".png" : ".jpg";
  }
  if (normalizedContentType.startsWith("video/")) {
    return ".mp4";
  }
  const pathname = (() => {
    try {
      return new URL(mediaUrl).pathname.toLowerCase();
    } catch {
      return "";
    }
  })();
  if (pathname.endsWith(".png")) {
    return ".png";
  }
  if (pathname.endsWith(".jpg") || pathname.endsWith(".jpeg")) {
    return ".jpg";
  }
  if (pathname.endsWith(".webp")) {
    return ".webp";
  }
  if (pathname.endsWith(".mp4")) {
    return ".mp4";
  }
  return telegramMediaMethodForMessage(message) === "sendVideo" ? ".mp4" : ".jpg";
}

async function downloadFetchableMediaToTempPath(
  api: any,
  message: Record<string, unknown>,
  probeDetails?: MediaProbeDetails | null
): Promise<string | null> {
  const mediaUrl = safeText(message.media_url).trim();
  if (!mediaUrl || !telegramMediaMethodForMessage(message)) {
    return null;
  }
  const tempRoot = temporaryMediaRoot();
  await mkdir(tempRoot, { recursive: true });
  const tempDir = await mkdtemp(path.join(tempRoot, "telegram-openclaw-ads-"));
  try {
    const response = await fetch(mediaUrl, {
      method: "GET",
      headers: {
        "User-Agent": "facebook-ads-agent/phase1",
      },
      redirect: "follow",
      signal: AbortSignal.timeout(MEDIA_DOWNLOAD_TIMEOUT_MS),
    });
    if (!response.ok) {
      throw new Error(`media download returned HTTP ${response.status}`);
    }
    const buffer = Buffer.from(await response.arrayBuffer());
    const contentType = safeText(response.headers.get("content-type")).trim().toLowerCase() || safeText(probeDetails?.contentType).trim();
    const extension = mediaTempFileExtension(message, contentType, mediaUrl);
    const mediaPath = path.join(tempDir, `telegram-media${extension}`);
    await writeFile(mediaPath, buffer);
    message.media_content_type = inferUploadMimeType(
      mediaPath,
      contentType,
      telegramMediaMethodForMessage(message) || "sendDocument"
    );
    message.media_size_bytes = buffer.byteLength;
    message.media_temp_dir = tempDir;
    return mediaPath;
  } catch (error) {
    await rm(tempDir, { recursive: true, force: true });
    api.logger.warn(`facebook ads media download retry failed for url=${mediaUrl}: ${String(error)}`);
    return null;
  }
}

function applyMediaProbeDetails(message: Record<string, unknown>, probeDetails: MediaProbeDetails): void {
  message.media_probe_status = probeDetails.status;
  message.media_content_type = probeDetails.contentType;
  message.media_size_bytes = probeDetails.sizeBytes;
  message.media_url_class = probeDetails.urlClass;
}

async function classifyNativeMediaFailure(
  message: Record<string, unknown>,
  probeDetails?: MediaProbeDetails | null
): Promise<string> {
  const mediaUrl = safeText(message.media_url).trim();
  if (!mediaUrl) {
    return "media_present_telegram_rejected";
  }
  const details = probeDetails || (await probeMediaUrl(mediaUrl));
  applyMediaProbeDetails(message, details);
  if (details.status === "ok") {
    return "media_present_telegram_rejected";
  }
  if (details.status === "expired_or_unfetchable") {
    return "media_present_expired_or_unfetchable";
  }
  return "media_present_fetch_failed";
}

function isVideoMediaMessage(message: Record<string, unknown>): boolean {
  return telegramMediaMethodForMessage(message) === "sendVideo";
}

function isPhotoMediaMessage(message: Record<string, unknown>): boolean {
  return telegramMediaMethodForMessage(message) === "sendPhoto";
}

function canAttemptVideoUploadRecovery(
  message: Record<string, unknown>,
  probeDetails: MediaProbeDetails | null | undefined
): boolean {
  if (probeDetails?.status === "expired_or_unfetchable" || probeDetails?.tooLargeForTelegram) {
    return false;
  }
  const sizeBytes = maybeNumber(message.media_size_bytes) ?? probeDetails?.sizeBytes ?? undefined;
  if (sizeBytes !== undefined && sizeBytes > TELEGRAM_BOT_VIDEO_MAX_BYTES) {
    return false;
  }
  return true;
}

function markSuccessfulMediaDelivery(
  message: Record<string, unknown>,
  delivery: {
    outcome: string;
    nativeMediaSent: boolean;
    documentRecovered: boolean;
  }
): void {
  message.media_outcome = delivery.outcome;
  message.text_fallback_used = false;
  message.native_media_sent = delivery.nativeMediaSent;
  message.document_recovered_sent = delivery.documentRecovered;
}

function markFailedMediaDelivery(
  message: Record<string, unknown>,
  delivery: {
    outcome: string;
    fallbackUsed: boolean;
  }
): void {
  message.media_outcome = delivery.outcome;
  message.text_fallback_used = delivery.fallbackUsed;
  message.native_media_sent = false;
  message.document_recovered_sent = false;
}

async function sendMediaRequestAndOptionalText(
  api: any,
  chatId: string,
  message: Record<string, unknown>,
  options: TelegramSendOptions,
  request: TelegramBotApiRequest,
  delivery: {
    outcome: string;
    nativeMediaSent: boolean;
    documentRecovered: boolean;
  }
): Promise<string[]> {
  const nativeMediaText = safeText(message.native_media_text).trim();
  const mediaResult = await telegramBotApiRequestWithRetry(api, options.accountId, request);
  markSuccessfulMediaDelivery(message, delivery);
  await bindSentMessage(api, message, extractTelegramMessageId(mediaResult));
  const sentTexts: string[] = [];
  if (nativeMediaText) {
    await sleep(TELEGRAM_SEND_DELAY_MS);
    const textResult = await sendTelegramTextMessage(api, chatId, nativeMediaText, options, message);
    await bindSentMessage(api, message, extractTelegramMessageId(textResult));
    sentTexts.push(nativeMediaText);
  }
  return sentTexts;
}

async function tryUploadedVideoRecovery(
  api: any,
  chatId: string,
  message: Record<string, unknown>,
  options: TelegramSendOptions,
  deliveryOptions: {
    allowDocumentRetry: boolean;
  }
): Promise<{ sentTexts: string[]; documentRecovered: boolean } | null> {
  const localVideoRequest = buildTelegramNativeMediaRequest(chatId, message, options);
  if (!localVideoRequest) {
    return null;
  }
  try {
    return {
      sentTexts: await sendMediaRequestAndOptionalText(api, chatId, message, options, localVideoRequest, {
        outcome: "media_present_native_sent",
        nativeMediaSent: true,
        documentRecovered: false,
      }),
      documentRecovered: false,
    };
  } catch (localVideoError) {
    if (!deliveryOptions.allowDocumentRetry) {
      throw localVideoError;
    }
    const documentRequest = buildTelegramDocumentRequest(chatId, message, options);
    if (!documentRequest) {
      throw localVideoError;
    }
    try {
      return {
        sentTexts: await sendMediaRequestAndOptionalText(api, chatId, message, options, documentRequest, {
          outcome: "media_present_document_sent",
          nativeMediaSent: false,
          documentRecovered: true,
        }),
        documentRecovered: true,
      };
    } catch (documentError) {
      api.logger.warn(
        `facebook ads document upload recovery failed after local video upload retry. mediaKind=${safeText(message.media_kind)} summary=${String(documentError)}`
      );
      throw localVideoError;
    }
  }
}

function actionEnv(api: any): NodeJS.ProcessEnv {
  const config = pluginConfig(api);
  const env: NodeJS.ProcessEnv = { ...process.env };
  env.FACEBOOK_ADS_PLUGIN_ROOT = __dirname;
  env.FACEBOOK_ADS_SESSION_DB_PATH =
    safeText(config.sessionDbPath).trim() || path.join(__dirname, "data", "facebook_ads_sessions.db");
  env.FACEBOOK_ADS_REQUEST_TIMEOUT_SEC = safeText(config.requestTimeoutSec).trim() || "30";
  env.FACEBOOK_ADS_SESSION_TTL_HOURS = safeText(config.sessionTtlHours).trim() || "12";
  env.FACEBOOK_ADS_REFERENCE_BASE_URL = safeText(config.referenceBaseUrl).trim();
  env.FACEBOOK_ADS_REFERENCE_SEARCH_PATH = safeText(config.referenceSearchPath).trim() || "/search";
  const referenceTokenEnvVar = safeText(config.referenceTokenEnvVar).trim();
  if (referenceTokenEnvVar && process.env[referenceTokenEnvVar]) {
    env.FACEBOOK_ADS_REFERENCE_TOKEN = process.env[referenceTokenEnvVar];
  }
  for (const [configKey, envKey] of [
    ["searchDocId", "FACEBOOK_ADS_SEARCH_DOC_ID"],
    ["detailsDocId", "FACEBOOK_ADS_DETAILS_DOC_ID"],
    ["collationDocId", "FACEBOOK_ADS_COLLATION_DOC_ID"],
    ["aggregateDocId", "FACEBOOK_ADS_AGGREGATE_DOC_ID"],
    ["filterContextDocId", "FACEBOOK_ADS_FILTER_CONTEXT_DOC_ID"],
  ]) {
    const value = safeText(config[configKey]).trim();
    if (value) {
      env[envKey] = value;
    }
  }
  return env;
}

async function runAdsAction(api: any, action: string, payload: JsonMap = {}): Promise<ActionResult> {
  const testHook = api?.facebookAdsTestHooks?.runAdsAction;
  if (typeof testHook === "function") {
    return await testHook(action, payload);
  }
  return await new Promise<ActionResult>((resolve) => {
    const proc = spawn(
      PYTHON,
      [RUNTIME_PATH, "--tool-action", action, "--tool-payload-json", JSON.stringify(payload)],
      {
        env: actionEnv(api),
        stdio: ["ignore", "pipe", "pipe"],
      }
    );

    let stdout = "";
    let stderr = "";
    let settled = false;
    let timedOut = false;
    const timeoutMs = actionTimeoutMs(action);

    const finish = (result: ActionResult): void => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      resolve(result);
    };

    const timer = setTimeout(() => {
      timedOut = true;
      proc.kill("SIGKILL");
    }, timeoutMs);

    proc.stdout?.setEncoding("utf8");
    proc.stderr?.setEncoding("utf8");
    proc.stdout?.on("data", (chunk: string) => {
      stdout += chunk;
    });
    proc.stderr?.on("data", (chunk: string) => {
      stderr += chunk;
    });

    proc.on("error", (error) => {
      finish({
        ok: false,
        status: "error",
        summary: `Facebook Ads action failed (${action}): ${String(error)}`,
      });
    });

    proc.on("close", (code, signal) => {
      const trimmedStdout = stdout.trim();
      const trimmedStderr = stderr.trim();

      if (timedOut) {
        finish({
          ok: false,
          status: "error",
          summary: `Facebook Ads action timed out (${action}) after ${Math.floor(timeoutMs / 1000)} seconds.`,
        });
        return;
      }

      if (code !== 0 && !trimmedStdout) {
        finish({
          ok: false,
          status: "error",
          summary: `Facebook Ads action failed (${action}): ${trimmedStderr || signal || "unknown error"}`,
        });
        return;
      }

      try {
        const parsed = JSON.parse(trimmedStdout || "{}");
        if (typeof parsed === "object" && parsed !== null) {
          if (trimmedStderr && !("raw_stderr" in parsed)) {
            (parsed as JsonMap).raw_stderr = trimmedStderr;
          }
          finish(parsed as ActionResult);
          return;
        }
      } catch {
        // fall through
      }

      finish({
        ok: false,
        status: "error",
        summary: `Facebook Ads action returned invalid JSON (${action}).`,
        raw_stdout: trimmedStdout,
        raw_stderr: trimmedStderr,
      });
    });
  });
}

function safeArray(input: unknown): Array<Record<string, unknown>> {
  return Array.isArray(input)
    ? (input.filter((item) => typeof item === "object" && item !== null) as Array<Record<string, unknown>>)
    : [];
}

function escapeRegex(input: string): string {
  return input.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function parseReplyContext(body: string): { sender: string; messageId?: number } | null {
  const match = body.match(/\[(?:Replying to|Quoting)\s+(.+?)(?:\s+id:(\d+))?\]\n/s);
  if (!match) {
    return null;
  }
  return {
    sender: (match[1] || "").trim(),
    messageId: maybeNumber(match[2]),
  };
}

function stripReplyContext(body: string): string {
  return body.replace(/^\[(?:Replying to|Quoting)\s+.+?(?:\s+id:\d+)?\]\n/s, "").trim();
}

function isReplyToBotPrompt(body: string, botUsername: string): number | undefined {
  const reply = parseReplyContext(body);
  if (!reply?.messageId) {
    return undefined;
  }
  const normalizedBot = botUsername.replace(/^@/, "").trim().toLowerCase();
  if (!normalizedBot) {
    return undefined;
  }
  const sender = reply.sender.replace(/^@/, "").trim().toLowerCase();
  if (sender === normalizedBot || sender.includes(normalizedBot)) {
    return reply.messageId;
  }
  return undefined;
}

const REPLY_ID_KEYS = ["reply_to_message_id", "replyToMessageId", "reply_to_msg_id", "replyToMsgId"] as const;
const MESSAGE_ID_KEYS = ["message_id", "messageId"] as const;
const REPLY_CONTAINER_KEYS = [
  "reply_to_message",
  "replyToMessage",
  "reply_to",
  "replyTo",
  "reply_message",
  "replyMessage",
  "reply",
] as const;

function isObjectLike(value: unknown): value is Record<string, unknown> | Array<unknown> {
  return typeof value === "object" && value !== null;
}

function isReplyLikeKey(key: string): boolean {
  return key.toLowerCase().includes("reply");
}

function objectHasReplyMetadata(value: unknown): boolean {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const record = value as Record<string, unknown>;
  return (
    REPLY_ID_KEYS.some((key) => record[key] !== undefined) ||
    REPLY_CONTAINER_KEYS.some((key) => isObjectLike(record[key])) ||
    Object.keys(record).some(isReplyLikeKey)
  );
}

function deepReplyMessageId(value: unknown, seen = new Set<unknown>()): number | undefined {
  return deepReplyMessageIdFromContainer(value, seen, false);
}

function deepReplyMessageIdFromContainer(
  value: unknown,
  seen: Set<unknown>,
  inReplyContainer: boolean
): number | undefined {
  if (!isObjectLike(value)) {
    return undefined;
  }
  if (seen.has(value)) {
    return undefined;
  }
  seen.add(value);

  if (Array.isArray(value)) {
    for (const item of value) {
      const candidate = deepReplyMessageIdFromContainer(item, seen, inReplyContainer);
      if (candidate !== undefined) {
        return candidate;
      }
    }
    return undefined;
  }

  const record = value as Record<string, unknown>;
  for (const key of REPLY_ID_KEYS) {
    const candidate = maybeNumber(record[key]);
    if (candidate !== undefined) {
      return candidate;
    }
  }

  if (inReplyContainer) {
    for (const key of MESSAGE_ID_KEYS) {
      const candidate = maybeNumber(record[key]);
      if (candidate !== undefined) {
        return candidate;
      }
    }
  }

  for (const key of REPLY_CONTAINER_KEYS) {
    const candidate = deepReplyMessageIdFromContainer(record[key], seen, true);
    if (candidate !== undefined) {
      return candidate;
    }
  }

  for (const [key, nested] of Object.entries(record)) {
    if (!isObjectLike(nested)) {
      continue;
    }
    if (!isReplyLikeKey(key) && !objectHasReplyMetadata(nested) && !Array.isArray(nested)) {
      continue;
    }
    const candidate = deepReplyMessageIdFromContainer(nested, seen, inReplyContainer || isReplyLikeKey(key));
    if (candidate !== undefined) {
      return candidate;
    }
  }

  return undefined;
}

export function structuredReplyMessageId(ctx: any): number | undefined {
  for (const candidate of [
    ctx?.replyToMessageId,
    ctx?.reply_to_message_id,
    ctx?.replyToMsgId,
    ctx?.reply_to_msg_id,
    ctx?.channelData?.replyToMessageId,
    ctx?.channelData?.reply_to_message_id,
    ctx?.channelData?.replyToMsgId,
    ctx?.channelData?.reply_to_msg_id,
    ctx?.channelData?.telegram?.replyToMessageId,
    ctx?.channelData?.telegram?.reply_to_message_id,
    ctx?.channelData?.telegram?.replyToMsgId,
    ctx?.channelData?.telegram?.reply_to_msg_id,
    ctx?.message?.replyToMessageId,
    ctx?.message?.reply_to_message_id,
    ctx?.message?.replyToMsgId,
    ctx?.message?.reply_to_msg_id,
  ]) {
    const messageId = maybeNumber(candidate);
    if (messageId !== undefined) {
      return messageId;
    }
  }
  for (const candidate of [
    { value: ctx?.reply_to_message, inReplyContainer: true },
    { value: ctx?.replyToMessage, inReplyContainer: true },
    { value: ctx?.reply_to, inReplyContainer: true },
    { value: ctx?.replyTo, inReplyContainer: true },
    { value: ctx?.message?.reply_to_message, inReplyContainer: true },
    { value: ctx?.message?.replyToMessage, inReplyContainer: true },
    { value: ctx?.message?.reply_to, inReplyContainer: true },
    { value: ctx?.message?.replyTo, inReplyContainer: true },
    { value: ctx?.channelData, inReplyContainer: false },
    { value: ctx?.channelData?.telegram, inReplyContainer: false },
    { value: ctx?.telegram, inReplyContainer: false },
    { value: ctx?.rawMessage, inReplyContainer: false },
    { value: ctx?.raw, inReplyContainer: false },
  ]) {
    const messageId = candidate.inReplyContainer
      ? deepReplyMessageIdFromContainer(candidate.value, new Set<unknown>(), true)
      : deepReplyMessageId(candidate.value);
    if (messageId !== undefined) {
      return messageId;
    }
  }
  return undefined;
}

export function replyContextBodies(ctx: any): string[] {
  const seen = new Set<string>();
  const bodies: string[] = [];
  for (const candidate of [
    ctx?.rawMessage?.text,
    ctx?.rawMessage?.caption,
    ctx?.rawMessage?.message,
    ctx?.raw?.message?.text,
    ctx?.raw?.message?.caption,
    ctx?.raw?.message?.message,
    ctx?.message?.text,
    ctx?.message?.caption,
    ctx?.message?.message,
    ctx?.Transcript,
    ctx?.transcript,
    ctx?.RawBody,
    ctx?.rawBody,
    ctx?.content,
    ctx?.BodyForAgent,
    ctx?.BodyForCommands,
    ctx?.CommandBody,
    ctx?.Body,
    ctx?.bodyForAgent,
    ctx?.bodyForCommands,
    ctx?.commandBody,
    ctx?.body,
    ctx?.text,
    ctx?.messageText,
  ]) {
    const text = safeText(candidate).trim();
    if (!text || text === SILENT_REPLY_TOKEN || seen.has(text)) {
      continue;
    }
    seen.add(text);
    bodies.push(text);
  }
  return bodies;
}

export function replyContextMessageId(ctx: any, botUsername: string): number | undefined {
  for (const body of replyContextBodies(ctx)) {
    const messageId = isReplyToBotPrompt(body, botUsername);
    if (messageId !== undefined) {
      return messageId;
    }
  }
  return undefined;
}

function extractReplyToPromptMessageId(ctx: any, botUsername: string): number | undefined {
  return structuredReplyMessageId(ctx) ?? replyContextMessageId(ctx, botUsername);
}

export function incomingMessageText(ctx: any): string {
  for (const candidate of [
    ctx?.rawMessage?.text,
    ctx?.rawMessage?.message,
    ctx?.rawMessage?.caption,
    ctx?.raw?.message?.text,
    ctx?.raw?.message?.message,
    ctx?.raw?.message?.caption,
    ctx?.message?.text,
    ctx?.message?.message,
    ctx?.message?.caption,
    ctx?.CommandBody,
    ctx?.commandBody,
    ctx?.BodyForCommands,
    ctx?.bodyForCommands,
    ctx?.content,
    ctx?.Transcript,
    ctx?.transcript,
    ctx?.RawBody,
    ctx?.rawBody,
    ctx?.BodyForAgent,
    ctx?.bodyForAgent,
    ctx?.Body,
    ctx?.body,
    ctx?.text,
    ctx?.messageText,
  ]) {
    const directText = safeText(candidate).trim();
    if (directText && directText !== SILENT_REPLY_TOKEN) {
      return directText;
    }
  }
  const decoratedText = safeText(
    ctx?.bodyForAgent
      || ctx?.BodyForAgent
      || ctx?.body
      || ctx?.Body
      || ctx?.rawBody
      || ctx?.RawBody
  );
  if (decoratedText.trim() === SILENT_REPLY_TOKEN) {
    return "";
  }
  return stripReplyContext(decoratedText);
}

function normalizeCommandText(text: string, botUsername: string): string {
  let trimmed = text.trim();
  const normalizedBot = botUsername.replace(/^@/, "").trim();
  if (normalizedBot) {
    trimmed = trimmed.replace(new RegExp(`^@${escapeRegex(normalizedBot)}\\b\\s*`, "i"), "");
  }
  return trimmed;
}

function extractAdsCommandArgs(text: string, botUsername: string): string | null {
  const trimmed = normalizeCommandText(text, botUsername);
  const commandMatch = trimmed.match(/^\/ads(?:@[a-zA-Z0-9_]+)?(?:\s+(.+))?$/i);
  if (!commandMatch) {
    return null;
  }
  return (commandMatch[1] || "").trim();
}

export function adsCommandPayloadText(ctx: any): string {
  const directArgs = safeText(ctx?.args).trim();
  if (directArgs && directArgs !== SILENT_REPLY_TOKEN) {
    return directArgs;
  }

  for (const candidate of [
    ctx?.CommandBody,
    ctx?.commandBody,
    ctx?.BodyForCommands,
    ctx?.bodyForCommands,
    ctx?.message?.text,
    ctx?.message?.message,
    ctx?.rawMessage?.text,
    ctx?.rawMessage?.message,
    ctx?.content,
    ctx?.text,
    ctx?.messageText,
  ]) {
    const text = safeText(candidate).trim();
    if (!text || text === SILENT_REPLY_TOKEN) {
      continue;
    }
    const explicitArgs = extractAdsCommandArgs(text, "");
    if (explicitArgs !== null) {
      return explicitArgs;
    }
    if (!text.startsWith("/")) {
      return text;
    }
  }

  return "";
}

function collapseTopicWhitespace(text: string): string {
  return safeText(text).replace(/\s+/g, " ").trim();
}

function stripTopicSearchPrefixes(text: string): { stripped: string; removedPrefix: boolean } {
  const cleaned = collapseTopicWhitespace(text);
  if (!cleaned) {
    return { stripped: "", removedPrefix: false };
  }
  const prefixPatterns = [
    /^(?:show(?:\s+me)?|find|search)\s+ads?\s+for\s+/i,
    /^(?:show(?:\s+me)?|find|search)\s+for\s+/i,
    /^(?:show(?:\s+me)?|find|search)\s+/i,
    /^ads?\s+for\s+/i,
  ];
  for (const pattern of prefixPatterns) {
    const stripped = cleaned.replace(pattern, "").trim();
    if (stripped !== cleaned) {
      return { stripped, removedPrefix: true };
    }
  }
  return { stripped: cleaned, removedPrefix: false };
}

function stripTopicRelativeDatePhrases(text: string): string {
  return collapseTopicWhitespace(
    text
      .replace(/\b(?:for\s+)?(?:the\s+)?(?:past|last)\s+\d+\s+days?\b/gi, " ")
      .replace(/\btoday\b/gi, " ")
      .replace(/\byesterday\b/gi, " ")
  ).trim();
}

export function conversationalSearchKeywordPreview(text: string, botUsername = ""): string {
  const trimmed = normalizeCommandText(text, botUsername);
  if (!trimmed) {
    return "";
  }
  const { stripped: withoutPrefix, removedPrefix } = stripTopicSearchPrefixes(trimmed);
  let keyword = stripTopicRelativeDatePhrases(withoutPrefix);
  if (removedPrefix && /\bads?\b$/i.test(keyword)) {
    keyword = keyword.replace(/\bads?\b$/i, "").trim();
  }
  return collapseTopicWhitespace(keyword.replace(/^[\s"'.,;:-]+|[\s"'.,;:-]+$/g, ""));
}

export function normalizedReplyCommand(text: string): "next 10" | "page" | "advertiser" | "domain" | "inspect" | null {
  const normalized = normalizeText(text);
  if (normalized === "next" || normalized === "next 10") {
    return "next 10";
  }
  if (normalized === "page") {
    return "page";
  }
  if (normalized === "advertiser") {
    return "advertiser";
  }
  if (normalized === "domain") {
    return "domain";
  }
  if (normalized === "inspect") {
    return "inspect";
  }
  return null;
}

export function isAdsTopicHelpRequest(text: string, botUsername = ""): boolean {
  const trimmed = normalizeCommandText(text, botUsername);
  if (!trimmed) {
    return false;
  }
  if (trimmed.startsWith("/") && extractAdsCommandArgs(trimmed, "") === null) {
    return true;
  }
  return ADS_TOPIC_HELP_MARKERS.has(normalizeText(trimmed));
}

function isAmbiguousTopicQuestion(text: string): boolean {
  const trimmed = collapseTopicWhitespace(text);
  if (!trimmed) {
    return false;
  }
  if (trimmed.includes("?")) {
    return true;
  }
  const normalized = normalizeText(trimmed);
  return ADS_TOPIC_QUESTION_PREFIXES.some((prefix) => normalized.startsWith(prefix));
}

function isReservedBareTopicPrefix(text: string): boolean {
  const normalized = normalizeText(text);
  if (["page", "domain", "inspect", "advertiser", "ads", "show", "show me", "find", "search"].includes(normalized)) {
    return true;
  }
  return ADS_TOPIC_RESERVED_PREFIXES.some((prefix) => normalized.startsWith(prefix));
}

function looksLikeConversationalSearch(text: string, botUsername = ""): boolean {
  const trimmed = normalizeCommandText(text, botUsername);
  if (!trimmed || trimmed.startsWith("/")) {
    return false;
  }
  if (normalizedReplyCommand(trimmed) !== null) {
    return false;
  }
  if (isAdsTopicHelpRequest(trimmed, "")) {
    return false;
  }
  if (URL_ONLY_PATTERN.test(trimmed)) {
    return false;
  }
  if (isAmbiguousTopicQuestion(trimmed)) {
    return false;
  }
  const normalized = normalizeText(trimmed);
  if (ADS_TOPIC_GREETINGS.has(normalized)) {
    return false;
  }
  if (isReservedBareTopicPrefix(trimmed)) {
    return false;
  }

  const keyword = conversationalSearchKeywordPreview(trimmed);
  if (!keyword) {
    return false;
  }
  const keywordNormalized = normalizeText(keyword);
  if (!keywordNormalized || ADS_TOPIC_GREETINGS.has(keywordNormalized)) {
    return false;
  }
  if (isReservedBareTopicPrefix(keyword)) {
    return false;
  }
  const tokens = keywordNormalized.split(/\s+/).filter(Boolean);
  if (tokens.length === 0) {
    return false;
  }
  if (tokens.length === 1 && tokens[0].length < 3) {
    return false;
  }
  return true;
}

export function inferImplicitAdsCommandArgs(text: string, botUsername = ""): string | null {
  const trimmed = normalizeCommandText(text, botUsername);
  if (!looksLikeConversationalSearch(trimmed, "")) {
    return null;
  }
  return trimmed;
}

export function classifyAdsTopicText(
  text: string,
  botUsername = ""
): { kind: "reply_command"; args: string } | { kind: "explicit_command"; args: string } | { kind: "conversational_search"; args: string } | { kind: "help"; reason?: string } {
  const trimmed = normalizeCommandText(text, botUsername);
  const replyCommand = normalizedReplyCommand(trimmed);
  if (replyCommand !== null) {
    return { kind: "reply_command", args: replyCommand };
  }
  const adsCommandArgs = extractAdsCommandArgs(trimmed, "");
  if (adsCommandArgs !== null) {
    return { kind: "explicit_command", args: adsCommandArgs };
  }
  const implicitAdsArgs = inferImplicitAdsCommandArgs(trimmed, "");
  if (implicitAdsArgs !== null) {
    return { kind: "conversational_search", args: implicitAdsArgs };
  }
  if (trimmed.startsWith("/") && adsCommandArgs === null) {
    return { kind: "help", reason: "This topic only supports Facebook Ads searches, pivots, and inspect." };
  }
  if (isReservedBareTopicPrefix(trimmed)) {
    return { kind: "help", reason: "Use `/ads page ...`, `/ads domain ...`, or `/ads inspect ...`, or reply directly to a current grouped card." };
  }
  if (isAdsTopicHelpRequest(trimmed, "")) {
    return { kind: "help" };
  }
  if (isAmbiguousTopicQuestion(trimmed) || ADS_TOPIC_GREETINGS.has(normalizeText(trimmed))) {
    return { kind: "help", reason: "This topic only handles Facebook Ads searches, pivots, and inspect on the current result set." };
  }
  return { kind: "help" };
}

export function suppressMainAgentReply(ctx: Record<string, unknown>): void {
  for (const key of [
    "body",
    "bodyForAgent",
    "bodyForCommands",
    "commandBody",
    "rawBody",
    "text",
    "messageText",
    "content",
    "Body",
    "BodyForAgent",
    "BodyForCommands",
    "CommandBody",
    "RawBody",
  ]) {
    ctx[key] = SILENT_REPLY_TOKEN;
  }
  for (const key of [
    "replyToId",
    "replyToMessageId",
    "replyToMsgId",
    "reply_to_message_id",
    "reply_to_msg_id",
    "replyToBody",
    "replyToSender",
    "replyToIsQuote",
    "replyToForwardedFrom",
    "replyToForwardedFromType",
    "replyToForwardedFromId",
    "replyToForwardedFromUsername",
    "replyToForwardedFromTitle",
    "replyToForwardedDate",
  ]) {
    ctx[key] = undefined;
  }
}

function adsTopicHelpResult(reason?: string): ActionResult {
  const text = reason ? `${reason}\n\n${ADS_TOPIC_HELP_TEXT}` : ADS_TOPIC_HELP_TEXT;
  return {
    ok: true,
    status: "ads_topic_help",
    summary: text,
    messages: [{ text }],
  };
}

function isAdsWorkingTaskClass(value: string): value is AdsWorkingTaskClass {
  return value === "search" || value === "pivot" || value === "inspect" || value === "next_10";
}

function defaultAdsTaskLabel(taskClass: AdsWorkingTaskClass): string {
  if (taskClass === "search") {
    return "the current Facebook Ads search";
  }
  if (taskClass === "pivot") {
    return "the current pivot search";
  }
  if (taskClass === "inspect") {
    return "the current inspect run";
  }
  return "the next 10 grouped ads request";
}

function normalizeAdsTaskLabel(taskClass: AdsWorkingTaskClass, taskLabel?: string): string {
  const normalized = safeText(taskLabel).trim();
  return normalized || defaultAdsTaskLabel(taskClass);
}

function adsBusyText(activeTaskLabel: string, requestedTaskLabel: string): string {
  return (
    `Still working on ${activeTaskLabel}. ` +
    `I didn't start ${requestedTaskLabel}. ` +
    "Wait for the current result in this thread, then send the next command again."
  );
}

function adsBusyResult(activeTask: AdsInFlightTask, requestedTask: AdsTaskPreview): ActionResult {
  const text = adsBusyText(activeTask.taskLabel, normalizeAdsTaskLabel(requestedTask.taskClass, requestedTask.taskLabel));
  return {
    ok: true,
    status: "ads_task_busy",
    summary: text,
    messages: [{ text }],
  };
}

function adsTaskScopeKey(route: TelegramRoute, userId: string): string {
  return `${route.scopedChatId}:${userId.trim()}`;
}

function adsInFlightTasksForApi(api: any): Map<string, AdsInFlightTask> {
  if (!api || (typeof api !== "object" && typeof api !== "function")) {
    return sharedAdsInFlightTasks;
  }
  const existing = adsInFlightTasksByApi.get(api as object);
  if (existing) {
    return existing;
  }
  const created = new Map<string, AdsInFlightTask>();
  adsInFlightTasksByApi.set(api as object, created);
  return created;
}

export function adsWorkingStatusText(taskClass: AdsWorkingTaskClass, taskLabel?: string): string {
  const suffix = ADS_WORKING_STATUS_TEXT[taskClass];
  const normalizedLabel = safeText(taskLabel).trim();
  if (!normalizedLabel) {
    if (taskClass === "search") {
      return `Searching Facebook Ads now. ${suffix}`;
    }
    if (taskClass === "pivot") {
      return `Running the pivot search now. ${suffix}`;
    }
    if (taskClass === "inspect") {
      return `Inspecting the landing page now. ${suffix}`;
    }
    return `Loading the next 10 grouped ads now. ${suffix}`;
  }
  if (taskClass === "search" && normalizedLabel.toLowerCase().startsWith("search ")) {
    return `Searching Facebook Ads for ${normalizedLabel.slice("search ".length)} now. ${suffix}`;
  }
  if (taskClass === "pivot") {
    return `Running ${normalizedLabel} now. ${suffix}`;
  }
  if (taskClass === "inspect" && normalizedLabel.toLowerCase().startsWith("inspect ")) {
    return `Inspecting ${normalizedLabel.slice("inspect ".length)} now. ${suffix}`;
  }
  if (taskClass === "next_10") {
    return `Loading ${normalizedLabel} now. ${suffix}`;
  }
  if (taskClass === "search") {
    return `Searching Facebook Ads now. ${suffix}`;
  }
  if (taskClass === "inspect") {
    return `Inspecting the landing page now. ${suffix}`;
  }
  return `Running the pivot search now. ${suffix}`;
}

function adsQueuedStatusText(taskClass: AdsWorkingTaskClass, taskLabel?: string): string {
  return `Queued request detected. ${adsWorkingStatusText(taskClass, taskLabel)}`;
}

async function previewAcceptedAdsTask(
  api: any,
  action: "run_ads_command" | "handle_reply",
  payload: JsonMap
): Promise<AdsTaskPreview | null> {
  const previewHook = api?.facebookAdsTestHooks?.previewAcceptedAdsTaskClass;
  if (typeof previewHook === "function") {
    const rawPreview = await previewHook(action, payload);
    if (typeof rawPreview === "string") {
      return isAdsWorkingTaskClass(rawPreview) ? { taskClass: rawPreview, taskLabel: "" } : null;
    }
    if (rawPreview && typeof rawPreview === "object") {
      const taskClass = safeText((rawPreview as JsonMap).taskClass ?? (rawPreview as JsonMap).task_class).trim();
      if (!isAdsWorkingTaskClass(taskClass)) {
        return null;
      }
      return {
        taskClass,
        taskLabel: safeText((rawPreview as JsonMap).taskLabel ?? (rawPreview as JsonMap).task_label).trim(),
      };
    }
    return null;
  }
  const previewAction = action === "run_ads_command" ? "preview_run_ads_command" : "preview_handle_reply";
  const preview = await runAdsAction(api, previewAction, payload);
  const previewData = (preview.data && typeof preview.data === "object" ? preview.data : {}) as JsonMap;
  const previewed = safeText(previewData.task_class).trim();
  if (preview.ok !== true || !isAdsWorkingTaskClass(previewed)) {
    return null;
  }
  return {
    taskClass: previewed,
    taskLabel: safeText(previewData.task_label).trim(),
  };
}

async function sendAdsWorkingStatusWithOptions(
  api: any,
  route: TelegramRoute,
  preview: AdsTaskPreview,
  options?: { queued?: boolean }
): Promise<void> {
  try {
    const text = options?.queued
      ? adsQueuedStatusText(preview.taskClass, preview.taskLabel)
      : adsWorkingStatusText(preview.taskClass, preview.taskLabel);
    await sendTelegramMessageWithRetry(api, route.chatId, text, {
      accountId: route.accountId,
      messageThreadId: route.threadId,
    }, {
      maxRetryDelayMs: ADS_WORKING_STATUS_MAX_RETRY_DELAY_MS,
    });
  } catch (error) {
    api.logger.warn(`facebook ads working-status send failed: taskClass=${preview.taskClass} summary=${String(error)}`);
  }
}

function inferredInspectDisplayTarget(rawTarget: string): string {
  const trimmed = rawTarget.trim();
  if (!trimmed) {
    return "the requested landing target";
  }
  try {
    const targetUrl = new URL(trimmed.match(/^[a-z][a-z0-9+.-]*:\/\//i) ? trimmed : `https://${trimmed}`);
    return targetUrl.hostname || trimmed;
  } catch {
    return trimmed;
  }
}

function inferredAdsTaskPreviewFromText(argsText: string): AdsTaskPreview | null {
  const trimmed = argsText.trim();
  if (!trimmed) {
    return null;
  }
  const normalized = trimmed.toLowerCase();
  if (normalized === "next" || normalized === "next 10") {
    return { taskClass: "next_10", taskLabel: "the next 10 grouped ads" };
  }
  if (normalized.startsWith("inspect ")) {
    return {
      taskClass: "inspect",
      taskLabel: `inspect ${inferredInspectDisplayTarget(trimmed.slice("inspect ".length))}`,
    };
  }
  if (normalized.startsWith("domain ")) {
    return {
      taskClass: "pivot",
      taskLabel: `domain pivot for ${trimmed.slice("domain ".length).trim() || "the requested domain"}`,
    };
  }
  if (normalized.startsWith("page ")) {
    return {
      taskClass: "pivot",
      taskLabel: `page pivot for ${trimmed.slice("page ".length).trim() || "the requested page"}`,
    };
  }
  if (normalized.startsWith("advertiser ")) {
    const advertiser = trimmed.slice("advertiser ".length).trim() || "the requested advertiser";
    return {
      taskClass: "pivot",
      taskLabel: `advertiser pivot for "${advertiser}"`,
    };
  }
  const keywordPreview = conversationalSearchKeywordPreview(trimmed) || trimmed;
  return {
    taskClass: "search",
    taskLabel: `search "${keywordPreview}"`,
  };
}

function incomingMessageTimestampMs(ctx: any): number | undefined {
  for (const candidate of [
    ctx?.message?.date,
    ctx?.message?.timestamp,
    ctx?.message?.messageDate,
    ctx?.rawMessage?.date,
    ctx?.rawMessage?.timestamp,
    ctx?.channelData?.date,
    ctx?.channelData?.timestamp,
    ctx?.channelData?.telegram?.date,
    ctx?.channelData?.telegram?.timestamp,
    ctx?.date,
    ctx?.timestamp,
  ]) {
    const timestampMs = maybeTimestampMs(candidate);
    if (timestampMs !== undefined) {
      return timestampMs;
    }
  }
  return undefined;
}

function queuedAdsRequest(ctx: any): boolean {
  const timestampMs = incomingMessageTimestampMs(ctx);
  return timestampMs !== undefined && Date.now() - timestampMs >= ADS_QUEUED_STATUS_AGE_MS;
}

async function resolveBotUsername(api: any): Promise<string> {
  if (cachedBotUsername !== null) {
    return cachedBotUsername;
  }
  if (botUsernamePromise) {
    return botUsernamePromise;
  }
  botUsernamePromise = (async () => {
    try {
      const route = configuredTelegramRoute(api);
      const telegramAccounts = api?.config?.channels?.telegram?.accounts;
      const accountId = route?.accountId || Object.keys(telegramAccounts || {})[0];
      const resolution = api.runtime.channel.telegram.resolveTelegramToken(api.config, { accountId });
      if (!resolution?.token) {
        cachedBotUsername = "";
        return cachedBotUsername;
      }
      const probe = await api.runtime.channel.telegram.probeTelegram(resolution.token, 10000);
      cachedBotUsername = safeText(probe?.bot?.username).trim();
      return cachedBotUsername;
    } catch (error) {
      api.logger.warn(`facebook ads bot identity probe failed: ${String(error)}`);
      cachedBotUsername = "";
      return cachedBotUsername;
    } finally {
      botUsernamePromise = null;
    }
  })();
  return botUsernamePromise;
}

async function sendStructuredGroupMessage(
  api: any,
  chatId: string,
  message: Record<string, unknown>,
  options: TelegramSendOptions
): Promise<string[]> {
  const fallbackText = safeText(message.text).trim();
  const nativeMediaRequest = buildTelegramNativeMediaRequest(chatId, message, options);
  if (!nativeMediaRequest) {
    message.media_outcome = safeText(message.media_outcome).trim() || "no_media_in_payload";
    message.text_fallback_used = false;
    message.native_media_sent = false;
    message.document_recovered_sent = false;
    if (!fallbackText) {
      return [];
    }
    const sent = await sendTelegramTextMessage(api, chatId, fallbackText, options, message);
    await bindSentMessage(api, message, extractTelegramMessageId(sent));
    return [fallbackText];
  }

  const mediaUrl = safeText(message.media_url).trim();
  const hasLocalMediaPath = Boolean(safeText(message.media_path).trim());
  const videoMessage = isVideoMediaMessage(message);
  let probeDetails: MediaProbeDetails | null = null;

  try {
    if (videoMessage && mediaUrl && !hasLocalMediaPath) {
      probeDetails = await probeMediaUrl(mediaUrl);
      applyMediaProbeDetails(message, probeDetails);
      if (probeDetails.status === "expired_or_unfetchable") {
        markFailedMediaDelivery(message, {
          outcome: "media_present_expired_or_unfetchable",
          fallbackUsed: Boolean(fallbackText),
        });
        api.logger.warn(
          `facebook ads video preflight found expired or unfetchable media; falling back to text card. urlClass=${probeDetails.urlClass}`
        );
        if (!fallbackText) {
          return [];
        }
        const sent = await sendTelegramTextMessage(api, chatId, fallbackText, options, message);
        await bindSentMessage(api, message, extractTelegramMessageId(sent));
        return [fallbackText];
      }
      if (probeDetails.status === "ok" && probeDetails.tooLargeForTelegram) {
        markFailedMediaDelivery(message, {
          outcome: "telegram_video_too_large",
          fallbackUsed: Boolean(fallbackText),
        });
        api.logger.warn(
          `facebook ads video preflight detected Telegram Bot API oversize video; falling back to text card. sizeBytes=${probeDetails.sizeBytes ?? "unknown"} urlClass=${probeDetails.urlClass}`
        );
        if (!fallbackText) {
          return [];
        }
        const sent = await sendTelegramTextMessage(api, chatId, fallbackText, options, message);
        await bindSentMessage(api, message, extractTelegramMessageId(sent));
        return [fallbackText];
      }
      if (probeDetails.status === "ok" && probeDetails.uploadFirstRecommended) {
        const downloadedMediaPath = await downloadFetchableMediaToTempPath(api, message, probeDetails);
        if (!downloadedMediaPath) {
          const refreshedProbe = await probeMediaUrl(mediaUrl);
          applyMediaProbeDetails(message, refreshedProbe);
          const downloadFailureOutcome =
            refreshedProbe.status === "expired_or_unfetchable"
              ? "media_present_expired_or_unfetchable"
              : "media_present_fetch_failed";
          markFailedMediaDelivery(message, {
            outcome: downloadFailureOutcome,
            fallbackUsed: Boolean(fallbackText),
          });
          api.logger.warn(
            `facebook ads upload-first video download failed; falling back to text card. urlClass=${probeDetails.urlClass} outcome=${downloadFailureOutcome}`
          );
          if (!fallbackText) {
            return [];
          }
          const sent = await sendTelegramTextMessage(api, chatId, fallbackText, options, message);
          await bindSentMessage(api, message, extractTelegramMessageId(sent));
          return [fallbackText];
        }
        message.media_path = downloadedMediaPath;
        try {
          const uploadResult = await tryUploadedVideoRecovery(api, chatId, message, options, {
            allowDocumentRetry: canAttemptVideoUploadRecovery(message, probeDetails),
          });
          if (uploadResult) {
            return uploadResult.sentTexts;
          }
        } catch (uploadError) {
          markFailedMediaDelivery(message, {
            outcome: "media_present_telegram_rejected",
            fallbackUsed: Boolean(fallbackText),
          });
          api.logger.warn(
            `facebook ads upload-first video delivery failed; falling back to text card. urlClass=${probeDetails.urlClass} summary=${String(uploadError)}`
          );
          if (!fallbackText) {
            throw uploadError;
          }
          const sent = await sendTelegramTextMessage(api, chatId, fallbackText, options, message);
          await bindSentMessage(api, message, extractTelegramMessageId(sent));
          return [fallbackText];
        }
      }
    }
    try {
      if (videoMessage && hasLocalMediaPath) {
        const uploadResult = await tryUploadedVideoRecovery(api, chatId, message, options, {
          allowDocumentRetry: canAttemptVideoUploadRecovery(message, probeDetails),
        });
        if (uploadResult) {
          return uploadResult.sentTexts;
        }
      }
      return await sendMediaRequestAndOptionalText(api, chatId, message, options, nativeMediaRequest, {
        outcome: "media_present_native_sent",
        nativeMediaSent: true,
        documentRecovered: false,
      });
    } catch (error) {
      const failureOutcome = await classifyNativeMediaFailure(message, probeDetails);
      markFailedMediaDelivery(message, {
        outcome: failureOutcome,
        fallbackUsed: Boolean(fallbackText),
      });
      const canRetryLocally =
        failureOutcome === "media_present_telegram_rejected" &&
        mediaUrl &&
        (!videoMessage || canAttemptVideoUploadRecovery(message, probeDetails));
      if (canRetryLocally) {
        const downloadedMediaPath = await downloadFetchableMediaToTempPath(api, message, probeDetails);
        if (downloadedMediaPath) {
          message.media_path = downloadedMediaPath;
          try {
            if (videoMessage) {
              const uploadResult = await tryUploadedVideoRecovery(api, chatId, message, options, {
                allowDocumentRetry: canAttemptVideoUploadRecovery(message, probeDetails),
              });
              if (uploadResult) {
                return uploadResult.sentTexts;
              }
            } else if (isPhotoMediaMessage(message)) {
              const localMediaRequest = buildTelegramNativeMediaRequest(chatId, message, options);
              if (localMediaRequest) {
                return await sendMediaRequestAndOptionalText(api, chatId, message, options, localMediaRequest, {
                  outcome: "media_present_native_sent",
                  nativeMediaSent: true,
                  documentRecovered: false,
                });
              }
            }
          } catch (localError) {
            api.logger.warn(
              `facebook ads local media upload retry failed; falling back to text card. mediaKind=${safeText(message.media_kind)} summary=${String(localError)}`
            );
            markFailedMediaDelivery(message, {
              outcome: "media_present_telegram_rejected",
              fallbackUsed: Boolean(fallbackText),
            });
          }
        }
      }
      api.logger.warn(
        `facebook ads native media send failed; falling back to text card. mediaKind=${safeText(message.media_kind)} outcome=${safeText(message.media_outcome)} summary=${String(error)}`
      );
      if (!fallbackText) {
        throw error;
      }
      const sent = await sendTelegramTextMessage(api, chatId, fallbackText, options, message);
      await bindSentMessage(api, message, extractTelegramMessageId(sent));
      return [fallbackText];
    }
  } finally {
    await cleanupLocalMediaPath(api, message);
  }
}

export async function dispatchMessages(
  api: any,
  params: {
    chatId: string;
    accountId?: string;
    messageThreadId?: number;
    fallbackReplyToMessageId?: number;
    result: ActionResult;
  }
): Promise<void> {
  const messages = safeArray(params.result.messages);
  const fallbackText = safeText(params.result.summary).trim();
  const accountId = params.accountId || "";
  const deferredPromptBindings: Array<{ searchSessionId: string; promptMessageId: number }> = [];

  if (messages.length === 0) {
    if (!fallbackText) {
      return;
    }
    await sendTelegramMessageWithRetry(api, params.chatId, fallbackText, {
      accountId,
      replyToMessageId: params.fallbackReplyToMessageId,
      messageThreadId: params.messageThreadId,
    });
    return;
  }

  const sentTexts = new Set<string>();
  for (const [index, message] of messages.entries()) {
    const text = safeText(message.text).trim();
    const nativeMediaText = safeText(message.native_media_text).trim();
    if (!text && !nativeMediaText) {
      continue;
    }
    const sendOptions: TelegramSendOptions = {
      accountId,
      replyToMessageId: maybeNumber(message.reply_to_message_id) ?? params.fallbackReplyToMessageId,
      messageThreadId: params.messageThreadId,
    };
    if (telegramMediaMethodForMessage(message)) {
      for (const sentText of await sendStructuredGroupMessage(api, params.chatId, message, sendOptions)) {
        sentTexts.add(sentText);
      }
    } else if (text) {
      sentTexts.add(text);
      const sendResult = await sendTelegramTextMessage(api, params.chatId, text, sendOptions, message);
      const sentMessageId = extractTelegramMessageId(sendResult);
      await bindSentMessage(api, message, sentMessageId);
      const deferredPromptBinding = deferredPromptBindingForMessage(message, sentMessageId);
      if (deferredPromptBinding) {
        deferredPromptBindings.push(deferredPromptBinding);
      }
    }
    if (index < messages.length - 1) {
      await sleep(TELEGRAM_SEND_DELAY_MS);
    }
  }

  if (fallbackText && !sentTexts.has(fallbackText)) {
    await sendTelegramMessageWithRetry(api, params.chatId, fallbackText, {
      accountId,
      replyToMessageId: params.fallbackReplyToMessageId,
      messageThreadId: params.messageThreadId,
    });
  }

  for (const binding of deferredPromptBindings) {
    await bindPromptMessageForApi(api, binding.searchSessionId, binding.promptMessageId);
  }
}

function commandChatId(ctx: any): string {
  return normalizeTelegramTarget(ctx?.to);
}

function commandUserId(ctx: any): string {
  return safeText(ctx?.senderId).trim();
}

async function executeAdsCommand(api: any, ctx: any, argsText: string): Promise<void | { text?: string }> {
  const route = routeMatchesTelegramContext(api, ctx);
  if (!route) {
    api.logger.warn(
      `facebook ads command route mismatch: accountId=${safeText(ctx?.accountId)} to=${safeText(ctx?.to)} groupId=${safeText(ctx?.groupId)} conversationId=${safeText(ctx?.conversationId)} messageThreadId=${safeText(ctx?.messageThreadId)}`
    );
    return;
  }
  const payload = {
    chat_id: route.scopedChatId,
    user_id: commandUserId(ctx),
    username: safeText(ctx?.senderUsername).trim() || safeText(ctx?.senderName).trim() || commandUserId(ctx),
    text: argsText,
  };
  const preview = await previewAcceptedAdsTask(api, "run_ads_command", payload);
  const statusPreview = preview ?? inferredAdsTaskPreviewFromText(argsText);
  const scopeKey = adsTaskScopeKey(route, payload.user_id);
  const inFlightTasks = adsInFlightTasksForApi(api);
  let inFlightToken: symbol | null = null;
  if (preview) {
    const activeTask = inFlightTasks.get(scopeKey);
    if (activeTask) {
      await dispatchMessages(api, {
        chatId: route.chatId,
        accountId: route.accountId,
        messageThreadId: route.threadId,
        result: adsBusyResult(activeTask, preview),
      });
      return;
    }
    inFlightToken = Symbol(scopeKey);
    inFlightTasks.set(scopeKey, {
      token: inFlightToken,
      taskClass: preview.taskClass,
      taskLabel: normalizeAdsTaskLabel(preview.taskClass, preview.taskLabel),
    });
    await sendAdsWorkingStatusWithOptions(api, route, preview, { queued: queuedAdsRequest(ctx) });
  } else if (statusPreview) {
    await sendAdsWorkingStatusWithOptions(api, route, statusPreview, { queued: queuedAdsRequest(ctx) });
  }
  try {
    const result = await runAdsAction(api, "run_ads_command", payload);
    if (result.ok !== true) {
      api.logger.warn(
        `facebook ads command failed: status=${safeText(result.status)} summary=${safeText(result.summary)} stderr=${safeText(result.raw_stderr)}`
      );
    }
    if (!route.chatId) {
      return { text: safeText(result.summary) || "Facebook Ads command failed." };
    }
    await dispatchMessages(api, {
      chatId: route.chatId,
      accountId: route.accountId,
      messageThreadId: route.threadId,
      result,
    });
  } finally {
    if (inFlightToken && inFlightTasks.get(scopeKey)?.token === inFlightToken) {
      inFlightTasks.delete(scopeKey);
    }
  }
}

export default function registerFacebookAdsPlugin(api: any): void {
  api.registerTool({
    name: "search_ads",
    description: "Search Facebook Ads Library in the USA and return grouped ad entities",
    parameters: {
      type: "object",
      properties: {
        keyword: { type: "string" },
        date_from: { type: "string" },
        date_to: { type: "string" },
        geo: { type: "string", enum: ["US"] },
        limit: { type: "integer", minimum: 1, maximum: 10 },
        chat_id: { type: "string" },
        user_id: { type: "string" },
        session_owner: { type: "string" }
      },
      required: ["keyword"],
      additionalProperties: false
    },
    execute: async (args: Record<string, unknown>) => await runAdsAction(api, "search_ads", args)
  });

  api.registerTool({
    name: "get_next_page",
    description: "Return the next grouped ad page for an existing search session",
    parameters: {
      type: "object",
      properties: {
        search_session_id: { type: "string" },
        limit: { type: "integer", minimum: 1, maximum: 10 }
      },
      required: ["search_session_id"],
      additionalProperties: false
    },
    execute: async (args: Record<string, unknown>) => await runAdsAction(api, "get_next_page", args)
  });

  api.registerTool({
    name: "get_ad_details",
    description: "Fetch detailed page metadata for an ad archive id and page id",
    parameters: {
      type: "object",
      properties: {
        ad_archive_id: { type: "string" },
        page_id: { type: "string" },
        graphql_session_id: { type: "string" }
      },
      required: ["ad_archive_id", "page_id"],
      additionalProperties: false
    },
    execute: async (args: Record<string, unknown>) => await runAdsAction(api, "get_ad_details", args)
  });

  api.registerTool({
    name: "format_grouped_ad_card",
    description: "Render a normalized grouped ad entity into a Telegram-friendly card",
    parameters: {
      type: "object",
      properties: {
        grouped_ad_entity: { type: "object" },
        include_media_line: { type: "boolean" }
      },
      required: ["grouped_ad_entity"],
      additionalProperties: false
    },
    execute: async (args: Record<string, unknown>) => await runAdsAction(api, "format_grouped_ad_card", args)
  });

  api.registerTool({
    name: "inspect_group_funnel",
    description: "Inspect the landing and redirect path for one grouped ad entity",
    parameters: {
      type: "object",
      properties: {
        search_session_id: { type: "string" },
        group_key: { type: "string" }
      },
      required: ["search_session_id", "group_key"],
      additionalProperties: false
    },
    execute: async (args: Record<string, unknown>) => await runAdsAction(api, "inspect_group_funnel", args)
  });

  api.registerTool({
    name: "ads_health_check",
    description: "Verify the unofficial Facebook Ads acquisition path and surface diagnostics",
    parameters: {
      type: "object",
      properties: {
        keyword: { type: "string" },
        date_from: { type: "string" },
        date_to: { type: "string" }
      },
      additionalProperties: false
    },
    execute: async (args: Record<string, unknown>) => await runAdsAction(api, "ads_health_check", args)
  });

  api.registerTool({
    name: "compare_reference_results",
    description: "Compare the plugin's grouped top results against the reference service",
    parameters: {
      type: "object",
      properties: {
        search_session_id: { type: "string" }
      },
      required: ["search_session_id"],
      additionalProperties: false
    },
    execute: async (args: Record<string, unknown>) => await runAdsAction(api, "compare_reference_results", args)
  });

  api.registerCommand({
    name: "ads",
    description: 'Search Ads Library: /ads keyword, /ads "keyword" from=YYYY-MM-DD to=YYYY-MM-DD, or topic text like "auto insurance for past 3 days"',
    acceptsArgs: true,
    handler: async (ctx: any) => executeAdsCommand(api, ctx, adsCommandPayloadText(ctx))
  });

  api.registerHook(
    "message:preprocessed",
    async (event: any) => {
      const ctx = (event?.context || {}) as Record<string, unknown>;
      const route = routeMatchesTelegramContext(api, ctx);
      if (!route) {
        return;
      }

      const text = incomingMessageText(ctx).trim();
      if (!text) {
        return;
      }
      const replyContextCandidates = replyContextBodies(ctx);
      const replyContextCount = replyContextCandidates.length;
      const structuredReplyToMessageId = structuredReplyMessageId(ctx);
      suppressMainAgentReply(ctx);
      const botUsername = await resolveBotUsername(api);
      const decision = classifyAdsTopicText(text, botUsername || "");

      if (decision.kind === "reply_command") {
        const replyCommand = decision.args;
        let replyToMessageId = structuredReplyToMessageId;
        if (replyToMessageId === undefined) {
          for (const body of replyContextCandidates) {
            const candidate = isReplyToBotPrompt(body, botUsername || "");
            if (candidate !== undefined) {
              replyToMessageId = candidate;
              break;
            }
          }
        }
        if (replyToMessageId === undefined) {
          api.logger.warn(
            `facebook ads reply hook missing reply target: command=${replyCommand} chat=${route.scopedChatId} user=${safeText(ctx.senderId || ctx.from).trim()} threadId=${route.threadId ?? "none"} bodyCandidates=${replyContextCount}`
          );
          if (replyCommand !== "next 10") {
            await dispatchMessages(api, {
              chatId: route.chatId,
              accountId: route.accountId,
              messageThreadId: route.threadId,
              result: adsTopicHelpResult("Reply `page`, `domain`, or `inspect` directly to a current grouped card."),
            });
            return;
          }
        }
        const payload = {
          chat_id: route.scopedChatId,
          user_id: safeText(ctx.senderId || ctx.from).trim(),
          text: replyCommand,
          reply_to_message_id: replyToMessageId,
        };
        const preview = await previewAcceptedAdsTask(api, "handle_reply", payload);
        const scopeKey = adsTaskScopeKey(route, payload.user_id);
        const inFlightTasks = adsInFlightTasksForApi(api);
        let inFlightToken: symbol | null = null;
        if (preview) {
          const activeTask = inFlightTasks.get(scopeKey);
          if (activeTask) {
            await dispatchMessages(api, {
              chatId: route.chatId,
              accountId: route.accountId,
              messageThreadId: route.threadId,
              fallbackReplyToMessageId: replyToMessageId,
              result: adsBusyResult(activeTask, preview),
            });
            return;
          }
          inFlightToken = Symbol(scopeKey);
          inFlightTasks.set(scopeKey, {
            token: inFlightToken,
            taskClass: preview.taskClass,
            taskLabel: normalizeAdsTaskLabel(preview.taskClass, preview.taskLabel),
          });
          await sendAdsWorkingStatusWithOptions(api, route, preview, { queued: queuedAdsRequest(ctx) });
        }
        try {
          const result = await runAdsAction(api, "handle_reply", payload);
          if (result.ok !== true) {
            api.logger.warn(
              `facebook ads reply hook failed: command=${replyCommand} replyTo=${replyToMessageId ?? "missing"} status=${safeText(result.status)} summary=${safeText(result.summary)}`
            );
          }
          await dispatchMessages(api, {
            chatId: route.chatId,
            accountId: route.accountId,
            messageThreadId: route.threadId,
            fallbackReplyToMessageId: replyToMessageId,
            result
          });
        } finally {
          if (inFlightToken && inFlightTasks.get(scopeKey)?.token === inFlightToken) {
            inFlightTasks.delete(scopeKey);
          }
        }
        return;
      }

      if (decision.kind === "explicit_command" || decision.kind === "conversational_search") {
        await executeAdsCommand(api, ctx, decision.args);
        return;
      }

      await dispatchMessages(api, {
        chatId: route.chatId,
        accountId: route.accountId,
        messageThreadId: route.threadId,
        result: adsTopicHelpResult(decision.reason),
      });
    },
    {
      name: "facebook_ads_pagination_reply_hook",
      description: "Handle Telegram direct replies to the Facebook Ads next-page prompt."
    }
  );
}
