const express = require("express");
const axios = require("axios");
const twilio = require("twilio");
const fs = require("fs");
const path = require("path");

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

const VoiceResponse = twilio.twiml.VoiceResponse;

const SAY_OPTIONS = {
  voice: "Polly.Matthew",
  language: "en-US"
};

const forecastCache = new Map();
const inFlightForecasts = new Map();
const canadaAlertCache = new Map();
const ecCityPageCache = new Map();
const exchangeRateCache = new Map();
const borderWaitCache = new Map();

const temporaryLocationsByCall = new Map();
const pendingLocationChoiceByCall = new Map();
const lastPlaybackByCall = new Map();
const menuStateByCall = new Map();
const menuHistoryByCall = new Map();
const unitPreferenceByCall = new Map();
const exchangeSelectionByCall = new Map();
const borderDirectionByCall = new Map();

const FORECAST_CACHE_MS = 15 * 60 * 1000;
const STALE_FORECAST_MS = 2 * 60 * 1000;
const ALERT_CACHE_MS = 10 * 60 * 1000;
const EC_CITYPAGE_CACHE_MS = 20 * 60 * 1000;
const EXCHANGE_CACHE_MS = 10 * 60 * 1000;
const BORDER_CACHE_MS = 30 * 1000;

const EC_API_TIMEOUT_MS = 5000;
const EC_ALERT_TIMEOUT_MS = 5000;
const OPEN_METEO_TIMEOUT_MS = 15000;
const BORDER_API_TIMEOUT_MS = 15000;

const VOICEMAILS_FILE = path.join(__dirname, "voicemails.json");

const PRESET_LOCATIONS = {
  "1": {
    id: "montreal",
    name: "Montreal, Quebec, Canada",
    latitude: 45.5239,
    longitude: -73.5997,
    timezone: "America/Toronto",
    label: "Montreal",
    country: "CA",
    postalCode: "H2V 4B7",
    alertFeedUrl: "https://weather.gc.ca/rss/alerts/45.5017_-73.5673_e.xml",
    ecCityPageId: ""
  },
  "2": {
    id: "tosh",
    name: "Boisbriand, Quebec, Canada",
    latitude: 45.6192,
    longitude: -73.8396,
    timezone: "America/Toronto",
    label: "Tosh",
    country: "CA",
    postalCode: "J7E 4H4",
    ecCityPageId: ""
  },
  "3": {
    id: "laurentians",
    name: "Laurentians, Quebec, Canada",
    latitude: 46.0168,
    longitude: -74.2239,
    timezone: "America/Toronto",
    label: "Laurentians",
    country: "CA",
    postalCode: "J0T 2B0",
    ecCityPageId: ""
  },
  "4": {
    id: "brooklyn",
    name: "Brooklyn, New York, USA",
    latitude: 40.7143,
    longitude: -73.9533,
    timezone: "America/New_York",
    label: "Brooklyn",
    country: "US"
  },
  "5": {
    id: "monsey",
    name: "Spring Valley, New York, USA",
    latitude: 41.1134,
    longitude: -74.0435,
    timezone: "America/New_York",
    label: "Monsey",
    country: "US"
  },
  "6": {
    id: "monroe",
    name: "Monroe, New York, USA",
    latitude: 41.3326,
    longitude: -74.186,
    timezone: "America/New_York",
    label: "Monroe",
    country: "US"
  }
};

const US_LOCATIONS = {
  "1": PRESET_LOCATIONS["4"],
  "2": PRESET_LOCATIONS["6"],
  "3": PRESET_LOCATIONS["5"]
};

const CHAMPLAIN_LACOLLE = {
  cbsaOfficeName: "St-Bernard-de-Lacolle",
  cbpPortNumber: "04071201",
  cbpPortName: "Champlain",
  spokenNameCanada: "Champlain border, entering Canada",
  spokenNameUs: "Champlain border, entering the United States",
  staleHours: 5,
  constructionWarning: "This crossing is under construction until 2027. Expect longer wait times."
};

function gatherOptions(action, timeout = 8, numDigits = 1) {
  return {
    input: "dtmf",
    action,
    method: "POST",
    timeout,
    numDigits,
    finishOnKey: ""
  };
}

function escapeForSsml(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function say(twiml, text, options = {}) {
  const cleaned = String(text || "")
    .replace(/\s+/g, " ")
    .replace(/\s+([,.!?;:])/g, "$1")
    .trim();

  if (!cleaned) return;

  const {
    rate = "100%",
    pitch = "default",
    volume = "default"
  } = options;

  const parts = cleaned
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);

  for (const part of parts) {
    const ssml = `
      <speak>
        <prosody rate="${rate}" pitch="${pitch}" volume="${volume}">
          ${escapeForSsml(part)}
        </prosody>
      </speak>
    `.replace(/\s+/g, " ").trim();

    twiml.say(SAY_OPTIONS, ssml);
  }
}

function getCallKey(req) {
  return String(req.body.CallSid || "unknown").trim();
}

function getDigits(req) {
  return String(req.body.Digits || "").trim();
}

function placeLabel(location) {
  return location?.label || location?.name || "your area";
}

function loadJsonFile(filePath, fallback) {
  try {
    if (!fs.existsSync(filePath)) return fallback;
    const raw = fs.readFileSync(filePath, "utf8");
    return JSON.parse(raw || JSON.stringify(fallback));
  } catch (error) {
    console.error(`Failed to read ${filePath}:`, error.message);
    return fallback;
  }
}

function saveJsonFile(filePath, value) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
  } catch (error) {
    console.error(`Failed to write ${filePath}:`, error.message);
  }
}

function getTemporaryLocation(req) {
  return temporaryLocationsByCall.get(getCallKey(req)) || null;
}

function saveTemporaryLocationForCall(req, location) {
  temporaryLocationsByCall.set(getCallKey(req), location);
}

function clearPendingLocationChoice(req) {
  pendingLocationChoiceByCall.delete(getCallKey(req));
}

function getActiveLocation(req) {
  return getTemporaryLocation(req);
}

function setLastPlayback(req, payload) {
  lastPlaybackByCall.set(getCallKey(req), payload);
}

function getLastPlayback(req) {
  return lastPlaybackByCall.get(getCallKey(req)) || null;
}

function setMenuState(req, state) {
  menuStateByCall.set(getCallKey(req), state);
}

function getMenuState(req) {
  return menuStateByCall.get(getCallKey(req)) || "";
}

function getMenuHistory(req) {
  return menuHistoryByCall.get(getCallKey(req)) || [];
}

function getBorderDirection(req) {
  return borderDirectionByCall.get(getCallKey(req)) || null;
}

function setBorderDirection(req, direction) {
  borderDirectionByCall.set(getCallKey(req), direction);
}

function shouldTrackHistoryState(state) {
  return [
    "root-menu",
    "location-menu",
    "us-location-menu",
    "main-menu",
    "forecast-menu",
    "exchange-menu"
  ].includes(state);
}

function pushMenuHistory(req, state) {
  if (!shouldTrackHistoryState(state)) return;
  const key = getCallKey(req);
  const history = menuHistoryByCall.get(key) || [];
  if (history[history.length - 1] !== state) history.push(state);
  menuHistoryByCall.set(key, history);
}

function popMenuHistory(req) {
  const key = getCallKey(req);
  const history = menuHistoryByCall.get(key) || [];
  if (history.length > 0) history.pop();
  menuHistoryByCall.set(key, history);
  return history;
}

function getUnitPreference(req) {
  return unitPreferenceByCall.get(getCallKey(req)) || "C";
}

function setUnitPreference(req, unit) {
  unitPreferenceByCall.set(getCallKey(req), unit === "F" ? "F" : "C");
}

function toggleUnitPreference(req) {
  const next = getUnitPreference(req) === "F" ? "C" : "F";
  setUnitPreference(req, next);
  return next;
}

function getExchangeSelection(req) {
  return exchangeSelectionByCall.get(getCallKey(req)) || null;
}

function setExchangeSelection(req, selection) {
  exchangeSelectionByCall.set(getCallKey(req), selection);
}

function clearCallState(req) {
  const key = getCallKey(req);
  temporaryLocationsByCall.delete(key);
  pendingLocationChoiceByCall.delete(key);
  lastPlaybackByCall.delete(key);
  menuStateByCall.delete(key);
  menuHistoryByCall.delete(key);
  unitPreferenceByCall.delete(key);
  exchangeSelectionByCall.delete(key);
  borderDirectionByCall.delete(key);
}

function appendVoicemailRecord(record) {
  const current = loadJsonFile(VOICEMAILS_FILE, []);
  current.push(record);
  saveJsonFile(VOICEMAILS_FILE, current);
}

function updateVoicemailRecord(callSid, updates) {
  const current = loadJsonFile(VOICEMAILS_FILE, []);
  let found = false;
  for (let i = current.length - 1; i >= 0; i--) {
    if (current[i].callSid === callSid) {
      current[i] = { ...current[i], ...updates };
      found = true;
      break;
    }
  }
  if (!found) current.push({ callSid, ...updates });
  saveJsonFile(filePath = VOICEMAILS_FILE, value = current);
}

function normalizePlaceName(name) {
  return String(name || "")
    .toUpperCase()
    .replace(/,\s*CANADA/g, "")
    .replace(/,\s*USA/g, "")
    .replace(/[^A-Z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function cacheKeyForLocation(location) {
  const place = normalizePlaceName(location.name);
  return place || `${Number(location.latitude).toFixed(2)},${Number(location.longitude).toFixed(2)}`;
}

function pruneForecastCache() {
  const now = Date.now();
  for (const [key, value] of forecastCache.entries()) {
    if (!value || now - value.timestamp > STALE_FORECAST_MS) forecastCache.delete(key);
  }
}

function getCachedForecast(location, { allowStale = false } = {}) {
  const key = cacheKeyForLocation(location);
  const cached = forecastCache.get(key);
  if (!cached) return null;
  const age = Date.now() - cached.timestamp;
  if (age <= FORECAST_CACHE_MS) return { data: cached.data, isStale: false, key, age };
  if (allowStale && age <= STALE_FORECAST_MS) return { data: cached.data, isStale: true, key, age };
  return null;
}

function setCachedForecast(location, data) {
  const key = cacheKeyForLocation(location);
  forecastCache.set(key, { data, timestamp: Date.now() });
  return key;
}

function parsePlainDate(isoDate) {
  const text = String(isoDate || "").slice(0, 10);
  const [year, month, day] = text.split("-").map(Number);
  if (!year || !month || !day) return new Date(isoDate);
  return new Date(year, month - 1, day, 12, 0, 0);
}

function fullDateLabel(iso, tz) {
  return parsePlainDate(iso).toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    timeZone: tz || "UTC"
  });
}

function getDatePartFromLocalIso(iso) {
  return String(iso || "").slice(0, 10);
}

function getHourFromLocalIso(iso) {
  const match = String(iso || "").match(/T(\d{1,2}):(\d{2})/);
  return match ? Number(match[1]) : 12;
}

function getMinuteFromLocalIso(iso) {
  const match = String(iso || "").match(/T(\d{1,2}):(\d{2})/);
  return match ? Number(match[1]) : 0;
}

function formatLocalIsoTimeLabel(iso) {
  const hour24 = getHourFromLocalIso(iso);
  const minute = getMinuteFromLocalIso(iso);
  const suffix = hour24 >= 12 ? "PM" : "AM";
  let hour12 = hour24 % 12;
  if (hour12 === 0) hour12 = 12;
  return `${hour12}:${String(minute).padStart(2, "0")} ${suffix}`;
}

function addHoursToLocalIso(iso, hoursToAdd) {
  const datePart = getDatePartFromLocalIso(iso);
  const [year, month, day] = datePart.split("-").map(Number);
  const hour = getHourFromLocalIso(iso);
  const minute = getMinuteFromLocalIso(iso);
  if (!year || !month || !day) return iso;
  const dt = new Date(Date.UTC(year, month - 1, day, hour, minute, 0));
  dt.setUTCHours(dt.getUTCHours() + Number(hoursToAdd || 0));
  return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, "0")}-${String(dt.getUTCDate()).padStart(2, "0")}T${String(dt.getUTCHours()).padStart(2, "0")}:${String(dt.getUTCMinutes()).padStart(2, "0")}`;
}

function addDaysToDateText(dateText, daysToAdd) {
  const [year, month, day] = String(dateText || "").split("-").map(Number);
  if (!year || !month || !day) return dateText;
  const dt = new Date(Date.UTC(year, month - 1, day, 12, 0, 0));
  dt.setUTCDate(dt.getUTCDate() + Number(daysToAdd || 0));
  return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, "0")}-${String(dt.getUTCDate()).padStart(2, "0")}`;
}

function issuedDateLabelToday(tz) {
  return new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
    timeZone: tz || "UTC"
  });
}

function issuedMonthDayLabel(rawTimestamp, timezone) {
  const text = String(rawTimestamp || "").trim();
  if (!text) {
    return new Date().toLocaleDateString("en-US", {
      month: "long",
      day: "numeric",
      timeZone: timezone || "America/Toronto"
    });
  }

  const parsed = new Date(text);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleDateString("en-US", {
      month: "long",
      day: "numeric",
      timeZone: timezone || "America/Toronto"
    });
  }

  const iso = formatEcLocalIso(text, timezone || "America/Toronto");
  if (iso) {
    const dateText = getDatePartFromLocalIso(iso);
    return parsePlainDate(dateText).toLocaleDateString("en-US", {
      month: "long",
      day: "numeric",
      timeZone: timezone || "America/Toronto"
    });
  }

  return new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    timeZone: timezone || "America/Toronto"
  });
}

function retrievedTimeLabelNow(tz) {
  return new Date().toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone: tz || "UTC"
  });
}

function getHourInTz(iso) {
  return getHourFromLocalIso(iso);
}

function getNowHourInTz(timezone) {
  const parts = new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    hour12: false,
    timeZone: timezone || "UTC"
  }).formatToParts(new Date());
  return Number(parts.find((p) => p.type === "hour")?.value || 12);
}

function getCurrentLocalDateParts(timezone) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone || "UTC",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  }).formatToParts(new Date());
  const get = (type) => parts.find((p) => p.type === type)?.value;
  return {
    date: `${get("year")}-${get("month")}-${get("day")}`,
    hour: Number(get("hour") || 0),
    minute: Number(get("minute") || 0)
  };
}

function getNextTopOfHourLocalIso(timezone) {
  const nowLocal = getCurrentLocalDateParts(timezone || "UTC");
  let dateText = nowLocal.date;
  let hour = nowLocal.hour + 1;

  if (hour >= 24) {
    hour = 0;
    dateText = addDaysToDateText(dateText, 1);
  }

  return `${dateText}T${String(hour).padStart(2, "0")}:00`;
}

function monthDayLabel(dateText, tz) {
  return parsePlainDate(dateText).toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    timeZone: tz || "UTC"
  });
}

function weekdayMonthDayLabel(dateText, tz) {
  return parsePlainDate(dateText).toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    timeZone: tz || "UTC"
  });
}

function relativeMenuDayLabel(dateText, tz) {
  const today = getCurrentLocalDateParts(tz || "UTC").date;
  const tomorrow = addDaysToDateText(today, 1);

  if (dateText === today) return `Today, ${monthDayLabel(dateText, tz)}`;
  if (dateText === tomorrow) return `Tomorrow, ${monthDayLabel(dateText, tz)}`;
  return weekdayMonthDayLabel(dateText, tz);
}

function getGreetingForTime(timezone) {
  const hour = getNowHourInTz(timezone || "America/Toronto");
  if (hour < 12) return "Good morning";
  if (hour < 18) return "Good afternoon";
  return "Good evening";
}

function isBackKey(req) {
  return getDigits(req) === "*";
}

function parseRootMenuChoice(req) {
  return getDigits(req);
}

function parseMainMenuChoice(req) {
  return getDigits(req);
}

function parseAfterChoice(req) {
  return getDigits(req);
}

function parseForecastDayChoice(req) {
  const digit = getDigits(req);
  if (digit === "0") return "all";
  if (/^[1-9]$/.test(digit)) return parseInt(digit, 10) - 1;
  return -1;
}

function parseLocationChoice(req) {
  const digit = getDigits(req);
  if (/^[1-4]$/.test(digit)) return digit;
  if (digit === "9") return "9";
  return "";
}

function parseUSLocationChoice(req) {
  const digit = getDigits(req);
  if (/^[1-3]$/.test(digit)) return digit;
  return "";
}

function parseExchangeChoice(req) {
  const digit = getDigits(req);
  if (digit === "1") return { from: "USD", to: "CAD" };
  if (digit === "2") return { from: "CAD", to: "USD" };
  return null;
}

function parseBorderDirectionChoice(req) {
  const digit = getDigits(req);
  if (digit === "1") return "canada";
  if (digit === "2") return "us";
  return "";
}

function parseBorderTypeChoice(req) {
  const digit = getDigits(req);
  if (digit === "1") return "official";
  if (digit === "2") return "live";
  return "";
}

function parseAmountDigits(req) {
  return String(req.body.Digits || "").replace(/[^\d]/g, "").trim();
}

function cToF(value) {
  return (Number(value || 0) * 9) / 5 + 32;
}

function kmhToMph(value) {
  return Number(value || 0) * 0.621371;
}

function tempValueForUnit(value, unit) {
  return unit === "F" ? cToF(value) : Number(value || 0);
}

function speedValueForUnit(value, unit) {
  return unit === "F" ? kmhToMph(value) : Number(value || 0);
}

function speedUnitLabel(unit) {
  return unit === "F" ? "miles per hour" : "kilometres per hour";
}

function formatSignedTemp(value, unit = "C") {
  const n = Math.round(tempValueForUnit(value, unit));
  if (n < 0) return `minus ${Math.abs(n)}`;
  if (n > 0) return `${n}`;
  return "zero";
}

function formatForecastTempValue(value, unit = "C") {
  const n = Math.round(tempValueForUnit(value, unit));
  if (n < 0) return `minus ${Math.abs(n)}`;
  if (n > 0) return `plus ${n}`;
  return "zero";
}

function formatMoneyAmount(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return "0.00";
  return n.toFixed(2);
}

function currencySpeech(code) {
  if (code === "USD") return "U S dollars";
  if (code === "CAD") return "Canadian dollars";
  return String(code || "");
}

function exchangeAsOfTime(timezone = "America/Toronto") {
  return new Date().toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone: timezone
  });
}

function buildRootMenuInto(twiml) {
  const gather = twiml.gather(gatherOptions("/root-menu", 8, 1));
  say(
    gather,
    "Welcome to Weather and Info Line. Press 1 for weather. Press 2 for exchange rate. Press 4 for border wait time."
  );
  twiml.redirect({ method: "POST" }, "/root-menu-prompt");
}

function buildMainMenuInto(twiml, activeLocationName) {
  const gather = twiml.gather(gatherOptions("/menu", 8, 1));
  say(gather, `${activeLocationName}. Press 1 for the forecast menu. Press 2 for hourly forecast. Press 3 for current weather. Press star for the previous menu.`);
  twiml.redirect({ method: "POST" }, "/main-menu");
}

function buildBorderMenuInto(twiml) {
  const gather = twiml.gather(gatherOptions("/border-menu", 8, 1));
  say(
    gather,
    "Champlain border wait time. Press 1 for entering Canada. Press 2 for entering the United States. Press star for the previous menu."
  );
  twiml.redirect({ method: "POST" }, "/border-menu-prompt");
}

function buildBorderSubmenuInto(twiml, direction) {
  const gather = twiml.gather(gatherOptions("/border-submenu", 8, 1));

  if (direction === "canada") {
    say(
      gather,
      "Entering Canada. Press 1 for official border wait time. Press 2 for live traffic. Press star for the previous menu."
    );
  } else {
    say(
      gather,
      "Entering the United States. Press 1 for official border wait time. Press 2 for live traffic. Press star for the previous menu."
    );
  }

  twiml.redirect({ method: "POST" }, "/border-submenu-prompt");
}

function rootMenuTwiml() {
  const twiml = new VoiceResponse();
  buildRootMenuInto(twiml);
  return twiml;
}

function locationMenuTwiml({ allowBack = false, allowVoicemail = false } = {}) {
  const twiml = new VoiceResponse();
  const gather = twiml.gather(gatherOptions("/set-location-choice", 7, 1));
  const parts = ["Press 1 for Montreal.", "2 for Tosh.", "3 for Laurentians.", "4 for United States."];
  if (allowVoicemail) parts.push("Press 9 to leave a comment or suggestion.");
  if (allowBack) parts.push("Press star for the previous menu.");
  say(gather, parts.join(" "));
  twiml.redirect({ method: "POST" }, "/location-menu-prompt");
  return twiml;
}

function usLocationMenuTwiml() {
  const twiml = new VoiceResponse();
  const gather = twiml.gather(gatherOptions("/set-us-location-choice", 7, 1));
  say(gather, "For United States, press 1 for Brooklyn. 2 for Monroe. 3 for Monsey. Press star for the previous menu.");
  twiml.redirect({ method: "POST" }, "/us-location-menu-prompt");
  return twiml;
}

function exchangeMenuTwiml() {
  const twiml = new VoiceResponse();
  const gather = twiml.gather(gatherOptions("/set-exchange-choice", 7, 1));

  say(
    gather,
    "Press 1 to convert U S dollars to Canadian dollars. Press 2 to convert Canadian dollars to U S dollars. Press star for the previous menu."
  );

  twiml.redirect({ method: "POST" }, "/exchange-menu-prompt");
  return twiml;
}

function borderMenuTwiml() {
  const twiml = new VoiceResponse();
  buildBorderMenuInto(twiml);
  return twiml;
}

function exchangeAmountPromptTwiml(req, selection) {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    input: "dtmf",
    action: "/exchange-amount",
    method: "POST",
    timeout: 8,
    finishOnKey: "#"
  });

  const directionText =
    selection.from === "USD" ? "U S to Canadian" : "Canadian to U S";

  say(
    gather,
    `${directionText} exchange rate as of ${selection.asOfTime} is ${Number(selection.rate).toFixed(4)}. Enter amount of ${currencySpeech(selection.from)} you want to exchange then press pound. Press star to go back.`
  );

  twiml.redirect({ method: "POST" }, "/exchange-amount-prompt");
  return twiml;
}

function afterActionTwiml(req) {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    input: "dtmf",
    action: "/after",
    method: "POST",
    timeout: 6,
    numDigits: 1,
    finishOnKey: ""
  });

  const playback = getLastPlayback(req);

  if (playback?.type === "exchange" || playback?.type === "border") {
    say(gather, "Press star to go back. Press pound to repeat. Press 5 for the main menu.");
  } else {
    const unit = getUnitPreference(req);
    const unitText = unit === "F" ? "Press 7 to switch back to Celsius." : "Press 7 to hear it in Fahrenheit.";
    say(gather, `Press star to go back. Press pound to repeat. Press 5 for the main menu. ${unitText}`);
  }

  twiml.hangup();
  return twiml;
}

function voicemailPromptTwiml() {
  const twiml = new VoiceResponse();
  say(twiml, "Please leave your message after the beep. When you are finished, just hang up.");
  twiml.record({
    action: "/handle-recording",
    method: "POST",
    maxLength: 180,
    finishOnKey: "",
    playBeep: true,
    trim: "trim-silence",
    recordingStatusCallback: "/recording-status",
    recordingStatusCallbackMethod: "POST",
    recordingStatusCallbackEvent: ["completed"]
  });
  say(twiml, "I did not receive a recording.");
  twiml.redirect({ method: "POST" }, "/after-prompt");
  return twiml;
}

function playbackWithStarTwiml(text, options = {}) {
  const twiml = new VoiceResponse();
  const gather = twiml.gather({
    input: "dtmf",
    action: "/during-playback",
    method: "POST",
    timeout: 1,
    numDigits: 1,
    finishOnKey: ""
  });
  say(gather, text, options);
  twiml.redirect({ method: "POST" }, "/after-prompt");
  return twiml;
}

function weatherCodeToText(code, isNight = false) {
  const n = Number(code);
  if (n === 0) return isNight ? "clear" : "sunny";
  if (n === 1) return isNight ? "mostly clear" : "mostly sunny";
  if (n === 2) return isNight ? "partly cloudy" : "a mix of sun and cloud";
  if (n === 3) return "cloudy";
  const map = {
    45: "foggy",
    48: "freezing fog",
    51: "light drizzle",
    53: "drizzle",
    55: "heavy drizzle",
    56: "light freezing drizzle",
    57: "heavy freezing drizzle",
    61: "light rain",
    63: "rain",
    65: "heavy rain",
    66: "light freezing rain",
    67: "heavy freezing rain",
    71: "light snow",
    73: "snow",
    75: "heavy snow",
    77: "ice pellets",
    80: "light showers",
    81: "showers",
    82: "heavy showers",
    85: "light snow showers",
    86: "heavy snow showers",
    95: "thunderstorms",
    96: "thunderstorms with hail",
    99: "severe thunderstorms with hail"
  };
  return map[n] || "mixed weather";
}

function isNightHourFromIso(iso) {
  const hour = getHourInTz(iso);
  return hour < 6 || hour >= 18;
}

function degreesToCompass(deg) {
  const directions = ["north", "northeast", "east", "southeast", "south", "southwest", "west", "northwest"];
  const normalized = ((Number(deg || 0) % 360) + 360) % 360;
  return directions[Math.round(normalized / 45) % 8];
}

function compassLettersToWords(text) {
  const value = String(text || "").trim().toUpperCase();
  const map = {
    N: "north",
    NNE: "north northeast",
    NE: "northeast",
    ENE: "east northeast",
    E: "east",
    ESE: "east southeast",
    SE: "southeast",
    SSE: "south southeast",
    S: "south",
    SSW: "south southwest",
    SW: "southwest",
    WSW: "west southwest",
    W: "west",
    WNW: "west northwest",
    NW: "northwest",
    NNW: "north northwest"
  };
  return map[value] || value.toLowerCase();
}

function firstNonNull(values) {
  for (const value of values) {
    if (value !== undefined && value !== null && Number.isFinite(Number(value))) return Number(value);
  }
  return null;
}

function classifyHourlyBucket(item, tz = "UTC") {
  const code = Number(item.code);
  const rainAmount = Number(item.rain || 0) + Number(item.showers || 0);
  const snowAmount = Number(item.snow || 0);
  const wind = Number(item.wind || 0);
  const precipChance = Number(item.rainChance || 0);
  const isNight = isNightHourFromIso(item.time, tz);
  let condition = weatherCodeToText(code, isNight);

  if (snowAmount > 0) condition = snowAmount >= 1.0 ? "snow" : "light snow";
  else if (rainAmount > 0) {
    condition = [80, 81, 82].includes(code)
      ? (rainAmount >= 1.5 ? "showers" : "light showers")
      : (rainAmount >= 1.5 ? "rain" : "light rain");
  }

  let precipTag = "dry";
  if ([95, 96, 99].includes(code)) precipTag = "storm";
  else if (snowAmount > 0.02 || [71, 73, 75, 77, 85, 86].includes(code)) precipTag = snowAmount >= 1.0 ? "snow" : "light-snow";
  else if (rainAmount > 0.05 || precipChance >= 35 || [51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82].includes(code)) precipTag = rainAmount >= 1.5 ? "rain" : "light-rain";

  let windTag = "calm";
  if (wind >= 28) windTag = "windy";
  else if (wind >= 18) windTag = "breezy";

  const tempBand = Math.round(Number(item.temp || 0) / 2);
  return `${condition}|${precipTag}|${windTag}|${tempBand}`;
}

function summarizeHourlyBlock(block, tz, unit = "C") {
  const first = block.items[0];
  const start = formatLocalIsoTimeLabel(block.start);
  const end = formatLocalIsoTimeLabel(addHoursToLocalIso(block.end, 1));
  const avgTempC = block.items.reduce((sum, x) => sum + Number(x.temp || 0), 0) / block.items.length;
  const avgAppTempC = block.items.reduce((sum, x) => sum + Number(x.apparentTemp ?? x.temp ?? 0), 0) / block.items.length;
  const avgTemp = Math.round(tempValueForUnit(avgTempC, unit));
  const avgAppTemp = Math.round(tempValueForUnit(avgAppTempC, unit));
  const maxRainChance = Math.max(...block.items.map((x) => Number(x.rainChance || 0)));
  const maxWind = Math.max(...block.items.map((x) => speedValueForUnit(x.wind || 0, unit)));
  const maxGust = Math.max(...block.items.map((x) => speedValueForUnit(x.gusts || 0, unit)));
  const totalRain = block.items.reduce((sum, x) => sum + Number(x.rain || 0) + Number(x.showers || 0), 0);
  const totalSnow = block.items.reduce((sum, x) => sum + Number(x.snow || 0), 0);
  const isNight = isNightHourFromIso(first.time, tz);
  const spokenCondition = weatherCodeToText(first.code, isNight);

  const parts = [`From ${start} until ${end}, ${spokenCondition}. Around ${avgTemp} degrees.`];

  if (Math.abs(avgAppTemp - avgTemp) >= 3) {
    parts.push(`Feels like ${formatSignedTemp(avgAppTempC, unit)}.`);
  }
  if (maxRainChance >= 45 && totalRain <= 0.05 && totalSnow <= 0.02) {
    parts.push(`${Math.round(maxRainChance)} percent chance of precipitation.`);
  }
  if (totalRain > 0.05) {
    parts.push(totalRain >= 3 ? "Rain at times." : "Light rain or a few showers.");
  }
  if (totalSnow > 0.02) {
    parts.push(totalSnow >= 2 ? "Snow at times." : "Light snow or flurries.");
  }
  if (maxWind >= (unit === "F" ? 22 : 35) || maxGust >= (unit === "F" ? 34 : 55)) {
    parts.push("Windy.");
  } else if (maxWind >= (unit === "F" ? 11 : 18)) {
    parts.push("A light breeze.");
  }

  return parts.join(" ");
}

function buildSmartHourlyBlocks(items, tz = "UTC") {
  if (!items.length) return [];
  const blocks = [];
  let current = {
    key: classifyHourlyBucket(items[0], tz),
    start: items[0].time,
    end: items[0].time,
    items: [items[0]]
  };

  for (let i = 1; i < items.length; i++) {
    const key = classifyHourlyBucket(items[i], tz);
    if (key === current.key) {
      current.end = items[i].time;
      current.items.push(items[i]);
    } else {
      blocks.push(current);
      current = {
        key,
        start: items[i].time,
        end: items[i].time,
        items: [items[i]]
      };
    }
  }

  blocks.push(current);
  return blocks;
}

function stripXmlTags(text) {
  return String(text || "")
    .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
    .replace(/<[^>]+>/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/\s+/g, " ")
    .trim();
}

function firstMatch(text, regex) {
  const match = String(text || "").match(regex);
  return match ? stripXmlTags(match[1]) : "";
}

function summarizeAlertText(text, maxLength = 260) {
  const clean = stripXmlTags(text);
  if (!clean) return "";
  if (clean.length <= maxLength) return clean;
  const shortened = clean.slice(0, maxLength);
  const cut = shortened.lastIndexOf(". ");
  return cut > 80 ? shortened.slice(0, cut + 1).trim() : `${shortened.trim()}...`;
}

async function fetchCanadianAlert(location) {
  if (!location || location.country !== "CA" || !location.alertFeedUrl) return null;
  const cacheKey = location.alertFeedUrl;
  const cached = canadaAlertCache.get(cacheKey);
  const now = Date.now();
  if (cached && now - cached.timestamp < ALERT_CACHE_MS) return cached.value;

  try {
    const response = await axios.get(location.alertFeedUrl, {
      timeout: EC_ALERT_TIMEOUT_MS,
      headers: { "User-Agent": "weather-line-canada/1.0" }
    });
    const xml = String(response.data || "");
    const items = xml.match(/<item\b[\s\S]*?<\/item>/gi) || [];
    let alertValue = null;

    if (items.length > 0) {
      const firstItem = items[0];
      const title = firstMatch(firstItem, /<title[^>]*>([\s\S]*?)<\/title>/i);
      const description = firstMatch(firstItem, /<description[^>]*>([\s\S]*?)<\/description>/i);
      const combined = `${title} ${description}`.toLowerCase();
      const noAlert =
        combined.includes("no watches or warnings in effect") ||
        combined.includes("no alerts in effect") ||
        combined.includes("no warning or watch in effect");
      if (!noAlert) {
        alertValue = {
          title: summarizeAlertText(title, 120),
          description: summarizeAlertText(description, 260)
        };
      }
    }

    canadaAlertCache.set(cacheKey, { value: alertValue, timestamp: now });
    return alertValue;
  } catch (error) {
    console.error("Canada alert fetch failed:", error.message);
    return null;
  }
}

function buildCanadianAlertSpeech(location, alert) {
  if (!location || !alert) return "";
  const parts = [`Environment Canada alert for ${placeLabel(location)}.`];
  if (alert.title) parts.push(alert.title);
  if (alert.description) parts.push(alert.description);
  return parts.join(" ");
}

function cacheGet(map, key, ttl) {
  const hit = map.get(key);
  if (!hit) return null;
  if (Date.now() - hit.timestamp > ttl) {
    map.delete(key);
    return null;
  }
  return hit.value;
}

function cacheSet(map, key, value) {
  map.set(key, { timestamp: Date.now(), value });
  return value;
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function safeNumber(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function formatDateToLocalIso(date, timezone) {
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: timezone || "America/Toronto",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false
  }).formatToParts(date);

  const get = (type) => parts.find((p) => p.type === type)?.value;
  return `${get("year")}-${get("month")}-${get("day")}T${get("hour")}:${get("minute")}`;
}

function formatEcLocalIso(raw, timezone = "America/Toronto") {
  const text = String(raw || "").trim();
  if (!text) return "";

  if (/[zZ]$|[+\-]\d{2}:\d{2}$/.test(text)) {
    const parsed = new Date(text);
    if (!Number.isNaN(parsed.getTime())) {
      return formatDateToLocalIso(parsed, timezone);
    }
  }

  const normalized = text.replace(" ", "T");
  return normalized.length >= 16 ? normalized.slice(0, 16) : normalized;
}

function buildEcBbox(lat, lon, km = 60) {
  const latDelta = km / 111;
  const lonDelta = km / (111 * Math.cos((Number(lat) * Math.PI) / 180));
  return [lon - lonDelta, lat - latDelta, lon + lonDelta, lat + latDelta].join(",");
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (n) => (Number(n) * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

async function ecApiGet(url, params = {}) {
  const started = Date.now();
  const response = await axios.get(url, {
    params,
    timeout: EC_API_TIMEOUT_MS,
    headers: {
      "User-Agent": "weather-line-ec/3.0",
      Accept: "application/json"
    }
  });
  console.log("EC API OK", url, "ms=", Date.now() - started);
  return response.data;
}

async function findNearestEcCityPagePoint(location) {
  const bbox = buildEcBbox(location.latitude, location.longitude, 120);
  const data = await ecApiGet(
    "https://api.weather.gc.ca/collections/citypageweather-realtime/items",
    { f: "json", lang: "en", bbox, limit: 250 }
  );

  const features = safeArray(data?.features)
    .map((f) => {
      const coords = safeArray(f?.geometry?.coordinates);
      const lon = Number(coords[0]);
      const lat = Number(coords[1]);
      return {
        feature: f,
        distanceKm: haversineKm(location.latitude, location.longitude, lat, lon)
      };
    })
    .sort((a, b) => a.distanceKm - b.distanceKm);

  if (!features.length) throw new Error(`No Environment Canada city page point found for ${placeLabel(location)}`);
  const chosen = features[0].feature;
  return { id: String(chosen.id || ""), distanceKm: features[0].distanceKm };
}

async function fetchEnvironmentCanadaCityPage(location) {
  if (!location || location.country !== "CA") return null;

  const cacheKey = `ec-citypage:${location.ecCityPageId || cacheKeyForLocation(location)}`;
  const cached = cacheGet(ecCityPageCache, cacheKey, EC_CITYPAGE_CACHE_MS);
  if (cached) return cached;

  let pointId = location.ecCityPageId || "";

  if (!pointId) {
    const nearest = await findNearestEcCityPagePoint(location);
    pointId = nearest.id;
    location.ecCityPageId = pointId;
    console.log("Resolved EC city page point:", location.label, "=>", pointId, "distanceKm=", nearest.distanceKm);
  }

  const item = await ecApiGet(
    `https://api.weather.gc.ca/collections/citypageweather-realtime/items/${encodeURIComponent(pointId)}`,
    { f: "json", lang: "en" }
  );

  return cacheSet(ecCityPageCache, cacheKey, item);
}

function mapEcConditionToCode(text) {
  const s = String(text || "").toLowerCase();
  if (!s) return 2;
  if (/thunder/.test(s)) return 95;
  if (/freezing rain/.test(s)) return 66;
  if (/ice pellets/.test(s)) return 77;
  if (/snow showers/.test(s)) return 85;
  if (/snow|flurr|blowing snow/.test(s)) return 73;
  if (/showers/.test(s)) return 81;
  if (/drizzle/.test(s)) return 53;
  if (/rain/.test(s)) return 63;
  if (/fog/.test(s)) return 45;
  if (/mostly sunny|mostly clear/.test(s)) return 1;
  if (/partly cloudy|mix of sun and cloud/.test(s)) return 2;
  if (/cloudy|overcast|mainly cloudy|mostly cloudy/.test(s)) return 3;
  if (/clear|sunny/.test(s)) return 0;
  return 2;
}

function formatIssuedTime(rawTimestamp, timezone) {
  const text = String(rawTimestamp || "").trim();
  if (!text) return "";
  const parsed = new Date(text);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleTimeString("en-US", {
      hour: "numeric",
      minute: "2-digit",
      hour12: true,
      timeZone: timezone || "America/Toronto"
    });
  }
  const iso = formatEcLocalIso(text, timezone || "America/Toronto");
  if (iso) return formatLocalIsoTimeLabel(iso);
  return text;
}

function ecIssuedSpeechLine(label, rawTimestamp, timezone) {
  const text = String(rawTimestamp || "").trim();
  if (!text) return `${label}.`;
  return `${label} at ${formatIssuedTime(text, timezone)} on ${issuedMonthDayLabel(text, timezone)}.`;
}

function normalizeEcCurrent(cityItem) {
  const cc = cityItem?.properties?.currentConditions || {};
  const condition = cc?.condition?.en || "";
  return {
    issuedAt: cc?.timestamp?.en || "",
    stationName: cc?.station?.value?.en || cc?.station?.value || "",
    condition,
    temperatureC: safeNumber(cc?.temperature?.value?.en ?? cc?.temperature?.value),
    windChillC: safeNumber(cc?.windChill?.value?.en ?? cc?.windChill?.value),
    humidexC: safeNumber(cc?.humidex?.value?.en ?? cc?.humidex?.value),
    windSpeedKmh: safeNumber(cc?.wind?.speed?.value?.en ?? cc?.wind?.speed?.value),
    windGustKmh: safeNumber(cc?.wind?.gust?.value?.en ?? cc?.wind?.gust?.value),
    windDirectionText: cc?.wind?.direction?.value?.en || cc?.wind?.direction?.en || cc?.wind?.direction?.value || "",
    windBearing: safeNumber(cc?.wind?.bearing?.value?.en ?? cc?.wind?.bearing?.value),
    weatherCode: mapEcConditionToCode(condition)
  };
}

function normalizeEcHourly(cityItem, timezone = "America/Toronto") {
  const rows = safeArray(cityItem?.properties?.hourlyForecastGroup?.hourlyForecasts);
  return rows
    .map((row) => {
      const condition = String(row?.condition?.en || row?.condition || "").toLowerCase();
      const windPeriod = safeArray(row?.winds?.periods)?.[0] || null;
      const temp = safeNumber(row?.temperature?.value?.en ?? row?.temperature?.value);
      const lop = safeNumber(row?.lop?.value?.en ?? row?.lop?.value) || 0;
      const uv = safeNumber(row?.uv?.index?.value?.en ?? row?.uv?.index?.value) || 0;
      const windSpeed = safeNumber(windPeriod?.speed?.value?.en ?? windPeriod?.speed?.value) || 0;
      const windGust = safeNumber(windPeriod?.gust?.value?.en ?? windPeriod?.gust?.value) || windSpeed;
      const windBearing = safeNumber(windPeriod?.bearing?.value?.en ?? windPeriod?.bearing?.value) || 0;

      return {
        rawTime: String(row?.timestamp || "").trim(),
        time: formatEcLocalIso(row?.timestamp || "", timezone),
        temperature_2m: temp,
        apparent_temperature: temp,
        precipitation_probability: lop,
        rain: /rain|drizzle|showers|thunder/.test(condition) && !/snow|flurr/.test(condition) ? (lop >= 60 ? 1 : 0) : 0,
        showers: /showers/.test(condition) ? (lop >= 60 ? 1 : 0) : 0,
        snowfall: /snow|flurr|ice pellets|blowing snow/.test(condition) ? (lop >= 60 ? 1 : 0) : 0,
        cloud_cover: /clear/.test(condition) ? 5 : /sun|few clouds/.test(condition) ? 20 : /partly/.test(condition) ? 45 : /mainly cloudy|mostly cloudy/.test(condition) ? 70 : 90,
        wind_speed_10m: windSpeed,
        wind_gusts_10m: windGust,
        wind_direction_10m: windBearing,
        uv_index: uv,
        weather_code: mapEcConditionToCode(condition),
        condition
      };
    })
    .filter((x) => x.time && Number.isFinite(Number(x.temperature_2m)));
}

function normalizeEcForecastPeriods(ecItem) {
  const issueTime = ecItem?.properties?.forecastGroup?.timestamp?.en || "";
  const periods = safeArray(ecItem?.properties?.forecastGroup?.forecasts);

  return {
    issuedAt: issueTime,
    periods: periods.map((p, index) => ({
      index,
      periodName: p?.period?.textForecastName?.en || p?.period?.name?.en || p?.title?.en || `Period ${index + 1}`,
      abbreviatedSummary: p?.abbreviatedForecast?.textSummary?.en || "",
      fullSummary: p?.textSummary?.en || p?.abbreviatedForecast?.textSummary?.en || "",
      summary:
        p?.textSummary?.en ||
        p?.abbreviatedForecast?.textSummary?.en ||
        p?.precipitation?.textSummary?.en ||
        "",
      pop:
        safeNumber(p?.abbreviatedForecast?.pop?.value) ??
        safeNumber(p?.pop?.value) ??
        null,
      temperature:
        safeNumber(p?.temperatures?.temperature?.textSummary?.value) ??
        safeNumber(p?.temperatures?.temperature?.value) ??
        safeNumber(p?.temperature?.value) ??
        null,
      windText: p?.winds?.textSummary?.en || p?.wind?.textSummary?.en || "",
      uvText: p?.uv?.textSummary?.en || "",
      tempText: p?.temperatures?.textSummary?.en || ""
    }))
  };
}

async function fetchOpenMeteoForecast(location) {
  const params = {
    latitude: Number(location.latitude),
    longitude: Number(location.longitude),
    apikey: process.env.OPEN_METEO_API_KEY,
    current: ["temperature_2m", "apparent_temperature", "rain", "showers", "snowfall", "cloud_cover", "wind_speed_10m", "wind_gusts_10m", "wind_direction_10m", "weather_code"].join(","),
    hourly: ["temperature_2m", "apparent_temperature", "precipitation_probability", "rain", "showers", "snowfall", "cloud_cover", "wind_speed_10m", "wind_gusts_10m", "wind_direction_10m", "uv_index", "weather_code"].join(","),
    daily: ["weather_code", "temperature_2m_max", "temperature_2m_min", "precipitation_probability_max", "rain_sum", "showers_sum", "snowfall_sum", "wind_speed_10m_max", "wind_gusts_10m_max", "uv_index_max"].join(","),
    timezone: location.timezone || "America/Toronto",
    forecast_days: 7,
    temperature_unit: "celsius",
    wind_speed_unit: "kmh",
    precipitation_unit: "mm"
  };

  const response = await axios.get("https://customer-api.open-meteo.com/v1/forecast", {
    params,
    timeout: OPEN_METEO_TIMEOUT_MS
  });

  const data = response.data;
  if (!data || !data.current || !data.hourly || !data.daily) {
    throw new Error("Open-Meteo returned incomplete forecast data");
  }

  if (!data.hourly.weather_code && data.hourly.weathercode) data.hourly.weather_code = data.hourly.weathercode;
  if (!data.daily.weather_code && data.daily.weathercode) data.daily.weather_code = data.daily.weathercode;

  data.source = "open-meteo";
  return data;
}

async function fetchEnvironmentCanadaData(location) {
  const started = Date.now();
  console.log("EC fetch start:", location.label);

  const item = await fetchEnvironmentCanadaCityPage(location);

  const result = {
    source: "environment-canada-citypage",
    item,
    current: normalizeEcCurrent(item),
    hourly: normalizeEcHourly(item, location.timezone || "America/Toronto"),
    forecastPeriods: normalizeEcForecastPeriods(item)
  };

  console.log("EC fetch done:", location.label, "ms=", Date.now() - started);
  return result;
}

function ecCurrentWeatherSpeech(location, ecData, unit = "C") {
  const c = ecData.current;
  const parts = [
    `Current weather for ${placeLabel(location)}.`,
    ecIssuedSpeechLine("Issued by Environment Canada", c.issuedAt, location.timezone)
  ];

  if (c.condition) parts.push(`It is ${String(c.condition).toLowerCase()}.`);
  if (Number.isFinite(c.temperatureC)) parts.push(`Temperature ${formatSignedTemp(c.temperatureC, unit)} degrees.`);

  const apparent = c.windChillC ?? c.humidexC;
  if (
    Number.isFinite(apparent) &&
    Number.isFinite(c.temperatureC) &&
    Math.round(tempValueForUnit(apparent, unit)) !== Math.round(tempValueForUnit(c.temperatureC, unit))
  ) {
    parts.push(`Feels like ${formatSignedTemp(apparent, unit)}.`);
  }

  if (Number.isFinite(c.windSpeedKmh) && c.windSpeedKmh > 0) {
    const speed = Math.round(speedValueForUnit(c.windSpeedKmh, unit));
    const dir = c.windDirectionText ? compassLettersToWords(c.windDirectionText) : degreesToCompass(c.windBearing || 0);
    if (Number.isFinite(c.windGustKmh) && c.windGustKmh > c.windSpeedKmh + 5) {
      const gust = Math.round(speedValueForUnit(c.windGustKmh, unit));
      parts.push(`Wind ${dir} ${speed} ${speedUnitLabel(unit)}, gusting to ${gust}.`);
    } else {
      parts.push(`Wind ${dir} ${speed} ${speedUnitLabel(unit)}.`);
    }
  }

  return parts.join(" ");
}

function chooseBestEcSummary(period) {
  const full = String(period.fullSummary || "").trim();
  const short = String(period.abbreviatedSummary || "").trim();
  if (full && full.length >= short.length) return full;
  return full || short || "";
}

function periodShouldGetExtraDetail(period) {
  const name = String(period.periodName || "").toLowerCase();
  return /tonight|today/.test(name) || period.index <= 2;
}

function classifyPrecipTypeFromHourlyRow(row) {
  const condition = String(row.condition || "").toLowerCase();
  const snowSignal = Number(row.snowfall || 0) > 0 || /snow|flurr|ice pellets|blowing snow/.test(condition);
  const rainSignal = Number(row.rain || 0) > 0 || Number(row.showers || 0) > 0 || /rain|drizzle|showers|thunder/.test(condition);

  if (snowSignal && rainSignal) return "mixed precipitation";
  if (snowSignal) return "snow";
  if (rainSignal) return /showers/.test(condition) ? "showers" : "rain";
  if (Number(row.precipitation_probability || 0) >= 40) {
    if (/snow|flurr/.test(condition)) return "snow";
    if (/rain|drizzle|showers|thunder/.test(condition)) return "rain";
  }
  return "";
}

function buildPeriodWindowFromIndex(period, location) {
  const nowLocal = getCurrentLocalDateParts(location.timezone || "America/Toronto");
  const today = nowLocal.date;
  const tomorrow = addDaysToDateText(today, 1);
  const dayAfter = addDaysToDateText(today, 2);
  const name = String(period.periodName || "").toLowerCase();

  if (period.index === 0 && /tonight/.test(name)) {
    return {
      start: `${today}T${String(nowLocal.hour).padStart(2, "0")}:${String(nowLocal.minute).padStart(2, "0")}`,
      end: `${tomorrow}T06:00`
    };
  }

  if (period.index === 0 && /today/.test(name)) {
    return {
      start: `${today}T${String(nowLocal.hour).padStart(2, "0")}:${String(nowLocal.minute).padStart(2, "0")}`,
      end: `${today}T18:00`
    };
  }

  if (period.index === 1 && !/night/.test(name)) {
    return { start: `${tomorrow}T06:00`, end: `${tomorrow}T18:00` };
  }

  if (period.index === 2 && /night/.test(name)) {
    return { start: `${tomorrow}T18:00`, end: `${dayAfter}T06:00` };
  }

  return null;
}

function inferTimingFromHourlyForPeriod(location, ecData, period) {
  const window = buildPeriodWindowFromIndex(period, location);
  if (!window) return "";

  const rows = safeArray(ecData.hourly).filter((row) => row.time >= window.start && row.time < window.end);
  if (!rows.length) return "";

  const precipRows = rows.filter((row) => {
    const type = classifyPrecipTypeFromHourlyRow(row);
    return !!type;
  });

  if (!precipRows.length) return "";

  const typeCounts = new Map();
  for (const row of precipRows) {
    const type = classifyPrecipTypeFromHourlyRow(row);
    typeCounts.set(type, (typeCounts.get(type) || 0) + 1);
  }

  const dominantType = [...typeCounts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || "precipitation";
  const start = precipRows[0].time;
  const end = addHoursToLocalIso(precipRows[precipRows.length - 1].time, 1);

  const startText = formatLocalIsoTimeLabel(start);
  const endText = formatLocalIsoTimeLabel(end);

  return `${dominantType.charAt(0).toUpperCase() + dominantType.slice(1)} likely starting around ${startText} and easing off near ${endText}.`;
}

function ecPeriodSpeech(location, ecData, period, unit = "C") {
  const parts = [];
  const summary = chooseBestEcSummary(period);
  const nearTerm = periodShouldGetExtraDetail(period);

  if (period.periodName) parts.push(`${period.periodName}.`);
  if (summary) parts.push(summary);

  const timing = nearTerm ? inferTimingFromHourlyForPeriod(location, ecData, period) : "";
  if (timing) {
    const joined = parts.join(" ").toLowerCase();
    const timingLower = timing.toLowerCase();
    if (!joined.includes(timingLower)) parts.push(timing);
  }

  if (period.tempText) {
    const joined = parts.join(" ").toLowerCase();
    const tempTextLower = String(period.tempText).toLowerCase();
    if (!joined.includes(tempTextLower)) parts.push(period.tempText);
  }

  if (period.windText) {
    const joined = parts.join(" ").toLowerCase();
    const windTextLower = String(period.windText).toLowerCase();
    if (!joined.includes(windTextLower)) parts.push(period.windText);
  }

  if (period.uvText) {
    const joined = parts.join(" ").toLowerCase();
    const uvTextLower = String(period.uvText).toLowerCase();
    if (!joined.includes(uvTextLower)) parts.push(period.uvText);
  }

  if (Number.isFinite(period.temperature)) {
    const joined = ` ${parts.join(" ").toLowerCase()} `;
    const alreadyHasHighLow = joined.includes(" high ") || joined.includes(" low ");
    if (!alreadyHasHighLow) {
      parts.push(`${/night|tonight/i.test(period.periodName) ? "Low" : "High"} ${formatForecastTempValue(period.temperature, unit)}.`);
    }
  }

  if (Number.isFinite(period.pop) && period.pop > 0 && period.pop < 100) {
    const joined = parts.join(" ").toLowerCase();
    const alreadyHasPop =
      joined.includes("percent chance") ||
      joined.includes("chance of precipitation") ||
      joined.includes("chance of flurries") ||
      joined.includes("chance of showers") ||
      joined.includes("chance of rain") ||
      joined.includes("chance of snow");

    if (!alreadyHasPop) {
      parts.push(`${Math.round(period.pop)} percent chance of precipitation.`);
    }
  }

  return parts.join(" ");
}

function buildEcDailyGroups(ecData, location) {
  const periods = ecData?.forecastPeriods?.periods || [];
  const tz = location?.timezone || "America/Toronto";
  const nowLocal = getCurrentLocalDateParts(tz);
  const today = nowLocal.date;

  if (!periods.length) return [];

  const groups = [];
  let currentDate = today;
  let startIndex = 0;

  const firstLower = String(periods[0]?.periodName || "").trim().toLowerCase();

  if (firstLower === "tonight" && nowLocal.hour < 6) {
    startIndex = 1;
  }

  const usablePeriods = periods.slice(startIndex);

  for (let i = 0; i < usablePeriods.length; i++) {
    const period = usablePeriods[i];
    const lower = String(period?.periodName || "").trim().toLowerCase();
    const isNight = /night|tonight/.test(lower);

    if (isNight) {
      const lastGroup = groups[groups.length - 1];

      if (lastGroup && lastGroup.dateText === currentDate) {
        lastGroup.periods.push(period);
      } else {
        groups.push({
          dateText: currentDate,
          label: relativeMenuDayLabel(currentDate, tz),
          periods: [period]
        });
      }

      currentDate = addDaysToDateText(currentDate, 1);
      continue;
    }

    groups.push({
      dateText: currentDate,
      label: relativeMenuDayLabel(currentDate, tz),
      periods: [period]
    });
  }

  return groups;
}

function ecHourlySpeech(location, ecData, hours = 12, unit = "C") {
  const rows = ecData.hourly || [];
  const tz = location.timezone || "America/Toronto";
  const nextHourStart = getNextTopOfHourLocalIso(tz);

  const nextHourDate = getDatePartFromLocalIso(nextHourStart);
  const nextHourHour = getHourFromLocalIso(nextHourStart);
  const nextHourMinute = getMinuteFromLocalIso(nextHourStart);

  const future = rows.filter((row) => {
    const rowTime = String(row.time || "").trim();
    if (!rowTime) return false;

    const rowDate = getDatePartFromLocalIso(rowTime);
    const rowHour = getHourFromLocalIso(rowTime);
    const rowMinute = getMinuteFromLocalIso(rowTime);

    return (
      rowDate > nextHourDate ||
      (rowDate === nextHourDate && rowHour > nextHourHour) ||
      (rowDate === nextHourDate && rowHour === nextHourHour && rowMinute >= nextHourMinute)
    );
  });

  const slice = future.slice(0, hours);

  if (!slice.length) {
    return `I could not find hourly forecast data for ${placeLabel(location)}.`;
  }

  const compact = slice.map((row) => ({
    time: row.time,
    temp: row.temperature_2m,
    apparentTemp: row.apparent_temperature,
    rainChance: row.precipitation_probability,
    rain: row.rain,
    showers: row.showers,
    snow: row.snowfall,
    clouds: row.cloud_cover,
    wind: row.wind_speed_10m,
    gusts: row.wind_gusts_10m,
    direction: row.wind_direction_10m,
    code: row.weather_code
  }));

  const blocks = buildSmartHourlyBlocks(compact, tz);
  const spoken = blocks
    .slice(0, 6)
    .map((b) => summarizeHourlyBlock(b, tz, unit))
    .join(" ");

  return [
    ecIssuedSpeechLine(
      "Issued by Environment Canada",
      ecData.forecastPeriods?.issuedAt || ecData.current?.issuedAt,
      tz
    ),
    `Here is the next ${slice.length} hour forecast for ${placeLabel(location)}.`,
    spoken
  ].join(" ");
}

function ecSingleForecastSpeech(location, ecData, index, unit = "C") {
  const groups = buildEcDailyGroups(ecData, location);
  if (index < 0 || index >= groups.length) {
    return `That forecast day is not available for ${placeLabel(location)}.`;
  }

  const group = groups[index];
  const parts = [
    `Forecast for ${group.label}.`,
    ecIssuedSpeechLine("Issued by Environment Canada", ecData.forecastPeriods?.issuedAt, location.timezone)
  ];

  for (const period of group.periods) {
    parts.push(ecPeriodSpeech(location, ecData, period, unit));
  }

  return parts.join(" ");
}

function ecAllForecastSpeech(location, ecData, unit = "C") {
  const periods = ecData.forecastPeriods?.periods || [];
  if (!periods.length) return `I could not find forecast periods for ${placeLabel(location)}.`;

  const parts = [
    `Forecast for ${placeLabel(location)}.`,
    ecIssuedSpeechLine("Issued by Environment Canada", ecData.forecastPeriods?.issuedAt, location.timezone)
  ];

  for (const period of periods) {
    parts.push(ecPeriodSpeech(location, ecData, period, unit));
  }

  return parts.join(" ");
}

function describeCurrentWeatherSentence(current, timezone) {
  const isNight = current?.time ? isNightHourFromIso(current.time, timezone) : false;
  const condition = weatherCodeToText(current.weather_code, isNight);
  if (condition === "sunny") return "It is sunny";
  if (condition === "mostly sunny") return "It is mostly sunny";
  if (condition === "a mix of sun and cloud") return "There is a mix of sun and cloud";
  if (condition === "clear") return "It is clear";
  if (condition === "mostly clear") return "It is mostly clear";
  if (condition === "partly cloudy") return "It is partly cloudy";
  if (condition === "cloudy") return "It is cloudy";
  if (condition === "foggy") return "It is foggy";
  return `It is ${condition}`;
}

function currentWeatherSpeech(location, forecast, unit = "C") {
  const c = forecast.current || {};
  const parts = [
    `Current weather for ${placeLabel(location)}.`,
    `Retrieved at ${retrievedTimeLabelNow(location.timezone)}.`,
    `${describeCurrentWeatherSentence(c, location.timezone)}.`,
    `Temperature ${formatSignedTemp(c.temperature_2m, unit)} degrees.`
  ];

  const apparent = firstNonNull([c.apparent_temperature]);
  if (apparent !== null && Math.round(tempValueForUnit(apparent, unit)) !== Math.round(tempValueForUnit(Number(c.temperature_2m || 0), unit))) {
    parts.push(`Feels like ${formatSignedTemp(apparent, unit)}.`);
  }

  const speed = Math.round(speedValueForUnit(c.wind_speed_10m, unit));
  const gusts = Math.round(speedValueForUnit(c.wind_gusts_10m, unit));
  const direction = degreesToCompass(c.wind_direction_10m);
  if (speed >= (unit === "F" ? 9 : 15)) {
    parts.push(
      gusts >= speed + (unit === "F" ? 6 : 10)
        ? `Wind ${direction} ${speed} ${speedUnitLabel(unit)}, gusting to ${gusts}.`
        : `Wind ${direction} ${speed} ${speedUnitLabel(unit)}.`
    );
  }

  return parts.join(" ");
}

function nextHoursSpeech(location, forecast, hours = 6, unit = "C") {
  const h = forecast.hourly || {};
  const tz = location.timezone || "UTC";
  const items = [];
  const nowLocal = getCurrentLocalDateParts(tz);

  for (let i = 0; i < (h.time || []).length; i++) {
    const t = String(h.time[i] || "");
    const datePart = getDatePartFromLocalIso(t);
    const hourPart = getHourFromLocalIso(t);
    const minutePart = getMinuteFromLocalIso(t);
    const isFuture =
      datePart > nowLocal.date ||
      (datePart === nowLocal.date && hourPart > nowLocal.hour) ||
      (datePart === nowLocal.date && hourPart === nowLocal.hour && minutePart >= nowLocal.minute);

    if (isFuture) {
      items.push({
        time: h.time[i],
        temp: h.temperature_2m?.[i],
        apparentTemp: h.apparent_temperature?.[i],
        rainChance: h.precipitation_probability?.[i],
        rain: h.rain?.[i],
        showers: h.showers?.[i],
        snow: h.snowfall?.[i],
        clouds: h.cloud_cover?.[i],
        wind: h.wind_speed_10m?.[i],
        gusts: h.wind_gusts_10m?.[i],
        direction: h.wind_direction_10m?.[i],
        code: h.weather_code?.[i]
      });
    }
    if (items.length >= hours) break;
  }

  if (!items.length) return `I could not find future hourly forecast data for ${placeLabel(location)}.`;

  const blocks = buildSmartHourlyBlocks(items, tz);
  const body = blocks.slice(0, 4).map((block) => summarizeHourlyBlock(block, tz, unit)).join(" ");
  return `Here is the next ${items.length} hours for ${placeLabel(location)}. ${body}`.trim();
}

function dailyForecastSpeech(location, forecast, index, unit = "C") {
  const d = forecast.daily || {};
  if (index < 0 || index >= (d.time || []).length) return `That forecast day is not available for ${placeLabel(location)}.`;

  const dateText = fullDateLabel(d.time[index], location.timezone);
  const high = d.temperature_2m_max?.[index];
  const low = d.temperature_2m_min?.[index];
  const code = d.weather_code?.[index];
  const summary = weatherCodeToText(code, false);
  const parts = [`Forecast for ${dateText}.`, `Conditions ${summary}.`];

  if (Number.isFinite(Number(high))) parts.push(`High ${formatForecastTempValue(high, unit)}.`);
  if (Number.isFinite(Number(low))) parts.push(`Low ${formatForecastTempValue(low, unit)}.`);
  if (Number.isFinite(Number(d.precipitation_probability_max?.[index]))) {
    parts.push(`${Math.round(Number(d.precipitation_probability_max[index]))} percent chance of precipitation.`);
  }
  if (Number.isFinite(Number(d.wind_speed_10m_max?.[index]))) {
    parts.push(`Wind up to ${Math.round(speedValueForUnit(d.wind_speed_10m_max[index], unit))} ${speedUnitLabel(unit)}.`);
  }

  return parts.join(" ");
}

function sevenDayForecastSpeech(location, forecast, unit = "C") {
  const d = forecast.daily || {};
  const count = Math.min(7, (d.time || []).length);
  const parts = [
    `Issued on ${issuedDateLabelToday(location.timezone)}.`,
    `Retrieved at ${retrievedTimeLabelNow(location.timezone)}.`,
    `Here is the 7 day forecast for ${placeLabel(location)}.`
  ];
  for (let i = 0; i < count; i++) parts.push(dailyForecastSpeech(location, forecast, i, unit));
  return parts.join(" ");
}

function getExchangeCacheKey(from, to) {
  return `${String(from || "").toUpperCase()}_${String(to || "").toUpperCase()}`;
}

function getCachedExchangeRate(from, to) {
  const key = getExchangeCacheKey(from, to);
  const cached = exchangeRateCache.get(key);
  if (!cached) return null;

  const age = Date.now() - cached.timestamp;
  if (age <= EXCHANGE_CACHE_MS) return cached.data;

  exchangeRateCache.delete(key);
  return null;
}

function setCachedExchangeRate(from, to, data) {
  const key = getExchangeCacheKey(from, to);
  exchangeRateCache.set(key, {
    data,
    timestamp: Date.now()
  });
}

async function fetchExchangeRate(from, to) {
  const cached = getCachedExchangeRate(from, to);
  if (cached) return cached;

  const response = await axios.get("https://api.frankfurter.dev/v1/latest", {
    params: { from, to },
    timeout: 10000
  });

  const data = response.data || {};
  const rate = Number(data?.rates?.[to]);

  if (!Number.isFinite(rate) || rate <= 0) {
    throw new Error("Invalid exchange rate response");
  }

  const payload = {
    from,
    to,
    rate,
    date: String(data.date || "").trim()
  };

  setCachedExchangeRate(from, to, payload);
  return payload;
}

function buildExchangeTotalSpeech(selection, amount) {
  const amountNumber = Number(amount || 0);
  const total = amountNumber * Number(selection.rate || 0);
  return `${formatMoneyAmount(amountNumber)} dollars equals ${formatMoneyAmount(total)} ${currencySpeech(selection.to)}.`;
}

function normalizeBorderWaitText(value) {
  const text = String(value || "").trim();
  if (!text || text === "--") return "currently unavailable";

  const lower = text.toLowerCase();

  if (lower.includes("no delay")) return "no delay";
  if (lower.includes("update pending")) return "update pending";
  if (lower.includes("lanes closed")) return "lanes closed";
  if (lower.includes("closed")) return "closed";

  const minutesMatch = lower.match(/^(\d+)\s*(min|mins|minute|minutes)?$/);
  if (minutesMatch) return `${minutesMatch[1]} minutes`;

  return text;
}

function parseCbsaBorderCsv(text) {
  return String(text || "")
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => line.split(";;").map((cell) => cell.trim()));
}

function decodeXmlText(text) {
  return String(text || "")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/\s+/g, " ")
    .trim();
}

function firstXmlMatch(block, tagName) {
  const re = new RegExp(`<${tagName}[^>]*>([\\s\\S]*?)<\\/${tagName}>`, "i");
  const m = String(block || "").match(re);
  return m ? decodeXmlText(m[1]) : "";
}

function parseCbsaTimestamp(raw) {
  const text = String(raw || "").trim();
  if (!text) return null;

  const m = text.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})\s+([A-Z]{2,4})$/i);
  if (!m) {
    const parsed = new Date(text);
    return Number.isNaN(parsed.getTime()) ? null : parsed;
  }

  const [, y, mo, d, h, mi] = m;
  return new Date(Number(y), Number(mo) - 1, Number(d), Number(h), Number(mi), 0);
}

function getHoursOld(dateObj) {
  if (!dateObj || Number.isNaN(dateObj.getTime())) return null;
  return (Date.now() - dateObj.getTime()) / (1000 * 60 * 60);
}

function isStaleCbsaUpdate(rawTimestamp, staleHours = 24) {
  const parsed = parseCbsaTimestamp(rawTimestamp);
  const hoursOld = getHoursOld(parsed);
  if (hoursOld === null) return false;
  return hoursOld > staleHours;
}

function formatSpokenCbsaUpdate(rawTimestamp) {
  const parsed = parseCbsaTimestamp(rawTimestamp);
  if (!parsed || Number.isNaN(parsed.getTime())) return String(rawTimestamp || "").trim();

  return parsed.toLocaleString("en-US", {
    month: "long",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    hour12: true
  });
}

function trafficLevelFromWait(waitText) {
  const text = String(waitText || "").trim().toLowerCase();

  if (!text || text === "currently unavailable" || text === "update pending") return "";
  if (text.includes("no delay")) return "light";
  if (text.includes("closed") || text.includes("lanes closed")) return "";

  const m = text.match(/(\d+)/);
  if (!m) return "";

  const minutes = Number(m[1]);

  if (minutes <= 10) return "light";
  if (minutes <= 25) return "moderate";
  return "heavy";
}

function getTrafficLevel(extraMinutes) {
  if (!Number.isFinite(Number(extraMinutes)) || Number(extraMinutes) <= 5) return "light";
  if (Number(extraMinutes) <= 15) return "moderate";
  return "heavy";
}

function estimateCarsFromDelay(extraMinutes) {
  const minutes = Number(extraMinutes || 0);
  if (!Number.isFinite(minutes) || minutes <= 0) return 0;
  return Math.max(0, Math.round(minutes * 8));
}

async function fetchLiveTrafficRoute(origin, destination) {
  if (!process.env.GOOGLE_MAPS_API_KEY) {
    throw new Error("GOOGLE_MAPS_API_KEY is missing");
  }

  const response = await axios.post(
    "https://routes.googleapis.com/directions/v2:computeRoutes",
    {
      origin: {
        location: {
          latLng: origin
        }
      },
      destination: {
        location: {
          latLng: destination
        }
      },
      travelMode: "DRIVE",
      routingPreference: "TRAFFIC_AWARE_OPTIMAL",
      extraComputations: ["TRAFFIC_ON_POLYLINE"]
    },
    {
      timeout: BORDER_API_TIMEOUT_MS,
      headers: {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": process.env.GOOGLE_MAPS_API_KEY,
        "X-Goog-FieldMask":
          "routes.duration,routes.staticDuration,routes.polyline.encodedPolyline,routes.travelAdvisory.speedReadingIntervals"
      }
    }
  );

  const route = Array.isArray(response.data?.routes) ? response.data.routes[0] : null;
  if (!route) {
    throw new Error("No live traffic route returned from Google Routes API");
  }

  const durationText = String(route.duration || "0s");
  const staticDurationText = String(route.staticDuration || durationText);

  const durationSeconds = Number(durationText.replace("s", "")) || 0;
  const staticDurationSeconds = Number(staticDurationText.replace("s", "")) || durationSeconds;

  const extraMinutes = Math.max(0, Math.round((durationSeconds - staticDurationSeconds) / 60));

  return {
    extraMinutes,
    trafficLevel: getTrafficLevel(extraMinutes),
    estimatedCars: estimateCarsFromDelay(extraMinutes)
  };
}

async function fetchLiveTrafficIntoCanada() {
  const traffic = await fetchLiveTrafficRoute(
    BORDER_TRAFFIC_POINTS.intoCanada.origin,
    BORDER_TRAFFIC_POINTS.intoCanada.destination
  );

  return {
    direction: "live_into_canada",
    locationSpeech: CHAMPLAIN_LACOLLE.spokenNameCanada,
    source: "google-routes",
    ...traffic
  };
}

async function fetchLiveTrafficIntoUs() {
  const traffic = await fetchLiveTrafficRoute(
    BORDER_TRAFFIC_POINTS.intoUs.origin,
    BORDER_TRAFFIC_POINTS.intoUs.destination
  );

  return {
    direction: "live_into_us",
    locationSpeech: CHAMPLAIN_LACOLLE.spokenNameUs,
    source: "google-routes",
    ...traffic
  };
}

function pickPrimaryLane(lanes) {
  if (!lanes || typeof lanes !== "object") {
    return {
      wait: "currently unavailable",
      updatedAt: "",
      lanesOpen: ""
    };
  }

  const preferredOrder = [
    "standard_lanes",
    "NEXUS_SENTRI_lanes",
    "FAST_lanes",
    "ready_lanes"
  ];

  const candidates = [];

  for (const key of preferredOrder) {
    const lane = lanes[key];
    if (!lane || typeof lane !== "object") continue;

    const operational = String(lane.operational_status || "").trim();
    const delayMinutes = String(lane.delay_minutes || "").trim();
    const updateTime = String(lane.update_time || "").trim();
    const lanesOpen = String(lane.lanes_open || "").trim();

    let wait = "currently unavailable";

    if (delayMinutes !== "" && Number.isFinite(Number(delayMinutes))) {
      wait = normalizeBorderWaitText(`${delayMinutes} minutes`);
    } else if (operational) {
      wait = normalizeBorderWaitText(operational);
    }

    const parsedTime = Date.parse(updateTime);

    candidates.push({
      key,
      wait,
      updatedAt: updateTime,
      lanesOpen,
      parsedTime: Number.isNaN(parsedTime) ? 0 : parsedTime
    });
  }

  if (!candidates.length) {
    return {
      wait: "currently unavailable",
      updatedAt: "",
      lanesOpen: ""
    };
  }

  candidates.sort((a, b) => {
    if (b.parsedTime !== a.parsedTime) return b.parsedTime - a.parsedTime;
    return preferredOrder.indexOf(a.key) - preferredOrder.indexOf(b.key);
  });

  return {
    wait: candidates[0].wait,
    updatedAt: candidates[0].updatedAt,
    lanesOpen: candidates[0].lanesOpen
  };
}

async function fetchChamplainLacolleIntoCanada() {
  const cacheKey = "border:into_canada:champlain_lacolle";
  const cached = cacheGet(borderWaitCache, cacheKey, BORDER_CACHE_MS);
  if (cached) return cached;

  const response = await axios.get("https://www.cbsa-asfc.gc.ca/bwt-taf/bwt-eng.csv", {
    timeout: BORDER_API_TIMEOUT_MS,
    headers: {
      "User-Agent": "weather-and-info-line/1.0",
      Accept: "text/csv,text/plain,*/*"
    }
  });

  const rows = parseCbsaBorderCsv(response.data);
  const match = rows.find((row) => String(row[0] || "").trim() === CHAMPLAIN_LACOLLE.cbsaOfficeName);

  if (!match) {
    throw new Error("Champlain/Lacolle Canada-bound wait time not found in CBSA feed");
  }

  const updatedAt = String(match[2] || "").trim();
  const payload = {
    direction: "into_canada",
    locationSpeech: CHAMPLAIN_LACOLLE.spokenNameCanada,
    updatedAt,
    updatedAtSpoken: formatSpokenCbsaUpdate(updatedAt),
    isStale: isStaleCbsaUpdate(updatedAt, CHAMPLAIN_LACOLLE.staleHours),
    commercialWait: normalizeBorderWaitText(match[3]),
    passengerWait: normalizeBorderWaitText(match[5]),
    commercialLanesOpen: "",
    passengerLanesOpen: "",
    source: "cbsa"
  };

  return cacheSet(borderWaitCache, cacheKey, payload);
}

async function fetchChamplainLacolleIntoUs() {
  const cacheKey = "border:into_us:champlain_lacolle";
  const cached = cacheGet(borderWaitCache, cacheKey, BORDER_CACHE_MS);
  if (cached) return cached;

  try {
    const response = await axios.get("https://bwt.cbp.gov/xml/bwt.xml", {
      timeout: BORDER_API_TIMEOUT_MS,
      headers: {
        "User-Agent": "weather-and-info-line/1.0",
        Accept: "application/xml,text/xml;q=0.9,*/*;q=0.8"
      }
    });

    const xml = String(response.data || "");
    console.log("CBP XML first 1500 chars:", xml.slice(0, 1500));

    const items = xml.match(/<item\b[\s\S]*?<\/item>/gi) || [];
    console.log("CBP XML item count:", items.length);

    if (items.length) {
      const wantedPortNumber = String(CHAMPLAIN_LACOLLE.cbpPortNumber || "").replace(/^0+/, "");
      const wantedPortName = String(CHAMPLAIN_LACOLLE.cbpPortName || "").trim().toLowerCase();

      const candidates = items.map((item) => {
        const title = firstXmlMatch(item, "title");
        const description = firstXmlMatch(item, "description");
        const pubDate = firstXmlMatch(item, "pubDate");
        const guid = firstXmlMatch(item, "guid");
        const link = firstXmlMatch(item, "link");

        const text = [title, description, pubDate, guid, link].join(" ");

        const portNumberMatch = text.match(/\b0*([0-9]{4,8})\b/);
        const normalizedPortNumber = portNumberMatch ? portNumberMatch[1] : "";

        const currentWaitMatch = text.match(/Current Wait[: ]+(\d+)\s*min/i);
        const averageWaitMatch = text.match(/Average Wait[: ]+(\d+)\s*min/i);
        const lanesOpenMatch = text.match(/At\s*([\d:]+\s*[ap]m\s*[A-Z]{2,4})\s*,\s*(\d+)\s*lanes?\s*open/i);

        return {
          title,
          description,
          pubDate,
          guid,
          link,
          text,
          normalizedPortNumber,
          currentWait: currentWaitMatch ? `${currentWaitMatch[1]} minutes` : "",
          averageWait: averageWaitMatch ? `${averageWaitMatch[1]} minutes` : "",
          updatedAtSpoken: lanesOpenMatch ? `At ${lanesOpenMatch[1]}` : "",
          passengerLanesOpen: lanesOpenMatch ? lanesOpenMatch[2] : ""
        };
      });

      const match =
        candidates.find((x) => x.normalizedPortNumber === wantedPortNumber) ||
        candidates.find((x) => x.text.toLowerCase().includes(wantedPortName));

      if (match) {
        console.log("CBP XML match:", {
          title: match.title,
          pubDate: match.pubDate,
          updatedAtSpoken: match.updatedAtSpoken,
          passengerLanesOpen: match.passengerLanesOpen,
          currentWait: match.currentWait,
          averageWait: match.averageWait
        });

        const payload = {
          direction: "into_us",
          locationSpeech: CHAMPLAIN_LACOLLE.spokenNameUs,
          updatedAt: match.updatedAtSpoken || match.pubDate || "",
          updatedAtSpoken: match.updatedAtSpoken || match.pubDate || "",
          isStale: false,
          commercialWait: "currently unavailable",
          passengerWait: normalizeBorderWaitText(match.currentWait || "currently unavailable"),
          commercialLanesOpen: "",
          passengerLanesOpen: match.passengerLanesOpen || "",
          source: "cbp-xml"
        };

        return cacheSet(borderWaitCache, cacheKey, payload);
      }
    }

    console.log("CBP XML did not produce a usable Champlain match. Falling back to bulk API.");
  } catch (error) {
    console.error("CBP XML fetch failed:", error.message);
    console.log("Falling back to bulk API.");
  }

  const response = await axios.get("https://bwt.cbp.gov/api/waittimes", {
    timeout: BORDER_API_TIMEOUT_MS,
    headers: {
      "User-Agent": "weather-and-info-line/1.0",
      Accept: "application/json"
    }
  });

  const data = Array.isArray(response.data) ? response.data : [];

  const normalize = (v) =>
    String(v || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]/g, "");

  const targetPortNumber = normalize(CHAMPLAIN_LACOLLE.cbpPortNumber);
  const targetPortName = normalize(CHAMPLAIN_LACOLLE.cbpPortName);

  const port =
    data.find((item) => normalize(item?.port_number) === targetPortNumber) ||
    data.find((item) => normalize(item?.port_name) === targetPortName);

  if (!port) {
    throw new Error("Champlain/Lacolle U.S.-bound wait time not found in CBP bulk API");
  }

  const passenger = pickPrimaryLane(port.passenger_vehicle_lanes);

  console.log("CBP bulk fallback used:", {
    port_number: port?.port_number,
    port_name: port?.port_name,
    passengerWait: passenger.wait,
    updatedAt: passenger.updatedAt,
    passengerLanesOpen: passenger.lanesOpen
  });

  const payload = {
    direction: "into_us",
    locationSpeech: CHAMPLAIN_LACOLLE.spokenNameUs,
    updatedAt: passenger.updatedAt || "",
    updatedAtSpoken: passenger.updatedAt || "",
    isStale: false,
    commercialWait: "currently unavailable",
    passengerWait: passenger.wait || "currently unavailable",
    commercialLanesOpen: "",
    passengerLanesOpen: passenger.lanesOpen || "",
    source: "cbp-bulk-fallback"
  };

  return cacheSet(borderWaitCache, cacheKey, payload);
}

async function fetchBorderWait(direction) {
  if (direction === "official_into_canada") return fetchChamplainLacolleIntoCanada();
  if (direction === "official_into_us") return fetchChamplainLacolleIntoUs();
  if (direction === "live_into_canada") return fetchLiveTrafficIntoCanada();
  if (direction === "live_into_us") return fetchLiveTrafficIntoUs();
  throw new Error("Invalid border direction");
}

const BORDER_TRAFFIC_POINTS = {
  intoCanada: {
    origin: { latitude: 44.9625, longitude: -73.4466 },
    destination: { latitude: 45.0060, longitude: -73.4544 }
  },
  intoUs: {
    origin: { latitude: 45.0475, longitude: -73.4635 },
    destination: { latitude: 44.9957, longitude: -73.4552 }
  }
};

function buildBorderSpeech(result) {
  if (result.direction === "live_into_canada" || result.direction === "live_into_us") {
    const parts = [
      `Current live traffic conditions for ${result.locationSpeech}.`,
      `Traffic approaching the crossing is ${result.trafficLevel}.`
    ];

    if (Number.isFinite(Number(result.estimatedCars)) && Number(result.estimatedCars) > 0) {
      parts.push(`Estimated traffic approaching the crossing is about ${result.estimatedCars} vehicles based on current route delay.`);
    } else {
      parts.push("There does not appear to be a significant queue approaching the crossing.");
    }

    parts.push("Information is based on live traffic conditions near the crossing, updated in near real time.");
    return parts.join(" ");
  }

  const parts = [
    `Border wait time for ${result.locationSpeech}.`,
    `Passenger wait time is ${result.passengerWait}.`
  ];

  const passengerTraffic = trafficLevelFromWait(result.passengerWait);
  if (passengerTraffic) {
    parts.push(`Traffic is ${passengerTraffic}.`);
  }

  if (result.passengerLanesOpen && Number.isFinite(Number(result.passengerLanesOpen))) {
    const n = Number(result.passengerLanesOpen);
    parts.push(`${result.passengerLanesOpen} passenger ${n === 1 ? "lane is" : "lanes are"} open.`);
  }

  if (result.commercialWait && result.commercialWait !== "currently unavailable") {
    parts.push(`Commercial wait time is ${result.commercialWait}.`);

    const commercialTraffic = trafficLevelFromWait(result.commercialWait);
    if (commercialTraffic) {
      parts.push(`Commercial traffic is ${commercialTraffic}.`);
    }
  }

  if (result.commercialLanesOpen && Number.isFinite(Number(result.commercialLanesOpen))) {
    const n = Number(result.commercialLanesOpen);
    parts.push(`${result.commercialLanesOpen} commercial ${n === 1 ? "lane is" : "lanes are"} open.`);
  }

  if (result.direction === "into_canada" && result.isStale) {
    parts.push("However, the official update has not been refreshed within the last few hours.");
    parts.push(CHAMPLAIN_LACOLLE.constructionWarning);
    return parts.join(" ");
  }

  if (result.updatedAtSpoken) {
    parts.push(`Last updated ${result.updatedAtSpoken}.`);
  }

  return parts.join(" ");
}

async function fetchForecast(location) {
  pruneForecastCache();
  const cached = getCachedForecast(location);
  if (cached) {
    console.log(`Using fresh cache for ${cached.key}`);
    return cached.data;
  }

  const key = cacheKeyForLocation(location);
  if (inFlightForecasts.has(key)) {
    console.log(`Joining in-flight request for ${key}`);
    return inFlightForecasts.get(key);
  }

  const requestPromise = (async () => {
    try {
      let data;
      if (location?.country === "CA") {
        data = await fetchEnvironmentCanadaData(location);
      } else {
        data = await fetchOpenMeteoForecast(location);
      }

      const savedKey = setCachedForecast(location, data);
      console.log(`Fetched fresh forecast for ${savedKey}`);
      return data;
    } catch (error) {
      const stale = getCachedForecast(location, { allowStale: true });
      if (stale) {
        console.log(`Using stale cache for ${stale.key} after API failure`);
        return stale.data;
      }

      console.error("FETCH FORECAST FAILED");
      console.error("Location:", location);
      console.error("Message:", error.message);
      if (error.response) {
        console.error("Status:", error.response.status);
        console.error("Data:", JSON.stringify(error.response.data));
      }
      throw error;
    } finally {
      inFlightForecasts.delete(key);
    }
  })();

  inFlightForecasts.set(key, requestPromise);
  return requestPromise;
}

function speakWeatherError(twiml, error, fallbackText) {
  if (error.response?.status === 429) say(twiml, "Weather service limit reached for today. Please try again later.");
  else say(twiml, fallbackText);
}

async function buildMainMenuResponse(req, twiml) {
  const activeLocation = getActiveLocation(req);
  if (activeLocation) {
    const canadaAlert = await fetchCanadianAlert(activeLocation);
    if (canadaAlert) say(twiml, buildCanadianAlertSpeech(activeLocation, canadaAlert));
    setMenuState(req, "main-menu");
    buildMainMenuInto(twiml, placeLabel(activeLocation));
  } else {
    twiml.redirect({ method: "POST" }, "/location-menu-prompt");
  }
}

async function forecastDayPromptTwiml(location, forecast) {
  const twiml = new VoiceResponse();
  const gather = twiml.gather(gatherOptions("/forecast-day", 8, 1));

  if (location?.country === "CA") {
    const groups = buildEcDailyGroups(forecast, location);
    const parts = ["For the full Environment Canada forecast, press 0."];
    for (let i = 0; i < Math.min(9, groups.length); i++) {
      parts.push(`Press ${i + 1} for ${groups[i].label}.`);
    }
    parts.push("Press star to go back to the previous menu.");
    say(gather, parts.join(" "));
    twiml.redirect({ method: "POST" }, "/forecast-menu");
    return twiml;
  }

  const dailyTimes = forecast.daily?.time || [];
  const parts = ["For the seven day forecast, press 0."];
  if (dailyTimes[0]) parts.push("Press 1 for today.");
  if (dailyTimes[1]) parts.push("Press 2 for tomorrow.");
  for (let i = 2; i < Math.min(7, dailyTimes.length); i++) {
    parts.push(`Press ${i + 1} for ${fullDateLabel(dailyTimes[i], location.timezone)}.`);
  }
  parts.push("Press star to go back to the previous menu.");
  say(gather, parts.join(" "));
  twiml.redirect({ method: "POST" }, "/forecast-menu");
  return twiml;
}

async function buildPlaybackSpeech(location, forecast, playback, unit = "C") {
  if (!playback || !playback.type) return "";

  if (playback.type === "exchange" || playback.type === "border") {
    return playback.speech || "";
  }

  if (location?.country === "CA") {
    if (playback.type === "current") return ecCurrentWeatherSpeech(location, forecast, unit);
    if (playback.type === "hourly") return ecHourlySpeech(location, forecast, playback.hours || 12, unit);
    if (playback.type === "daily") return ecSingleForecastSpeech(location, forecast, playback.index, unit);
    if (playback.type === "all7") return ecAllForecastSpeech(location, forecast, unit);
    return "";
  }

  if (playback.type === "current") return currentWeatherSpeech(location, forecast, unit);
  if (playback.type === "hourly") return nextHoursSpeech(location, forecast, playback.hours || 6, unit);
  if (playback.type === "daily") return dailyForecastSpeech(location, forecast, playback.index, unit);
  if (playback.type === "all7") return sevenDayForecastSpeech(location, forecast, unit);

  return "";
}

async function buildStateTwiml(req, state, { push = true } = {}) {
  if (push && shouldTrackHistoryState(state)) pushMenuHistory(req, state);
  setMenuState(req, state);

  if (state === "root-menu") return rootMenuTwiml();

  if (state === "location-menu") {
    return locationMenuTwiml({ allowBack: true, allowVoicemail: false });
  }

  if (state === "us-location-menu") return usLocationMenuTwiml();

  if (state === "exchange-menu") return exchangeMenuTwiml();

  if (state === "exchange-amount") {
    const selection = getExchangeSelection(req);
    if (!selection) return exchangeMenuTwiml();
    return exchangeAmountPromptTwiml(req, selection);
  }

  if (state === "main-menu") {
    const twiml = new VoiceResponse();
    await buildMainMenuResponse(req, twiml);
    return twiml;
  }

  if (state === "forecast-menu") {
    const location = getActiveLocation(req);
    if (!location) return locationMenuTwiml({ allowBack: true, allowVoicemail: false });
    const forecast = await fetchForecast(location);
    return forecastDayPromptTwiml(location, forecast);
  }

  if (state === "playback") {
    const playback = getLastPlayback(req);
    const twiml = new VoiceResponse();

    if (!playback) {
      say(twiml, "There is nothing to repeat yet.");
      twiml.redirect({ method: "POST" }, "/root-menu-prompt");
      return twiml;
    }

    if (playback.type === "exchange" || playback.type === "border") {
      if (!playback.speech) {
        say(twiml, "There is nothing to repeat yet.");
        twiml.redirect({ method: "POST" }, "/root-menu-prompt");
        return twiml;
      }
      return playbackWithStarTwiml(playback.speech, {
        rate: playback.speechRate || "100%"
      });
    }

    const location = getActiveLocation(req);
    if (!location) {
      say(twiml, "Please choose a location first.");
      twiml.redirect({ method: "POST" }, "/location-menu-prompt");
      return twiml;
    }

    const forecast = await fetchForecast(location);
    const speech = await buildPlaybackSpeech(location, forecast, playback, getUnitPreference(req));

    if (!speech) {
      say(twiml, "There is nothing to repeat yet.");
      twiml.redirect({ method: "POST" }, "/root-menu-prompt");
      return twiml;
    }

    return playbackWithStarTwiml(speech, {
      rate: playback.speechRate || "100%"
    });
  }

  if (state === "after-prompt") return afterActionTwiml(req);
  if (state === "voicemail") return voicemailPromptTwiml();

  return rootMenuTwiml();
}

async function goBackOneMenu(req) {
  const currentState = getMenuState(req);
  const history = getMenuHistory(req);

  if (currentState === "playback" || currentState === "after-prompt") {
    return buildStateTwiml(req, history[history.length - 1] || "root-menu", { push: false });
  }

  if (history.length <= 1) return buildStateTwiml(req, "root-menu", { push: false });

  popMenuHistory(req);
  const updated = getMenuHistory(req);
  return buildStateTwiml(req, updated[updated.length - 1] || "root-menu", { push: false });
}

app.get("/", (req, res) => {
  res.send("Weather and exchange phone server is running.");
});

app.get("/voice", (req, res) => {
  const twiml = new VoiceResponse();
  say(twiml, "Weather and Info Line is running. Please call the phone number to use the interactive menu.");
  res.type("text/xml").send(twiml.toString());
});

app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();
  try {
    clearCallState(req);
    setUnitPreference(req, "C");
    say(twiml, `${getGreetingForTime("America/Toronto")}.`);
    buildRootMenuInto(twiml);
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("VOICE route error:", error.message);
    say(twiml, "Sorry, an application error occurred. Please try again.");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/root-menu-prompt", async (req, res) => {
  const twiml = await buildStateTwiml(req, "root-menu");
  res.type("text/xml").send(twiml.toString());
});

app.post("/root-menu", async (req, res) => {
  const choice = parseRootMenuChoice(req);
  const twiml = new VoiceResponse();

  try {
    if (choice === "1") {
      const locationTwiml = await buildStateTwiml(req, "location-menu");
      return res.type("text/xml").send(locationTwiml.toString());
    }

    if (choice === "2") {
      const exchangeTwiml = await buildStateTwiml(req, "exchange-menu");
      return res.type("text/xml").send(exchangeTwiml.toString());
    }

    if (choice === "3") {
      say(twiml, "That feature is not available yet.");
      twiml.redirect({ method: "POST" }, "/root-menu-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    if (choice === "4") {
      const borderTwiml = borderMenuTwiml();
      return res.type("text/xml").send(borderTwiml.toString());
    }

    say(twiml, "I did not understand that choice.");
    twiml.redirect({ method: "POST" }, "/root-menu-prompt");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("ROOT-MENU route error:", error.message);
    say(twiml, "Sorry, an application error occurred. Please try again.");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/border-menu-prompt", async (req, res) => {
  const twiml = borderMenuTwiml();
  res.type("text/xml").send(twiml.toString());
});

app.post("/border-menu", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    if (isBackKey(req)) {
      twiml.redirect({ method: "POST" }, "/root-menu-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    const direction = parseBorderDirectionChoice(req);

    if (!direction) {
      say(twiml, "I did not understand that choice.");
      twiml.redirect({ method: "POST" }, "/border-menu-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    setBorderDirection(req, direction);

    const submenuTwiml = new VoiceResponse();
    buildBorderSubmenuInto(submenuTwiml, direction);

    return res.type("text/xml").send(submenuTwiml.toString());
  } catch (error) {
    console.error(error);
    say(twiml, "Sorry, I could not retrieve border information.");
    twiml.redirect({ method: "POST" }, "/border-menu-prompt");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/border-submenu-prompt", async (req, res) => {
  const twiml = new VoiceResponse();
  const direction = getBorderDirection(req) || "canada";
  buildBorderSubmenuInto(twiml, direction);
  res.type("text/xml").send(twiml.toString());
});

app.post("/border-submenu", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    if (isBackKey(req)) {
      twiml.redirect({ method: "POST" }, "/border-menu-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    const direction = getBorderDirection(req);
    const type = parseBorderTypeChoice(req);

    if (!direction || !type) {
      say(twiml, "I did not understand that choice.");
      twiml.redirect({ method: "POST" }, "/border-submenu-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    let choice = "";

    if (direction === "canada" && type === "official") choice = "official_into_canada";
    if (direction === "canada" && type === "live") choice = "live_into_canada";
    if (direction === "us" && type === "official") choice = "official_into_us";
    if (direction === "us" && type === "live") choice = "live_into_us";

    const result = await fetchBorderWait(choice);
    const speech = buildBorderSpeech(result);

    setLastPlayback(req, {
      type: "border",
      speech,
      borderDirection: direction
    });

    const playbackTwiml = playbackWithStarTwiml(speech);
    return res.type("text/xml").send(playbackTwiml.toString());
  } catch (error) {
    console.error(error);
    say(twiml, "Sorry, I could not retrieve border information.");
    twiml.redirect({ method: "POST" }, "/border-submenu-prompt");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/main-menu", async (req, res) => {
  const twiml = new VoiceResponse();
  try {
    await buildMainMenuResponse(req, twiml);
    const history = getMenuHistory(req);
    if (!history.length || history[history.length - 1] !== "main-menu") pushMenuHistory(req, "main-menu");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("MAIN-MENU route error:", error.message);
    say(twiml, "Sorry, an application error occurred. Please try again.");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/location-menu-prompt", async (req, res) => {
  const twiml = await buildStateTwiml(req, "location-menu");
  res.type("text/xml").send(twiml.toString());
});

app.post("/us-location-menu-prompt", async (req, res) => {
  const twiml = await buildStateTwiml(req, "us-location-menu");
  res.type("text/xml").send(twiml.toString());
});

app.post("/exchange-menu-prompt", async (req, res) => {
  const twiml = await buildStateTwiml(req, "exchange-menu");
  res.type("text/xml").send(twiml.toString());
});

app.post("/exchange-amount-prompt", async (req, res) => {
  const twiml = await buildStateTwiml(req, "exchange-amount", { push: false });
  res.type("text/xml").send(twiml.toString());
});

app.post("/set-location-choice", async (req, res) => {
  const twiml = new VoiceResponse();

  if (isBackKey(req)) {
    const backTwiml = await goBackOneMenu(req);
    return res.type("text/xml").send(backTwiml.toString());
  }

  const choice = parseLocationChoice(req);
  console.log("SET-LOCATION-CHOICE choice:", choice, "digits:", getDigits(req));

  if (choice === "9") {
    const voiceTwiml = await buildStateTwiml(req, "voicemail", { push: false });
    return res.type("text/xml").send(voiceTwiml.toString());
  }

  if (choice === "4") {
    const usTwiml = await buildStateTwiml(req, "us-location-menu");
    return res.type("text/xml").send(usTwiml.toString());
  }

  if (!choice || !PRESET_LOCATIONS[choice]) {
    say(twiml, "I did not understand that location choice.");
    const menuTwiml = await buildStateTwiml(req, "location-menu", { push: false });
    return res.type("text/xml").send(menuTwiml.toString());
  }

  saveTemporaryLocationForCall(req, PRESET_LOCATIONS[choice]);
  clearPendingLocationChoice(req);
  pushMenuHistory(req, "main-menu");
  setMenuState(req, "main-menu");
  buildMainMenuInto(twiml, PRESET_LOCATIONS[choice].label);
  return res.type("text/xml").send(twiml.toString());
});

app.post("/set-us-location-choice", async (req, res) => {
  const twiml = new VoiceResponse();

  if (isBackKey(req)) {
    const backTwiml = await goBackOneMenu(req);
    return res.type("text/xml").send(backTwiml.toString());
  }

  const choice = parseUSLocationChoice(req);
  console.log("SET-US-LOCATION-CHOICE choice:", choice, "digits:", getDigits(req));

  if (!choice || !US_LOCATIONS[choice]) {
    say(twiml, "I did not understand that location choice.");
    const menuTwiml = await buildStateTwiml(req, "us-location-menu", { push: false });
    return res.type("text/xml").send(menuTwiml.toString());
  }

  saveTemporaryLocationForCall(req, US_LOCATIONS[choice]);
  clearPendingLocationChoice(req);
  pushMenuHistory(req, "main-menu");
  setMenuState(req, "main-menu");
  buildMainMenuInto(twiml, US_LOCATIONS[choice].label);
  return res.type("text/xml").send(twiml.toString());
});

app.post("/set-exchange-choice", async (req, res) => {
  const twiml = new VoiceResponse();

  if (isBackKey(req)) {
    const backTwiml = await goBackOneMenu(req);
    return res.type("text/xml").send(backTwiml.toString());
  }

  const selection = parseExchangeChoice(req);

  if (!selection) {
    say(twiml, "I did not understand that exchange choice.");
    const menuTwiml = await buildStateTwiml(req, "exchange-menu", { push: false });
    return res.type("text/xml").send(menuTwiml.toString());
  }

  try {
    const ratePayload = await fetchExchangeRate(selection.from, selection.to);

    setExchangeSelection(req, {
      from: selection.from,
      to: selection.to,
      rate: ratePayload.rate,
      asOfTime: exchangeAsOfTime("America/Toronto")
    });

    const amountTwiml = await buildStateTwiml(req, "exchange-amount", { push: false });
    return res.type("text/xml").send(amountTwiml.toString());
  } catch (error) {
    console.error("SET-EXCHANGE-CHOICE error:", error.message);
    say(twiml, "Sorry, I could not retrieve the exchange rate right now.");
    twiml.redirect({ method: "POST" }, "/exchange-menu-prompt");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/forecast-menu", async (req, res) => {
  try {
    if (isBackKey(req)) {
      const backTwiml = await goBackOneMenu(req);
      return res.type("text/xml").send(backTwiml.toString());
    }
    const twiml = await buildStateTwiml(req, "forecast-menu");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    const twiml = new VoiceResponse();
    console.error("FORECAST-MENU error:", error.message);
    speakWeatherError(twiml, error, "Sorry, I could not retrieve that forecast.");
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/menu", async (req, res) => {
  const choice = parseMainMenuChoice(req);
  const location = getActiveLocation(req);
  const twiml = new VoiceResponse();

  console.log("MENU CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("MENU choice:", choice, "digits:", getDigits(req));
  console.log("MENU active location:", location);

  if (isBackKey(req)) {
    const backTwiml = await goBackOneMenu(req);
    return res.type("text/xml").send(backTwiml.toString());
  }

  if (!location) {
    const locationTwiml = await buildStateTwiml(req, "location-menu");
    return res.type("text/xml").send(locationTwiml.toString());
  }

  try {
    if (choice === "1") {
      const forecastTwiml = await buildStateTwiml(req, "forecast-menu");
      return res.type("text/xml").send(forecastTwiml.toString());
    }

    if (choice === "2") {
      setLastPlayback(req, { type: "hourly", hours: location.country === "CA" ? 12 : 6 });
      const playbackTwiml = await buildStateTwiml(req, "playback", { push: false });
      return res.type("text/xml").send(playbackTwiml.toString());
    }

    if (choice === "3") {
      setLastPlayback(req, { type: "current" });
      const playbackTwiml = await buildStateTwiml(req, "playback", { push: false });
      return res.type("text/xml").send(playbackTwiml.toString());
    }

    say(twiml, "I did not understand that choice.");
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("MENU weather error:", error.message);
    speakWeatherError(twiml, error, "Sorry, I could not retrieve the weather right now. Please try again later.");
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/forecast-day", async (req, res) => {
  const location = getActiveLocation(req);
  const twiml = new VoiceResponse();

  console.log("FORECAST-DAY CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("FORECAST-DAY active location:", location, "digits:", getDigits(req));

  if (isBackKey(req)) {
    const backTwiml = await goBackOneMenu(req);
    return res.type("text/xml").send(backTwiml.toString());
  }

  if (!location) {
    const locationTwiml = await buildStateTwiml(req, "location-menu");
    return res.type("text/xml").send(locationTwiml.toString());
  }

  try {
    const forecast = await fetchForecast(location);
    const selected = parseForecastDayChoice(req);

    if (selected === "all") {
      setLastPlayback(req, { type: "all7", speechRate: "88%" });
      const playbackTwiml = await buildStateTwiml(req, "playback", { push: false });
      return res.type("text/xml").send(playbackTwiml.toString());
    }

    if (selected < 0) {
      say(twiml, "I did not understand the forecast day.");
      const forecastTwiml = await buildStateTwiml(req, "forecast-menu", { push: false });
      return res.type("text/xml").send(forecastTwiml.toString());
    }

    if (location.country === "CA") {
      const groupCount = buildEcDailyGroups(forecast, location).length;
      if (selected >= groupCount) {
        say(twiml, "I did not understand the forecast day.");
        const forecastTwiml = await buildStateTwiml(req, "forecast-menu", { push: false });
        return res.type("text/xml").send(forecastTwiml.toString());
      }
    } else if (selected > 6) {
      say(twiml, "I did not understand the forecast day.");
      const forecastTwiml = await buildStateTwiml(req, "forecast-menu", { push: false });
      return res.type("text/xml").send(forecastTwiml.toString());
    }

    setLastPlayback(req, { type: "daily", index: selected });
    const playbackTwiml = await buildStateTwiml(req, "playback", { push: false });
    return res.type("text/xml").send(playbackTwiml.toString());
  } catch (error) {
    console.error("FORECAST-DAY error:", error.message);
    speakWeatherError(twiml, error, "Sorry, I could not retrieve that forecast.");
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/exchange-amount", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    if (isBackKey(req)) {
      const backTwiml = await goBackOneMenu(req);
      return res.type("text/xml").send(backTwiml.toString());
    }

    const selection = getExchangeSelection(req);

    if (!selection) {
      const menuTwiml = await buildStateTwiml(req, "exchange-menu");
      return res.type("text/xml").send(menuTwiml.toString());
    }

    const amountDigits = parseAmountDigits(req);
    const amount = Number(amountDigits);

    if (!amountDigits || !Number.isFinite(amount) || amount <= 0) {
      say(twiml, "I did not understand that amount.");
      const amountTwiml = await buildStateTwiml(req, "exchange-amount", { push: false });
      return res.type("text/xml").send(amountTwiml.toString());
    }

    const speech = buildExchangeTotalSpeech(selection, amount);

    setLastPlayback(req, {
      type: "exchange",
      speech
    });

    const playbackTwiml = playbackWithStarTwiml(speech);
    return res.type("text/xml").send(playbackTwiml.toString());
  } catch (error) {
    console.error("EXCHANGE-AMOUNT error:", error.message);
    say(twiml, "Sorry, I could not retrieve the exchange rate right now.");
    twiml.redirect({ method: "POST" }, "/exchange-menu-prompt");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/during-playback", async (req, res) => {
  const playback = getLastPlayback(req);

  if (isBackKey(req)) {
    if (playback?.type === "border") {
      const twiml = new VoiceResponse();

      if (playback?.borderDirection) {
        setBorderDirection(req, playback.borderDirection);
        twiml.redirect({ method: "POST" }, "/border-submenu-prompt");
      } else {
        twiml.redirect({ method: "POST" }, "/border-menu-prompt");
      }

      return res.type("text/xml").send(twiml.toString());
    }

    const backTwiml = await goBackOneMenu(req);
    return res.type("text/xml").send(backTwiml.toString());
  }

  const afterTwiml = await buildStateTwiml(req, "after-prompt", { push: false });
  res.type("text/xml").send(afterTwiml.toString());
});

app.post("/after-prompt", async (req, res) => {
  const twiml = await buildStateTwiml(req, "after-prompt", { push: false });
  res.type("text/xml").send(twiml.toString());
});

app.post("/after", async (req, res) => {
  const twiml = new VoiceResponse();
  const playback = getLastPlayback(req);

  console.log("AFTER Digits:", getDigits(req));

  if (isBackKey(req)) {
    if (playback?.type === "border") {
      if (playback?.borderDirection) {
        setBorderDirection(req, playback.borderDirection);
        twiml.redirect({ method: "POST" }, "/border-submenu-prompt");
      } else {
        twiml.redirect({ method: "POST" }, "/border-menu-prompt");
      }

      return res.type("text/xml").send(twiml.toString());
    }

    const backTwiml = await goBackOneMenu(req);
    return res.type("text/xml").send(backTwiml.toString());
  }

  const choice = parseAfterChoice(req);

  if (choice === "5") {
    if (playback?.type === "border") {
      const rootTwiml = await buildStateTwiml(req, "root-menu", { push: false });
      return res.type("text/xml").send(rootTwiml.toString());
    }

    const mainTwiml = await buildStateTwiml(req, "main-menu", { push: false });
    return res.type("text/xml").send(mainTwiml.toString());
  }

  if (choice === "#") {
    const playbackTwiml = await buildStateTwiml(req, "playback", { push: false });
    return res.type("text/xml").send(playbackTwiml.toString());
  }

  if (choice === "7" && playback && playback.type !== "exchange" && playback.type !== "border") {
    toggleUnitPreference(req);
    const playbackTwiml = await buildStateTwiml(req, "playback", { push: false });
    return res.type("text/xml").send(playbackTwiml.toString());
  }

  say(twiml, "I did not understand that choice.");
  const afterTwiml = await buildStateTwiml(req, "after-prompt", { push: false });
  return res.type("text/xml").send(afterTwiml.toString());
});

app.post("/handle-recording", (req, res) => {
  const twiml = new VoiceResponse();
  const callSid = String(req.body.CallSid || "");
  const from = String(req.body.From || "");
  const to = String(req.body.To || "");
  const recordingUrl = String(req.body.RecordingUrl || "");
  const recordingDuration = String(req.body.RecordingDuration || "");

  console.log("HANDLE-RECORDING", { callSid, from, to, recordingUrl, recordingDuration });

  if (recordingUrl) {
    appendVoicemailRecord({
      callSid,
      from,
      to,
      recordingUrl,
      recordingDuration,
      createdAt: new Date().toISOString(),
      source: "action"
    });
    say(twiml, "Thank you. Your message has been saved.");
  } else {
    say(twiml, "I did not receive a message.");
  }

  twiml.redirect({ method: "POST" }, "/after-prompt");
  res.type("text/xml").send(twiml.toString());
});

app.post("/recording-status", (req, res) => {
  const callSid = String(req.body.CallSid || "");
  updateVoicemailRecord(callSid, {
    callSid,
    recordingSid: String(req.body.RecordingSid || ""),
    recordingUrl: String(req.body.RecordingUrl || ""),
    recordingStatus: String(req.body.RecordingStatus || ""),
    recordingDuration: String(req.body.RecordingDuration || ""),
    from: String(req.body.From || ""),
    to: String(req.body.To || ""),
    updatedAt: new Date().toISOString(),
    source: "recordingStatusCallback"
  });
  if (callSid) pendingLocationChoiceByCall.delete(callSid);
  res.status(204).send();
});

app.post("/call-ended", (req, res) => {
  clearCallState(req);
  res.status(204).send();
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Weather and exchange phone server running on port ${port}`);
});