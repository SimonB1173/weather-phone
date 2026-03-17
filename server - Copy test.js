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

const INTRO_AUDIO_URL = process.env.INTRO_AUDIO_URL || "";

const forecastCache = new Map();
const inFlightForecasts = new Map();
const canadaAlertCache = new Map();

const temporaryLocationsByCall = new Map();
const pendingLocationChoiceByCall = new Map();
const lastPlaybackByCall = new Map();

const FORECAST_CACHE_MS = 10 * 60 * 1000;
const STALE_FORECAST_MS = 60 * 60 * 1000;
const ALERT_CACHE_MS = 10 * 60 * 1000;

const DATA_FILE = path.join(__dirname, "saved-locations.json");
const VOICEMAILS_FILE = path.join(__dirname, "voicemails.json");
const QA_LOG_FILE = path.join(__dirname, "weather-qa-log.jsonl");

const PRESET_LOCATIONS = {
  "1": {
    id: "montreal",
    name: "Montreal, Quebec, Canada",
    latitude: 45.5239,
    longitude: -73.5997,
    timezone: "America/Toronto",
    label: "Montreal",
    country: "CA",
    alertFeedUrl: "https://weather.gc.ca/rss/alerts/45.5017_-73.5673_e.xml"
  },
  "2": {
    id: "tosh",
    pending: true,
    label: "Tosh"
  },
  "3": {
    id: "brooklyn",
    name: "Brooklyn, New York, USA",
    latitude: 40.7143,
    longitude: -73.9533,
    timezone: "America/New_York",
    label: "Brooklyn",
    country: "US"
  },
  "4": {
    id: "monsey",
    name: "Spring Valley, New York, USA",
    latitude: 41.1134,
    longitude: -74.0435,
    timezone: "America/New_York",
    label: "Monsey",
    country: "US"
  },
  "5": {
    id: "monroe",
    name: "Monroe, New York, USA",
    latitude: 41.3326,
    longitude: -74.186,
    timezone: "America/New_York",
    label: "Monroe",
    country: "US"
  }
};

function say(twiml, text) {
  const parts = String(text || "")
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);

  if (!parts.length) return;

  for (let i = 0; i < parts.length; i++) {
    twiml.say(SAY_OPTIONS, parts[i]);
    if (i < parts.length - 1) {
      twiml.pause({ length: 1 });
    }
  }
}

function normalizeCaller(v) {
  return String(v || "").trim();
}

function getCallerKey(req) {
  return normalizeCaller(req.body.From || "unknown");
}

function getCallKey(req) {
  return String(req.body.CallSid || "unknown").trim();
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

function appendJsonLine(filePath, payload) {
  try {
    fs.appendFileSync(filePath, JSON.stringify(payload) + "\n", "utf8");
  } catch (error) {
    console.error(`Failed to append ${filePath}:`, error.message);
  }
}

const savedLocationsByCaller = new Map(
  Object.entries(loadJsonFile(DATA_FILE, {}))
);

function persistSavedLocations() {
  saveJsonFile(DATA_FILE, Object.fromEntries(savedLocationsByCaller.entries()));
}

function getSavedLocation(req) {
  const caller = getCallerKey(req);
  return savedLocationsByCaller.get(caller) || null;
}

function saveLocationForCaller(req, location) {
  const caller = getCallerKey(req);
  savedLocationsByCaller.set(caller, location);
  persistSavedLocations();
}

function getTemporaryLocation(req) {
  const callKey = getCallKey(req);
  return temporaryLocationsByCall.get(callKey) || null;
}

function saveTemporaryLocationForCall(req, location) {
  const callKey = getCallKey(req);
  temporaryLocationsByCall.set(callKey, location);
}

function clearTemporaryLocationForCall(req) {
  const callKey = getCallKey(req);
  temporaryLocationsByCall.delete(callKey);
}

function setPendingLocationChoice(req, location) {
  const callKey = getCallKey(req);
  pendingLocationChoiceByCall.set(callKey, location);
}

function getPendingLocationChoice(req) {
  const callKey = getCallKey(req);
  return pendingLocationChoiceByCall.get(callKey) || null;
}

function clearPendingLocationChoice(req) {
  const callKey = getCallKey(req);
  pendingLocationChoiceByCall.delete(callKey);
}

function getActiveLocation(req) {
  return getTemporaryLocation(req) || getSavedLocation(req);
}

function setLastPlayback(req, payload) {
  const callKey = getCallKey(req);
  lastPlaybackByCall.set(callKey, payload);
}

function getLastPlayback(req) {
  const callKey = getCallKey(req);
  return lastPlaybackByCall.get(callKey) || null;
}

function clearCallState(req) {
  const callKey = getCallKey(req);
  temporaryLocationsByCall.delete(callKey);
  pendingLocationChoiceByCall.delete(callKey);
  lastPlaybackByCall.delete(callKey);
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

  if (!found) {
    current.push({
      callSid,
      ...updates
    });
  }

  saveJsonFile(VOICEMAILS_FILE, current);
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
  if (place) return place;
  return `${Number(location.latitude).toFixed(2)},${Number(location.longitude).toFixed(2)}`;
}

function pruneForecastCache() {
  const now = Date.now();
  for (const [key, value] of forecastCache.entries()) {
    if (!value || now - value.timestamp > STALE_FORECAST_MS) {
      forecastCache.delete(key);
    }
  }
}

function getCachedForecast(location, { allowStale = false } = {}) {
  const key = cacheKeyForLocation(location);
  const cached = forecastCache.get(key);

  if (!cached) return null;

  const age = Date.now() - cached.timestamp;

  if (age <= FORECAST_CACHE_MS) {
    return { data: cached.data, isStale: false, key, age };
  }

  if (allowStale && age <= STALE_FORECAST_MS) {
    return { data: cached.data, isStale: true, key, age };
  }

  return null;
}

function setCachedForecast(location, data) {
  const key = cacheKeyForLocation(location);
  forecastCache.set(key, {
    data,
    timestamp: Date.now()
  });
  return key;
}

function parsePlainDate(isoDate) {
  const text = String(isoDate || "").slice(0, 10);
  const [year, month, day] = text.split("-").map(Number);

  if (!year || !month || !day) {
    return new Date(isoDate);
  }

  return new Date(year, month - 1, day, 12, 0, 0);
}

function dayName(iso, tz) {
  return parsePlainDate(iso).toLocaleDateString("en-US", {
    weekday: "long",
    timeZone: tz || "UTC"
  });
}

function monthDayLabel(iso, tz) {
  return parsePlainDate(iso).toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    timeZone: tz || "UTC"
  });
}

function longDayWithDateLabel(iso, tz) {
  return parsePlainDate(iso).toLocaleDateString("en-US", {
    weekday: "long",
    month: "long",
    day: "numeric",
    timeZone: tz || "UTC"
  });
}

function timeLabel(iso, tz) {
  return new Date(iso).toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone: tz || "UTC"
  });
}

function getHourInTz(iso, tz) {
  const parts = new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    hour12: false,
    timeZone: tz || "UTC"
  }).formatToParts(new Date(iso));

  const hourPart = parts.find((p) => p.type === "hour");
  return Number(hourPart?.value || 0);
}

function timeOfDayPhrase(hour) {
  if (hour < 6) return "overnight";
  if (hour < 12) return "this morning";
  if (hour < 18) return "this afternoon";
  if (hour < 24) return "this evening";
  return "later";
}

function nightTransitionPhrase(hour) {
  if (hour < 3) return "after midnight";
  if (hour < 6) return "overnight";
  if (hour < 12) return "toward morning";
  return "later";
}

function findTransitionPhrase(entries, tz, type = "day") {
  if (!entries || entries.length < 2) return "later";

  const splitIndex = Math.max(1, Math.floor(entries.length / 2));
  const pivot = entries[splitIndex]?.time || entries[entries.length - 1]?.time;

  if (!pivot) return "later";

  const hour = getHourInTz(pivot, tz);
  return type === "night" ? nightTransitionPhrase(hour) : timeOfDayPhrase(hour);
}

function getGreetingForTime(timezone) {
  const parts = new Intl.DateTimeFormat("en-US", {
    hour: "numeric",
    hour12: false,
    timeZone: timezone || "America/Toronto"
  }).formatToParts(new Date());

  const hourPart = parts.find((p) => p.type === "hour");
  const hour = Number(hourPart?.value || 12);

  if (hour < 12) return "Good morning";
  if (hour < 18) return "Good afternoon";
  return "Good evening";
}

function isBackKey(req) {
  return String(req.body.Digits || "").trim() === "*";
}

function parseMainMenuChoice(req) {
  const digit = String(req.body.Digits || "").trim();
  const speech = String(req.body.SpeechResult || "").toLowerCase();

  if (digit) return digit;

  if (
    speech.includes("seven") ||
    speech.includes("7 day") ||
    speech.includes("7-day") ||
    speech.includes("weekly") ||
    speech.includes("week") ||
    speech.includes("forecast") ||
    speech.includes("today") ||
    speech.includes("tomorrow")
  ) return "1";

  if (speech.includes("hour")) return "2";
  if (speech.includes("current") || speech.includes("now")) return "3";
  if (speech.includes("change") || speech.includes("location")) return "5";

  if (
    speech.includes("message") ||
    speech.includes("comment") ||
    speech.includes("advertise") ||
    speech.includes("advertisement") ||
    speech.includes("voicemail") ||
    speech.includes("voice mail")
  ) return "6";

  return "";
}

function parseAfterChoice(req) {
  const digit = String(req.body.Digits || "").trim();
  const speech = String(req.body.SpeechResult || "").toLowerCase();

  if (digit === "5" || digit === "6" || digit === "#") return digit;
  if (speech.includes("change") || speech.includes("location")) return "5";

  if (
    speech.includes("message") ||
    speech.includes("comment") ||
    speech.includes("advertise") ||
    speech.includes("voicemail") ||
    speech.includes("voice mail")
  ) return "6";

  if (speech.includes("repeat") || speech.includes("again")) return "#";
  return "";
}

function parseForecastDayChoice(req, forecast, timezone) {
  const digit = String(req.body.Digits || "").trim();
  const speech = String(req.body.SpeechResult || "").toLowerCase();

  if (digit === "0") return "all";
  if (/^[1-7]$/.test(digit)) return parseInt(digit, 10) - 1;

  if (speech.includes("all")) return "all";
  if (speech.includes("today")) return 0;
  if (speech.includes("tomorrow")) return 1;

  for (let i = 2; i < Math.min(7, forecast.daily.time.length); i++) {
    const weekday = dayName(forecast.daily.time[i], timezone).toLowerCase();
    const fullDate = longDayWithDateLabel(forecast.daily.time[i], timezone).toLowerCase();
    const monthDay = monthDayLabel(forecast.daily.time[i], timezone).toLowerCase();

    if (
      speech.includes(weekday) ||
      speech.includes(fullDate) ||
      speech.includes(monthDay)
    ) {
      return i;
    }
  }

  const match = speech.match(/\b([0-7])\b/);
  if (match) {
    if (match[1] === "0") return "all";
    return parseInt(match[1], 10) - 1;
  }

  return -1;
}

function parseLocationChoice(req) {
  const digit = String(req.body.Digits || "").trim();
  const speech = String(req.body.SpeechResult || "").toLowerCase();

  if (/^[1-5]$/.test(digit)) return digit;
  if (speech.includes("montreal")) return "1";
  if (speech.includes("tosh")) return "2";
  if (speech.includes("brooklyn")) return "3";
  if (speech.includes("monsey") || speech.includes("spring valley")) return "4";
  if (speech.includes("monroe")) return "5";

  return "";
}

function parseLocationModeChoice(req) {
  const digit = String(req.body.Digits || "").trim();
  const speech = String(req.body.SpeechResult || "").toLowerCase();

  if (digit === "1" || digit === "2") return digit;
  if (
    speech.includes("this call") ||
    speech.includes("temporary") ||
    speech.includes("temp") ||
    speech.includes("not save") ||
    speech.includes("without saving")
  ) return "1";
  if (
    speech.includes("save") ||
    speech.includes("future") ||
    speech.includes("default")
  ) return "2";

  return "";
}

function pickLocationByQuery(queryValue) {
  const raw = String(queryValue || "").trim().toLowerCase();

  if (!raw) return PRESET_LOCATIONS["1"];
  if (PRESET_LOCATIONS[raw]) return PRESET_LOCATIONS[raw];

  for (const location of Object.values(PRESET_LOCATIONS)) {
    if (!location || location.pending) continue;
    const label = String(location.label || "").toLowerCase();
    const name = String(location.name || "").toLowerCase();
    if (raw === label || raw === name || name.includes(raw) || label.includes(raw)) {
      return location;
    }
  }

  return PRESET_LOCATIONS["1"];
}

function getServerTimeDebug() {
  return {
    now_iso: new Date().toISOString(),
    server_timezone_offset_minutes: new Date().getTimezoneOffset(),
    locale_string: new Date().toString()
  };
}

function buildMainMenuInto(twiml, activeLocationName) {
  const gather = twiml.gather({
    input: "speech dtmf",
    action: "/menu",
    method: "POST",
    timeout: 8,
    speechTimeout: "auto",
    numDigits: 1
  });

  say(
    gather,
    `Your current location is ${activeLocationName}. ` +
      `Press or say 1 for the 7 day forecast, ` +
      `2 for hourly forecast, ` +
      `3 for current weather, ` +
      `5 to change location, ` +
      `or 6 to leave a comment or suggestion.`
  );

  twiml.redirect({ method: "POST" }, "/main-menu");
}

function locationMenuTwiml() {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    input: "speech dtmf",
    action: "/set-location-choice",
    method: "POST",
    timeout: 7,
    speechTimeout: "auto",
    numDigits: 1
  });

  say(
    gather,
    `Press or say 1 for Montreal, ` +
      `2 for Tosh, ` +
      `3 for Brooklyn, ` +
      `4 for Monsey, ` +
      `5 for Monroe. ` +
      `After you choose a location, you can use it for this call only or save it for future calls. ` +
      `Press star for main menu.`
  );

  twiml.redirect({ method: "POST" }, "/location-menu-prompt");
  return twiml;
}

function locationModeTwiml(location) {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    input: "speech dtmf",
    action: "/set-location-mode",
    method: "POST",
    timeout: 7,
    speechTimeout: "auto",
    numDigits: 1
  });

  say(
    gather,
    `You chose ${placeLabel(location)}. ` +
      `Press or say 1 to use this location for this call only. ` +
      `Press or say 2 to save this location for future calls. ` +
      `Press star for main menu.`
  );

  twiml.redirect({ method: "POST" }, "/location-menu-prompt");
  return twiml;
}

function afterActionTwiml() {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    input: "speech dtmf",
    action: "/after",
    method: "POST",
    timeout: 6,
    speechTimeout: "auto",
    numDigits: 1
  });

  say(
    gather,
    `Press star for main menu, ` +
      `5 for change location, ` +
      `6 for voicemail, ` +
      `or press pound to repeat.`
  );

  twiml.hangup();
  return twiml;
}

function forecastDayPromptTwiml(location, forecast) {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    input: "speech dtmf",
    action: "/forecast-day",
    method: "POST",
    timeout: 8,
    speechTimeout: "auto",
    numDigits: 1
  });

  const dailyTimes = forecast.daily?.time || [];
  const parts = [
    `For the 7 day forecast in ${placeLabel(location)}, press or say 0 to hear all 7 days.`
  ];

  if (dailyTimes[0]) {
    parts.push(`Press or say 1 for today.`);
  }

  if (dailyTimes[1]) {
    parts.push(`Press or say 2 for tomorrow.`);
  }

  for (let i = 2; i < Math.min(7, dailyTimes.length); i++) {
    parts.push(
      `Press or say ${i + 1} for ${longDayWithDateLabel(dailyTimes[i], location.timezone)}.`
    );
  }

  parts.push(`Press star for main menu.`);

  say(gather, parts.join(" "));
  twiml.redirect({ method: "POST" }, "/main-menu");
  return twiml;
}

function voicemailPromptTwiml() {
  const twiml = new VoiceResponse();

  say(
    twiml,
    "Please leave your message after the beep. " +
      "You can use this for comments or suggestions. " +
      "Press the star key when you are finished."
  );

  twiml.record({
    action: "/handle-recording",
    method: "POST",
    maxLength: 180,
    finishOnKey: "*",
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

function weatherCodeToText(code) {
  const map = {
    0: "clear",
    1: "mostly clear",
    2: "partly cloudy",
    3: "cloudy",
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
    77: "snow grains",
    80: "light showers",
    81: "showers",
    82: "heavy showers",
    85: "light snow showers",
    86: "heavy snow showers",
    95: "thunderstorms",
    96: "thunderstorms with hail",
    99: "severe thunderstorms with hail"
  };
  return map[code] || "mixed weather";
}

function cloudCoverToPhrase(percent) {
  const p = Number(percent || 0);
  if (p < 10) return "sunny";
  if (p < 30) return "mostly sunny";
  if (p < 55) return "a mix of sun and cloud";
  if (p < 75) return "mainly cloudy";
  return "cloudy";
}

function weatherCodeToOfficialPhrase(code, cloudCoverLike = 60) {
  const n = Number(code);

  if ([0, 1, 2, 3].includes(n)) {
    return cloudCoverToPhrase(cloudCoverLike);
  }

  const map = {
    45: "foggy",
    48: "freezing fog",
    51: "drizzle",
    53: "drizzle",
    55: "heavy drizzle",
    56: "freezing drizzle",
    57: "heavy freezing drizzle",
    61: "rain",
    63: "rain",
    65: "heavy rain",
    66: "freezing rain",
    67: "heavy freezing rain",
    71: "flurries",
    73: "snow",
    75: "heavy snow",
    77: "snow grains",
    80: "showers",
    81: "showers",
    82: "heavy showers",
    85: "snow showers",
    86: "heavy snow showers",
    95: "thunderstorms",
    96: "thunderstorms with hail",
    99: "severe thunderstorms with hail"
  };

  return map[n] || "mixed weather";
}

function describeCurrentCondition(current) {
  const codeText = weatherCodeToText(current.weather_code);
  const cloudText = cloudCoverToPhrase(current.cloud_cover);

  if ([0, 1, 2, 3].includes(Number(current.weather_code))) {
    return cloudText;
  }

  return codeText;
}

function describeCurrentWeatherSentence(current) {
  if ((current.snowfall || 0) > 0) return "Current conditions show snow";
  if ((current.rain || 0) > 0 || (current.showers || 0) > 0) return "Current conditions show rain";

  const condition = describeCurrentCondition(current);

  if (condition === "sunny") return "It is sunny";
  if (condition === "mostly sunny") return "It is mostly sunny";
  if (condition === "a mix of sun and cloud") return "There is a mix of sun and cloud";
  if (condition === "mainly cloudy") return "It is mainly cloudy";
  if (condition === "cloudy") return "It is cloudy";
  if (condition === "foggy") return "It is foggy";
  if (condition === "freezing fog") return "There is freezing fog";
  if (condition === "thunderstorms") return "There are thunderstorms";
  if (condition === "thunderstorms with hail") return "There are thunderstorms with hail";
  if (condition === "severe thunderstorms with hail") return "There are severe thunderstorms with hail";

  return `It is ${condition}`;
}

function formatTemp(value) {
  return `${Math.round(value)} degrees`;
}

function formatSignedTemp(value) {
  const n = Math.round(Number(value || 0));
  if (n < 0) return `minus ${Math.abs(n)}`;
  if (n > 0) return `${n}`;
  return "zero";
}

function uvIndexPhrase(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return "";
  const rounded = Math.round(n);

  if (rounded <= 2) return `UV index ${rounded}, low`;
  if (rounded <= 5) return `UV index ${rounded}, moderate`;
  if (rounded <= 7) return `UV index ${rounded}, high`;
  if (rounded <= 10) return `UV index ${rounded}, very high`;
  return `UV index ${rounded}, extreme`;
}

function degreesToCompass(deg) {
  const directions = [
    "north",
    "northeast",
    "east",
    "southeast",
    "south",
    "southwest",
    "west",
    "northwest"
  ];
  const normalized = ((Number(deg || 0) % 360) + 360) % 360;
  const index = Math.round(normalized / 45) % 8;
  return directions[index];
}

function average(values) {
  const nums = values.map(Number).filter((v) => Number.isFinite(v));
  if (!nums.length) return 0;
  return nums.reduce((sum, v) => sum + v, 0) / nums.length;
}

function maxValue(values) {
  const nums = values.map(Number).filter((v) => Number.isFinite(v));
  if (!nums.length) return 0;
  return Math.max(...nums);
}

function firstNonNull(values) {
  for (const value of values) {
    if (value !== undefined && value !== null && Number.isFinite(Number(value))) {
      return Number(value);
    }
  }
  return null;
}

function getHourlyEntriesForDay(forecast, dateStr) {
  const h = forecast.hourly || {};
  const entries = [];
  const times = h.time || [];

  for (let i = 0; i < times.length; i++) {
    if (String(times[i]).slice(0, 10) === dateStr) {
      entries.push({
        time: times[i],
        temperature: h.temperature_2m?.[i],
        apparentTemperature: h.apparent_temperature?.[i],
        precipitationProbability: h.precipitation_probability?.[i],
        rain: h.rain?.[i] || 0,
        showers: h.showers?.[i] || 0,
        snowfall: h.snowfall?.[i] || 0,
        cloudCover: h.cloud_cover?.[i] || 0,
        wind: h.wind_speed_10m?.[i] || 0,
        windGusts: h.wind_gusts_10m?.[i] || 0,
        windDirection: h.wind_direction_10m?.[i],
        uvIndex: h.uv_index?.[i],
        weatherCode: h.weather_code?.[i]
      });
    }
  }

  return entries;
}

function getNightEntriesForDayIndex(forecast, index, timezone) {
  const dailyTimes = forecast.daily?.time || [];
  const currentDate = dailyTimes[index];
  const nextDate = dailyTimes[index + 1];
  const h = forecast.hourly || {};
  const entries = [];
  const times = h.time || [];
  const tz = timezone || forecast.timezone || "UTC";

  for (let i = 0; i < times.length; i++) {
    const timeText = String(times[i]);
    const datePart = timeText.slice(0, 10);
    const hour = getHourInTz(times[i], tz);

    const isLateCurrent = datePart === currentDate && hour >= 18;
    const isEarlyNext = nextDate && datePart === nextDate && hour < 6;

    if (isLateCurrent || isEarlyNext) {
      entries.push({
        time: h.time?.[i],
        temperature: h.temperature_2m?.[i],
        apparentTemperature: h.apparent_temperature?.[i],
        precipitationProbability: h.precipitation_probability?.[i],
        rain: h.rain?.[i] || 0,
        showers: h.showers?.[i] || 0,
        snowfall: h.snowfall?.[i] || 0,
        cloudCover: h.cloud_cover?.[i] || 0,
        wind: h.wind_speed_10m?.[i] || 0,
        windGusts: h.wind_gusts_10m?.[i] || 0,
        windDirection: h.wind_direction_10m?.[i],
        uvIndex: h.uv_index?.[i],
        weatherCode: h.weather_code?.[i]
      });
    }
  }

  return entries;
}

function groupConsecutiveHours(entries, predicate) {
  const groups = [];
  let current = null;

  for (const entry of entries) {
    if (predicate(entry)) {
      if (!current) {
        current = {
          start: entry.time,
          end: entry.time,
          items: [entry]
        };
      } else {
        current.end = entry.time;
        current.items.push(entry);
      }
    } else if (current) {
      groups.push(current);
      current = null;
    }
  }

  if (current) groups.push(current);
  return groups;
}

function pickMainWindow(groups) {
  if (!groups.length) return null;

  return groups
    .map((g) => {
      const avgProbability =
        g.items.reduce((sum, item) => sum + Number(item.precipitationProbability || 0), 0) /
        g.items.length;

      const totalRain = g.items.reduce(
        (sum, item) => sum + Number(item.rain || 0) + Number(item.showers || 0),
        0
      );
      const totalSnow = g.items.reduce((sum, item) => sum + Number(item.snowfall || 0), 0);
      const score = avgProbability + totalRain * 20 + totalSnow * 20 + g.items.length * 5;

      return { ...g, score };
    })
    .sort((a, b) => b.score - a.score)[0];
}

function formatTimeRange(startIso, endIso, tz) {
  const start = timeLabel(startIso, tz);
  const endDate = new Date(endIso);
  endDate.setHours(endDate.getHours() + 1);
  const end = endDate.toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone: tz || "UTC"
  });

  return `${start} until ${end}`;
}

function getDailyTimingDetails(entries, tz) {
  const rainGroups = groupConsecutiveHours(
    entries,
    (e) => Number(e.rain || 0) + Number(e.showers || 0) >= 0.15 || Number(e.precipitationProbability || 0) >= 45
  );
  const snowGroups = groupConsecutiveHours(
    entries,
    (e) => Number(e.snowfall || 0) >= 0.1
  );
  const stormGroups = groupConsecutiveHours(
    entries,
    (e) => [95, 96, 99].includes(Number(e.weatherCode))
  );

  const rainWindow = pickMainWindow(rainGroups);
  const snowWindow = pickMainWindow(snowGroups);
  const stormWindow = pickMainWindow(stormGroups);

  return {
    rain: rainWindow ? formatTimeRange(rainWindow.start, rainWindow.end, tz) : "",
    snow: snowWindow ? formatTimeRange(snowWindow.start, snowWindow.end, tz) : "",
    storm: stormWindow ? formatTimeRange(stormWindow.start, stormWindow.end, tz) : ""
  };
}

function describeWindForCurrent(current) {
  const speed = Math.round(Number(current.wind_speed_10m || 0));
  const gusts = Math.round(Number(current.wind_gusts_10m || 0));
  const direction = degreesToCompass(current.wind_direction_10m);

  if (speed >= 40 || gusts >= 65) {
    if (gusts > speed + 10) {
      return `Extremely windy. Wind ${direction} ${speed} kilometres per hour, gusting to ${gusts}.`;
    }
    return `Extremely windy. Wind ${direction} ${speed} kilometres per hour.`;
  }

  if (speed >= 15) {
    if (gusts > speed + 10) {
      return `Wind ${direction} ${speed} kilometres per hour, gusting to ${gusts}.`;
    }
    return `Wind ${direction} ${speed} kilometres per hour.`;
  }

  return "";
}

function currentWeatherSpeech(location, forecast) {
  const c = forecast.current;
  const parts = [
    `Current weather for ${placeLabel(location)}.`,
    `${describeCurrentWeatherSentence(c)}.`,
    `Temperature ${formatTemp(c.temperature_2m)}.`
  ];

  const apparent = firstNonNull([c.apparent_temperature]);
  if (
    apparent !== null &&
    Math.round(apparent) !== Math.round(Number(c.temperature_2m || 0)) &&
    apparent <= c.temperature_2m - 3
  ) {
    parts.push(`Feels like ${formatSignedTemp(apparent)}.`);
  }

  const windLine = describeWindForCurrent(c);
  if (windLine) {
    parts.push(windLine);
  }

  if ((c.showers || 0) > 0 && (c.rain || 0) <= 0) {
    parts.push(`There are showers right now.`);
  }

  return parts.join(" ");
}

function classifyHourlyBucket(item) {
  const code = Number(item.code);
  const rainAmount = Number(item.rain || 0) + Number(item.showers || 0);
  const snowAmount = Number(item.snow || 0);
  const wind = Number(item.wind || 0);
  const precipChance = Number(item.rainChance || 0);
  const clouds = Number(item.clouds || 0);

  const condition = [0, 1, 2, 3].includes(code)
    ? cloudCoverToPhrase(clouds)
    : weatherCodeToText(code);

  let precipTag = "dry";
  if ([95, 96, 99].includes(code)) {
    precipTag = "storm";
  } else if (snowAmount > 0) {
    precipTag = snowAmount >= 0.8 ? "snow" : "light-snow";
  } else if (rainAmount > 0 || precipChance >= 35) {
    precipTag = rainAmount >= 1.5 ? "rain" : "light-rain";
  }

  let windTag = "calm";
  if (wind >= 28) windTag = "windy";
  else if (wind >= 18) windTag = "breezy";

  const tempBand = Math.round(Number(item.temp || 0) / 2);

  return `${condition}|${precipTag}|${windTag}|${tempBand}`;
}

function summarizeHourlyBlock(block, tz) {
  const first = block.items[0];
  const start = timeLabel(block.start, tz);
  const endDate = new Date(block.end);
  endDate.setHours(endDate.getHours() + 1);
  const end = endDate.toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone: tz || "UTC"
  });

  const avgTemp =
    block.items.reduce((sum, x) => sum + Number(x.temp || 0), 0) / block.items.length;
  const maxRainChance = Math.max(...block.items.map((x) => Number(x.rainChance || 0)));
  const maxWind = Math.max(...block.items.map((x) => Number(x.wind || 0)));
  const maxGust = Math.max(...block.items.map((x) => Number(x.gusts || 0)));
  const totalRain = block.items.reduce(
    (sum, x) => sum + Number(x.rain || 0) + Number(x.showers || 0),
    0
  );
  const totalSnow = block.items.reduce((sum, x) => sum + Number(x.snow || 0), 0);

  const condition = [0, 1, 2, 3].includes(Number(first.code))
    ? cloudCoverToPhrase(first.clouds)
    : weatherCodeToText(first.code);

  const parts = [
    `From ${start} until ${end}, ${condition}, around ${Math.round(avgTemp)} degrees.`
  ];

  if (maxRainChance >= 45) {
    parts.push(`Chance around ${Math.round(maxRainChance)} percent.`);
  }

  if (totalRain > 0) {
    if (totalRain >= 3) {
      parts.push(`Rain at times.`);
    } else if (totalRain >= 1) {
      parts.push(`A few showers.`);
    } else {
      parts.push(`A slight chance of a little rain.`);
    }
  }

  if (totalSnow > 0) {
    if (totalSnow >= 2) {
      parts.push(`Snow at times.`);
    } else if (totalSnow >= 0.8) {
      parts.push(`Some snow.`);
    } else {
      parts.push(`A light bit of snow.`);
    }
  }

  if (maxWind >= 35 || maxGust >= 55) {
    parts.push(`Windy.`);
  } else if (maxWind >= 18) {
    parts.push(`A light breeze.`);
  }

  return parts.join(" ");
}

function buildSmartHourlyBlocks(items) {
  if (!items.length) return [];

  const blocks = [];
  let current = {
    key: classifyHourlyBucket(items[0]),
    start: items[0].time,
    end: items[0].time,
    items: [items[0]]
  };

  for (let i = 1; i < items.length; i++) {
    const key = classifyHourlyBucket(items[i]);
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

function nextHoursSpeech(location, forecast, hours = 6) {
  const now = Date.now();
  const h = forecast.hourly || {};
  const tz = location.timezone || "UTC";
  const items = [];

  for (let i = 0; i < (h.time || []).length; i++) {
    const t = new Date(h.time[i]).getTime();
    if (t >= now) {
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

  if (!items.length) {
    return `I could not find future hourly forecast data for ${placeLabel(location)}.`;
  }

  const blocks = buildSmartHourlyBlocks(items);
  const topBlocks = blocks.slice(0, 4);

  const firstCondition = [0, 1, 2, 3].includes(Number(items[0].code))
    ? cloudCoverToPhrase(items[0].clouds)
    : weatherCodeToText(items[0].code);

  const maxWind = maxValue(items.map((x) => x.wind));
  const maxGust = maxValue(items.map((x) => x.gusts));
  const openingParts = [
    `Here is the next ${items.length} hours for ${placeLabel(location)}.`,
    `Conditions begin with ${firstCondition}.`
  ];

  if (maxWind >= 35 || maxGust >= 55) {
    openingParts.push(`Windy at times.`);
  }

  const body = topBlocks.map((block) => summarizeHourlyBlock(block, tz)).join(" ");
  return `${openingParts.join(" ")} ${body}`.trim();
}

function getDayLabel(index, dateStr, timezone) {
  if (index === 0) return `Today, ${monthDayLabel(dateStr, timezone)}`;
  if (index === 1) return `Tomorrow, ${monthDayLabel(dateStr, timezone)}`;
  return longDayWithDateLabel(dateStr, timezone);
}

function getNightLabel(index, dateStr, timezone) {
  if (index === 0) return "Tonight";
  if (index === 1) return "Tomorrow night";
  return `${dayName(dateStr, timezone)} night`;
}

function describeDayIntro(entries, dailyCode) {
  if (!entries.length) {
    return weatherCodeToOfficialPhrase(dailyCode, 60);
  }

  const midday = entries[Math.floor(entries.length / 2)];
  const avgCloud = average(entries.map((e) => e.cloudCover));
  const totalSnow = entries.reduce((sum, e) => sum + Number(e.snowfall || 0), 0);
  const totalRain = entries.reduce((sum, e) => sum + Number(e.rain || 0) + Number(e.showers || 0), 0);
  const maxChance = maxValue(entries.map((e) => e.precipitationProbability));
  const hasStorm = entries.some((e) => [95, 96, 99].includes(Number(e.weatherCode)));

  if (hasStorm) return "Thunderstorms expected";

  if (totalSnow > 0.2) {
    if (maxChance >= 60) return "Snow";
    return "A chance of flurries";
  }

  if (totalRain > 0.2) {
    if (maxChance >= 60) return "Rain";
    return "A chance of showers";
  }

  return weatherCodeToOfficialPhrase(midday?.weatherCode ?? dailyCode, avgCloud);
}

function describeNightIntro(entries, fallbackCode, timezone) {
  if (!entries.length) {
    return weatherCodeToOfficialPhrase(fallbackCode, 70);
  }

  const avgCloud = average(entries.map((e) => e.cloudCover));
  const totalSnow = entries.reduce((sum, e) => sum + Number(e.snowfall || 0), 0);
  const totalRain = entries.reduce((sum, e) => sum + Number(e.rain || 0) + Number(e.showers || 0), 0);
  const maxChance = maxValue(entries.map((e) => e.precipitationProbability));
  const firstCode = firstNonNull(entries.map((e) => e.weatherCode));
  const lastCode = firstNonNull([...entries].reverse().map((e) => e.weatherCode));

  if (totalSnow > 0.2) {
    if (maxChance >= 60) return "Snow";
    return "Cloudy with a chance of flurries";
  }

  if (totalRain > 0.2) {
    if (maxChance >= 60) return "Rain";
    return "Cloudy with a chance of showers";
  }

  const startPhrase = weatherCodeToOfficialPhrase(firstCode ?? fallbackCode, avgCloud);
  const endPhrase = weatherCodeToOfficialPhrase(lastCode ?? fallbackCode, avgCloud);

  if (startPhrase !== endPhrase) {
    const phrase = findTransitionPhrase(entries, timezone, "night");
    return `${startPhrase}, becoming ${endPhrase} ${phrase}`;
  }

  return startPhrase;
}

function describeWindForPeriod(entries) {
  if (!entries.length) return "";

  const avgDirection = degreesToCompass(average(entries.map((e) => e.windDirection)));
  const maxWind = Math.round(maxValue(entries.map((e) => e.wind)));
  const maxGust = Math.round(maxValue(entries.map((e) => e.windGusts)));

  if (maxWind < 15 && maxGust < 25) return "";

  if (maxGust >= maxWind + 10) {
    return `Wind ${avgDirection} ${maxWind} kilometres per hour, gusting to ${maxGust}.`;
  }

  return `Wind ${avgDirection} ${maxWind} kilometres per hour.`;
}

function describeWindTrend(entries, tz, type = "day") {
  if (!entries.length) return "";

  const firstHalf = entries.slice(0, Math.ceil(entries.length / 2));
  const secondHalf = entries.slice(Math.floor(entries.length / 2));

  if (!firstHalf.length || !secondHalf.length) return "";

  const firstWind = Math.round(maxValue(firstHalf.map((e) => e.wind)));
  const secondWind = Math.round(maxValue(secondHalf.map((e) => e.wind)));
  const firstGust = Math.round(maxValue(firstHalf.map((e) => e.windGusts)));
  const secondGust = Math.round(maxValue(secondHalf.map((e) => e.windGusts)));
  const firstDir = degreesToCompass(average(firstHalf.map((e) => e.windDirection)));
  const secondDir = degreesToCompass(average(secondHalf.map((e) => e.windDirection)));

  const directionChanged = firstDir !== secondDir;
  const speedDropped = firstWind - secondWind >= 12 || firstGust - secondGust >= 15;
  const speedIncreased = secondWind - firstWind >= 12 || secondGust - firstGust >= 15;

  if (!directionChanged && !speedDropped && !speedIncreased) {
    return "";
  }

  const phrase = findTransitionPhrase(entries, tz, type);

  let start = `Wind ${firstDir} ${firstWind}`;
  if (firstGust >= firstWind + 10) start += ` gusting to ${firstGust}`;

  let end = `${secondDir} ${secondWind}`;
  if (secondGust >= secondWind + 10) end += ` gusting to ${secondGust}`;

  if (directionChanged) {
    return `${start}, becoming ${end} ${phrase}.`;
  }

  if (speedDropped) {
    return `${start}, easing to ${end} ${phrase}.`;
  }

  return `${start}, increasing to ${end} ${phrase}.`;
}

function describeWindChill(entries, overnight = false) {
  if (!entries.length) return "";

  const minApparent = Math.round(Math.min(...entries.map((e) => Number(e.apparentTemperature ?? e.temperature ?? 0))));
  const minTemp = Math.round(Math.min(...entries.map((e) => Number(e.temperature ?? 0))));

  if (!Number.isFinite(minApparent)) return "";
  if (minApparent >= minTemp - 3) return "";

  if (overnight) {
    return `Wind chill ${formatSignedTemp(minApparent)} overnight.`;
  }

  return `Wind chill ${formatSignedTemp(minApparent)}.`;
}

function describeDayTemperatureTrend(forecast, index, dayEntries) {
  const maxTemp = forecast.daily?.temperature_2m_max?.[index];
  if (!Number.isFinite(Number(maxTemp))) return "";

  if (index === 0 && forecast.current && Number.isFinite(Number(forecast.current.temperature_2m))) {
    const currentTemp = Number(forecast.current.temperature_2m);
    const highTemp = Number(maxTemp);

    if (highTemp <= currentTemp - 2) {
      return `Temperature falling to ${formatSignedTemp(highTemp)} this afternoon.`;
    }
  }

  if (dayEntries.length >= 4) {
    const firstHalf = average(dayEntries.slice(0, Math.ceil(dayEntries.length / 2)).map((e) => e.temperature));
    const secondHalf = average(dayEntries.slice(Math.floor(dayEntries.length / 2)).map((e) => e.temperature));

    if (Number.isFinite(firstHalf) && Number.isFinite(secondHalf) && firstHalf - secondHalf >= 3) {
      return `Temperature falling through the afternoon.`;
    }
  }

  return `High ${formatSignedTemp(maxTemp)}.`;
}

function describePrecipTiming(entries, tz, mode = "day") {
  const timing = getDailyTimingDetails(entries, tz);
  const totalSnow = entries.reduce((sum, e) => sum + Number(e.snowfall || 0), 0);
  const totalRain = entries.reduce((sum, e) => sum + Number(e.rain || 0) + Number(e.showers || 0), 0);

  if (timing.storm) {
    return `Thunderstorms most likely from ${timing.storm}.`;
  }

  if (totalSnow > 0.2 && timing.snow) {
    return `Snow most likely from ${timing.snow}.`;
  }

  if (totalRain > 0.2 && timing.rain) {
    return `Rain most likely from ${timing.rain}.`;
  }

  if (totalRain <= 0.2 && timing.rain) {
    return mode === "night"
      ? `A chance of showers later tonight, most likely from ${timing.rain}.`
      : `A chance of showers, most likely from ${timing.rain}.`;
  }

  if (totalSnow <= 0.2 && timing.snow) {
    return mode === "night"
      ? `A chance of flurries later tonight, most likely from ${timing.snow}.`
      : `A chance of flurries, most likely from ${timing.snow}.`;
  }

  return "";
}

function buildDaySection(location, forecast, index) {
  const d = forecast.daily || {};
  const dateStr = d.time?.[index];
  const entries = getHourlyEntriesForDay(forecast, dateStr);
  const label = getDayLabel(index, dateStr, location.timezone);

  const intro = describeDayIntro(entries, d.weather_code?.[index]);
  const windTrend = describeWindTrend(entries, location.timezone, "day");
  const windLine = windTrend || describeWindForPeriod(entries);
  const tempLine = describeDayTemperatureTrend(forecast, index, entries);
  const windChillLine = describeWindChill(entries, false);
  const precipTiming = describePrecipTiming(entries, location.timezone, "day");
  const uvLine = uvIndexPhrase(d.uv_index_max?.[index]);

  const parts = [
    `${label}.`,
    `${intro}.`
  ];

  if (windLine) parts.push(windLine);
  if (tempLine) parts.push(tempLine);
  if (windChillLine) parts.push(windChillLine);
  if (precipTiming) parts.push(precipTiming);
  if (uvLine) parts.push(`${uvLine}.`);

  return parts.join(" ");
}

function buildNightSection(location, forecast, index) {
  const d = forecast.daily || {};
  const dateStr = d.time?.[index];
  const entries = getNightEntriesForDayIndex(forecast, index, location.timezone);
  const label = getNightLabel(index, dateStr, location.timezone);
  const intro = describeNightIntro(entries, d.weather_code?.[index], location.timezone);
  const windTrend = describeWindTrend(entries, location.timezone, "night");
  const windLine = windTrend || describeWindForPeriod(entries);
  const windChillLine = describeWindChill(entries, true);
  const precipTiming = describePrecipTiming(entries, location.timezone, "night");

  const nextDayMin = d.temperature_2m_min?.[index + 1];
  const sameDayMin = d.temperature_2m_min?.[index];
  const lowValue = Number.isFinite(Number(nextDayMin)) ? nextDayMin : sameDayMin;

  const parts = [
    `${label}.`,
    `${intro}.`
  ];

  if (windLine) parts.push(windLine);
  if (Number.isFinite(Number(lowValue))) {
    parts.push(`Low ${formatSignedTemp(lowValue)}.`);
  }
  if (windChillLine) parts.push(windChillLine);
  if (precipTiming) parts.push(precipTiming);

  return parts.join(" ");
}

function dailyForecastSpeech(location, forecast, index) {
  const d = forecast.daily || {};
  if (index < 0 || index >= (d.time || []).length) {
    return `That forecast day is not available for ${placeLabel(location)}.`;
  }

  const daySection = buildDaySection(location, forecast, index);
  const nightSection = buildNightSection(location, forecast, index);

  return `${daySection} ${nightSection}`.trim();
}

function shortDaySection(location, forecast, index) {
  const d = forecast.daily || {};
  const dateStr = d.time?.[index];
  const entries = getHourlyEntriesForDay(forecast, dateStr);

  const label =
    index === 0 ? "Today" :
    index === 1 ? "Tomorrow" :
    dayName(dateStr, location.timezone);

  const intro = describeDayIntro(entries, d.weather_code?.[index]);
  return `${label}. ${intro}. High ${formatSignedTemp(d.temperature_2m_max?.[index])}.`;
}

function shortNightSection(location, forecast, index) {
  const d = forecast.daily || {};
  const dateStr = d.time?.[index];
  const entries = getNightEntriesForDayIndex(forecast, index, location.timezone);

  const label =
    index === 0 ? "Tonight" :
    index === 1 ? "Tomorrow night" :
    `${dayName(dateStr, location.timezone)} night`;

  const intro = describeNightIntro(entries, d.weather_code?.[index], location.timezone);

  const nextDayMin = d.temperature_2m_min?.[index + 1];
  const sameDayMin = d.temperature_2m_min?.[index];
  const lowValue = Number.isFinite(Number(nextDayMin)) ? nextDayMin : sameDayMin;

  return `${label}. ${intro}. Low ${formatSignedTemp(lowValue)}.`;
}

function sevenDayForecastSpeech(location, forecast) {
  const d = forecast.daily || {};
  const count = Math.min(7, (d.time || []).length);
  const parts = [`Here is the 7 day forecast for ${placeLabel(location)}.`];

  for (let i = 0; i < count; i++) {
    parts.push(shortDaySection(location, forecast, i));
    parts.push(shortNightSection(location, forecast, i));
  }

  return parts.join(" ");
}

async function buildPlaybackSpeech(location, forecast, playback) {
  if (!playback || !playback.type) return "";

  if (playback.type === "current") {
    return currentWeatherSpeech(location, forecast);
  }

  if (playback.type === "hourly") {
    return nextHoursSpeech(location, forecast, playback.hours || 6);
  }

  if (playback.type === "daily") {
    return dailyForecastSpeech(location, forecast, playback.index);
  }

  if (playback.type === "all7") {
    return sevenDayForecastSpeech(location, forecast);
  }

  return "";
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
  if (cut > 80) {
    return shortened.slice(0, cut + 1).trim();
  }
  return `${shortened.trim()}...`;
}

async function fetchCanadianAlert(location) {
  if (!location || location.country !== "CA" || !location.alertFeedUrl) {
    return null;
  }

  const cacheKey = location.alertFeedUrl;
  const cached = canadaAlertCache.get(cacheKey);
  const now = Date.now();

  if (cached && now - cached.timestamp < ALERT_CACHE_MS) {
    return cached.value;
  }

  try {
    const response = await axios.get(location.alertFeedUrl, {
      timeout: 8000,
      headers: {
        "User-Agent": "weather-line-canada/1.0"
      }
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

    canadaAlertCache.set(cacheKey, {
      value: alertValue,
      timestamp: now
    });

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

function getHourlyAuditRows(forecast, count = 12) {
  const h = forecast.hourly || {};
  const rows = [];
  const limit = Math.min(count, (h.time || []).length);

  for (let i = 0; i < limit; i++) {
    rows.push({
      index: i,
      time: h.time?.[i],
      temperature_2m: h.temperature_2m?.[i],
      apparent_temperature: h.apparent_temperature?.[i],
      precipitation_probability: h.precipitation_probability?.[i],
      rain: h.rain?.[i],
      showers: h.showers?.[i],
      snowfall: h.snowfall?.[i],
      cloud_cover: h.cloud_cover?.[i],
      wind_speed_10m: h.wind_speed_10m?.[i],
      wind_gusts_10m: h.wind_gusts_10m?.[i],
      wind_direction_10m: h.wind_direction_10m?.[i],
      weather_code: h.weather_code?.[i],
      local_hour_from_helper: getHourInTz(h.time?.[i], forecast.timezone || "UTC"),
      local_clock_label: timeLabel(h.time?.[i], forecast.timezone || "UTC")
    });
  }

  return rows;
}

function getDailyAuditRows(forecast) {
  const d = forecast.daily || {};
  const rows = [];
  const count = Math.min(7, (d.time || []).length);

  for (let i = 0; i < count; i++) {
    rows.push({
      index: i,
      date: d.time?.[i],
      weekday: dayName(d.time?.[i], forecast.timezone || "UTC"),
      weather_code: d.weather_code?.[i],
      temperature_2m_max: d.temperature_2m_max?.[i],
      temperature_2m_min: d.temperature_2m_min?.[i],
      precipitation_probability_max: d.precipitation_probability_max?.[i],
      rain_sum: d.rain_sum?.[i],
      showers_sum: d.showers_sum?.[i],
      snowfall_sum: d.snowfall_sum?.[i],
      wind_speed_10m_max: d.wind_speed_10m_max?.[i],
      wind_gusts_10m_max: d.wind_gusts_10m_max?.[i],
      uv_index_max: d.uv_index_max?.[i]
    });
  }

  return rows;
}

function buildForecastAuditPayload(location, forecast) {
  return {
    generatedAt: new Date().toISOString(),
    location: {
      id: location.id,
      label: location.label,
      name: location.name,
      latitude: location.latitude,
      longitude: location.longitude,
      timezone: location.timezone,
      country: location.country
    },
    server_time: getServerTimeDebug(),
    request_debug: forecast._debug || null,
    forecast_meta: {
      timezone: forecast.timezone,
      timezone_abbreviation: forecast.timezone_abbreviation,
      utc_offset_seconds: forecast.utc_offset_seconds,
      latitude: forecast.latitude,
      longitude: forecast.longitude,
      elevation: forecast.elevation
    },
    current_raw: forecast.current || null,
    current_units: forecast.current_units || null,
    hourly_units: forecast.hourly_units || null,
    daily_units: forecast.daily_units || null,
    first_12_hourly_rows: getHourlyAuditRows(forecast, 12),
    daily_rows: getDailyAuditRows(forecast),
    spoken_output: {
      current: currentWeatherSpeech(location, forecast),
      next6: nextHoursSpeech(location, forecast, 6),
      day0: dailyForecastSpeech(location, forecast, 0),
      day1: dailyForecastSpeech(location, forecast, 1),
      all7: sevenDayForecastSpeech(location, forecast)
    }
  };
}

function writeForecastAudit(location, forecast, tag = "manual") {
  const payload = {
    tag,
    ...buildForecastAuditPayload(location, forecast)
  };

  appendJsonLine(QA_LOG_FILE, payload);
  return payload;
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

  const params = {
    latitude: Number(location.latitude),
    longitude: Number(location.longitude),
    apikey: process.env.OPEN_METEO_API_KEY,
    current: [
      "temperature_2m",
      "apparent_temperature",
      "rain",
      "showers",
      "snowfall",
      "cloud_cover",
      "wind_speed_10m",
      "wind_gusts_10m",
      "wind_direction_10m",
      "weather_code"
    ].join(","),
    hourly: [
      "temperature_2m",
      "apparent_temperature",
      "precipitation_probability",
      "rain",
      "showers",
      "snowfall",
      "cloud_cover",
      "wind_speed_10m",
      "wind_gusts_10m",
      "wind_direction_10m",
      "uv_index",
      "weather_code"
    ].join(","),
    daily: [
      "weather_code",
      "temperature_2m_max",
      "temperature_2m_min",
      "precipitation_probability_max",
      "rain_sum",
      "showers_sum",
      "snowfall_sum",
      "wind_speed_10m_max",
      "wind_gusts_10m_max",
      "uv_index_max"
    ].join(","),
    timezone: location.timezone || "America/Toronto",
    forecast_days: 7,
    temperature_unit: "celsius",
    wind_speed_unit: "kmh",
    precipitation_unit: "mm"
  };

  const requestPromise = (async () => {
    try {
      const response = await axios.get("https://customer-api.open-meteo.com/v1/forecast", {
        params,
        timeout: 15000
      });

      const data = response.data;

      if (!data || !data.current || !data.hourly || !data.daily) {
        throw new Error("Open-Meteo returned incomplete forecast data");
      }

      if (!data.hourly.weather_code && data.hourly.weathercode) {
        data.hourly.weather_code = data.hourly.weathercode;
      }

      if (!data.daily.weather_code && data.daily.weathercode) {
        data.daily.weather_code = data.daily.weathercode;
      }

      data._debug = {
        fetchedAt: new Date().toISOString(),
        requestParams: params,
        requestedLocation: {
          label: location.label,
          name: location.name,
          latitude: location.latitude,
          longitude: location.longitude,
          timezone: location.timezone,
          country: location.country
        },
        responseMeta: {
          latitude: data.latitude,
          longitude: data.longitude,
          timezone: data.timezone,
          timezone_abbreviation: data.timezone_abbreviation,
          utc_offset_seconds: data.utc_offset_seconds,
          elevation: data.elevation
        }
      };

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
      console.error("Request params:", params);
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
  if (error.response?.status === 429) {
    say(twiml, "Weather service limit reached for today. Please try again later.");
  } else {
    say(twiml, fallbackText);
  }
}

async function buildMainMenuResponse(req, twiml) {
  const activeLocation = getActiveLocation(req);

  if (activeLocation) {
    const canadaAlert = await fetchCanadianAlert(activeLocation);
    if (canadaAlert) {
      say(twiml, buildCanadianAlertSpeech(activeLocation, canadaAlert));
    }

    buildMainMenuInto(twiml, placeLabel(activeLocation));
  } else {
    say(twiml, "No saved location was found for this number.");
    twiml.redirect({ method: "POST" }, "/location-menu-prompt");
  }
}

app.get("/", (req, res) => {
  res.send("Weather phone server is running.");
});

app.get("/debug-weather", async (req, res) => {
  try {
    const location = pickLocationByQuery(req.query.location);
    const before = getCachedForecast(location, { allowStale: true });
    const forecast = await fetchForecast(location);
    const after = getCachedForecast(location, { allowStale: true });
    const canadaAlert = await fetchCanadianAlert(location);

    const audit = buildForecastAuditPayload(location, forecast);

    res.json({
      ok: true,
      cache_key: cacheKeyForLocation(location),
      cache_hit_before: !!before,
      cache_hit_after: !!after,
      cached_locations: forecastCache.size,
      in_flight_requests: inFlightForecasts.size,
      location,
      canada_alert: canadaAlert,
      audit
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message,
      status: error.response?.status || null,
      details: error.response?.data || null
    });
  }
});

app.get("/qa-speech", async (req, res) => {
  try {
    const location = pickLocationByQuery(req.query.location);
    const forecast = await fetchForecast(location);

    res.json({
      ok: true,
      location,
      spoken_output: {
        current: currentWeatherSpeech(location, forecast),
        next6: nextHoursSpeech(location, forecast, 6),
        today: dailyForecastSpeech(location, forecast, 0),
        tomorrow: dailyForecastSpeech(location, forecast, 1),
        all7: sevenDayForecastSpeech(location, forecast)
      }
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message,
      status: error.response?.status || null,
      details: error.response?.data || null
    });
  }
});

app.get("/qa-timezone", async (req, res) => {
  try {
    const location = pickLocationByQuery(req.query.location);
    const forecast = await fetchForecast(location);
    const rows = getHourlyAuditRows(forecast, 16);

    res.json({
      ok: true,
      location,
      server_time: getServerTimeDebug(),
      forecast_timezone: forecast.timezone,
      forecast_timezone_abbreviation: forecast.timezone_abbreviation,
      first_16_hour_rows: rows
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message,
      status: error.response?.status || null,
      details: error.response?.data || null
    });
  }
});

app.get("/qa-write", async (req, res) => {
  try {
    const location = pickLocationByQuery(req.query.location);
    const tag = String(req.query.tag || "manual");
    const forecast = await fetchForecast(location);
    const payload = writeForecastAudit(location, forecast, tag);

    res.json({
      ok: true,
      message: "QA audit written",
      file: QA_LOG_FILE,
      tag,
      location: location.label,
      spoken_output: payload.spoken_output
    });
  } catch (error) {
    res.status(500).json({
      ok: false,
      message: error.message,
      status: error.response?.status || null,
      details: error.response?.data || null
    });
  }
});

app.get("/voice", (req, res) => {
  const twiml = new VoiceResponse();
  say(twiml, "Weather Line is running. Please call the phone number to use the interactive weather menu.");
  res.type("text/xml").send(twiml.toString());
});

app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    const activeLocation = getActiveLocation(req);
    const timezone = activeLocation?.timezone || "America/Toronto";
    const greeting = getGreetingForTime(timezone);

    console.log("VOICE CallSid:", req.body.CallSid, "From:", req.body.From);
    console.log("VOICE active location:", activeLocation);

    if (INTRO_AUDIO_URL) {
      twiml.play(INTRO_AUDIO_URL);
    }

    twiml.say(
      SAY_OPTIONS,
      `${greeting}, welcome to Weather Line. ` +
        `Please note this line is still in progress and should be fully running on Thursday, March 19. ` +
        `You are welcome to leave comments or ideas for improvement by pressing 6 from the main menu.`
    );

    await buildMainMenuResponse(req, twiml);
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("VOICE route error:", error.message);
    console.error("VOICE route details:", error.response?.data || null);
    say(twiml, "Sorry, an application error occurred. Please try again.");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/main-menu", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    await buildMainMenuResponse(req, twiml);
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("MAIN-MENU route error:", error.message);
    console.error("MAIN-MENU route details:", error.response?.data || null);
    say(twiml, "Sorry, an application error occurred. Please try again.");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/location-menu-prompt", (req, res) => {
  res.type("text/xml").send(locationMenuTwiml().toString());
});

app.post("/set-location-choice", (req, res) => {
  const twiml = new VoiceResponse();

  if (isBackKey(req)) {
    clearPendingLocationChoice(req);
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }

  const choice = parseLocationChoice(req);

  console.log("SET-LOCATION-CHOICE From:", req.body.From, "choice:", choice);

  if (!choice || !PRESET_LOCATIONS[choice]) {
    say(twiml, "I did not understand that location choice.");
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  const preset = PRESET_LOCATIONS[choice];

  if (preset.pending) {
    say(twiml, `${preset.label} has not been set up yet.`);
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  setPendingLocationChoice(req, preset);
  return res.type("text/xml").send(locationModeTwiml(preset).toString());
});

app.post("/set-location-mode", async (req, res) => {
  const twiml = new VoiceResponse();

  if (isBackKey(req)) {
    clearPendingLocationChoice(req);
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }

  const choice = parseLocationModeChoice(req);
  const pendingLocation = getPendingLocationChoice(req);

  console.log("SET-LOCATION-MODE From:", req.body.From, "choice:", choice, "pendingLocation:", pendingLocation);

  if (!pendingLocation) {
    say(twiml, "Please choose a location first.");
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  if (!choice) {
    say(twiml, "I did not understand that choice.");
    return res.type("text/xml").send(locationModeTwiml(pendingLocation).toString());
  }

  clearPendingLocationChoice(req);

  if (choice === "1") {
    saveTemporaryLocationForCall(req, pendingLocation);
    say(twiml, `${pendingLocation.label} will be used for the rest of this call only.`);
  } else {
    clearTemporaryLocationForCall(req);
    saveLocationForCaller(req, pendingLocation);
    say(twiml, `Your location has been saved as ${pendingLocation.label}.`);
  }

  const canadaAlert = await fetchCanadianAlert(pendingLocation);
  if (canadaAlert) {
    say(twiml, buildCanadianAlertSpeech(pendingLocation, canadaAlert));
  }

  buildMainMenuInto(twiml, pendingLocation.label);
  return res.type("text/xml").send(twiml.toString());
});

app.post("/menu", async (req, res) => {
  const choice = parseMainMenuChoice(req);
  const location = getActiveLocation(req);
  const twiml = new VoiceResponse();

  console.log("MENU CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("MENU choice:", choice);
  console.log("MENU active location:", location);

  if (choice === "5") {
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  if (choice === "6") {
    return res.type("text/xml").send(voicemailPromptTwiml().toString());
  }

  if (!location) {
    say(twiml, "You need to choose a location first.");
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  try {
    const forecast = await fetchForecast(location);

    if (choice === "1") {
      return res.type("text/xml").send(forecastDayPromptTwiml(location, forecast).toString());
    }

    if (choice === "2") {
      setLastPlayback(req, { type: "hourly", hours: 6 });
      say(twiml, nextHoursSpeech(location, forecast, 6));
      twiml.redirect({ method: "POST" }, "/after-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    if (choice === "3") {
      setLastPlayback(req, { type: "current" });
      say(twiml, currentWeatherSpeech(location, forecast));
      twiml.redirect({ method: "POST" }, "/after-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    say(twiml, "I did not understand that choice.");
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("MENU weather error:", error.message);
    console.error("MENU weather details:", error.response?.data || null);
    speakWeatherError(
      twiml,
      error,
      "Sorry, I could not retrieve the weather right now. Please try again later."
    );
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/forecast-day", async (req, res) => {
  const location = getActiveLocation(req);
  const twiml = new VoiceResponse();

  console.log("FORECAST-DAY CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("FORECAST-DAY active location:", location);

  if (isBackKey(req)) {
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }

  if (!location) {
    say(twiml, "You need to choose a location first.");
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  try {
    const forecast = await fetchForecast(location);
    const selected = parseForecastDayChoice(req, forecast, location.timezone);

    if (selected === "all") {
      setLastPlayback(req, { type: "all7" });
      say(twiml, sevenDayForecastSpeech(location, forecast));
      twiml.redirect({ method: "POST" }, "/after-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    if (selected < 0 || selected > 6) {
      say(twiml, "I did not understand the forecast day.");
      return res.type("text/xml").send(forecastDayPromptTwiml(location, forecast).toString());
    }

    setLastPlayback(req, { type: "daily", index: selected });
    say(twiml, dailyForecastSpeech(location, forecast, selected));
    twiml.redirect({ method: "POST" }, "/after-prompt");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("FORECAST-DAY error:", error.message);
    console.error("FORECAST-DAY details:", error.response?.data || null);
    speakWeatherError(twiml, error, "Sorry, I could not retrieve that forecast.");
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/after-prompt", (req, res) => {
  res.type("text/xml").send(afterActionTwiml().toString());
});

app.post("/after", async (req, res) => {
  const twiml = new VoiceResponse();

  console.log("AFTER Digits:", req.body.Digits, "SpeechResult:", req.body.SpeechResult);

  if (isBackKey(req)) {
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }

  const choice = parseAfterChoice(req);

  if (choice === "5") {
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  if (choice === "6") {
    return res.type("text/xml").send(voicemailPromptTwiml().toString());
  }

  if (choice === "#") {
    const location = getActiveLocation(req);
    const lastPlayback = getLastPlayback(req);

    if (!location || !lastPlayback) {
      say(twiml, "There is nothing to repeat yet.");
      twiml.redirect({ method: "POST" }, "/main-menu");
      return res.type("text/xml").send(twiml.toString());
    }

    try {
      const forecast = await fetchForecast(location);
      const speech = await buildPlaybackSpeech(location, forecast, lastPlayback);

      if (!speech) {
        say(twiml, "There is nothing to repeat yet.");
        twiml.redirect({ method: "POST" }, "/main-menu");
        return res.type("text/xml").send(twiml.toString());
      }

      say(twiml, speech);
      twiml.redirect({ method: "POST" }, "/after-prompt");
      return res.type("text/xml").send(twiml.toString());
    } catch (error) {
      console.error("AFTER repeat error:", error.message);
      console.error("AFTER repeat details:", error.response?.data || null);
      speakWeatherError(twiml, error, "Sorry, I could not repeat that weather report.");
      twiml.redirect({ method: "POST" }, "/main-menu");
      return res.type("text/xml").send(twiml.toString());
    }
  }

  say(twiml, "Goodbye.");
  twiml.hangup();
  return res.type("text/xml").send(twiml.toString());
});

app.post("/handle-recording", (req, res) => {
  const twiml = new VoiceResponse();

  const callSid = String(req.body.CallSid || "");
  const from = String(req.body.From || "");
  const to = String(req.body.To || "");
  const recordingUrl = String(req.body.RecordingUrl || "");
  const recordingDuration = String(req.body.RecordingDuration || "");

  console.log("HANDLE-RECORDING", {
    callSid,
    from,
    to,
    recordingUrl,
    recordingDuration
  });

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
  const recordingSid = String(req.body.RecordingSid || "");
  const recordingUrl = String(req.body.RecordingUrl || "");
  const recordingStatus = String(req.body.RecordingStatus || "");
  const recordingDuration = String(req.body.RecordingDuration || "");
  const from = String(req.body.From || "");
  const to = String(req.body.To || "");

  console.log("RECORDING-STATUS", {
    callSid,
    recordingSid,
    recordingUrl,
    recordingStatus,
    recordingDuration
  });

  updateVoicemailRecord(callSid, {
    callSid,
    recordingSid,
    recordingUrl,
    recordingStatus,
    recordingDuration,
    from,
    to,
    updatedAt: new Date().toISOString(),
    source: "recordingStatusCallback"
  });

  if (callSid) {
    pendingLocationChoiceByCall.delete(callSid);
  }

  res.status(204).send();
});

app.post("/call-ended", (req, res) => {
  clearCallState(req);
  res.status(204).send();
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Weather phone server running on port ${port}`);
});