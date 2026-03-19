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
const menuStateByCall = new Map();
const menuHistoryByCall = new Map();
const unitPreferenceByCall = new Map();

const FORECAST_CACHE_MS = 10 * 60 * 1000;
const STALE_FORECAST_MS = 60 * 60 * 1000;
const ALERT_CACHE_MS = 10 * 60 * 1000;

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
    ecCurrentCoords: "45.483,-73.633",
    alertFeedUrl: "https://weather.gc.ca/rss/alerts/45.5017_-73.5673_e.xml"
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
    ecCurrentCoords: "45.613,-73.838"
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
    ecCurrentCoords: "46.206,-74.470"
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

function say(twiml, text) {
  const cleaned = String(text || "")
    .replace(/\s+/g, " ")
    .replace(/\s+([,.!?])/g, "$1")
    .trim();

  if (!cleaned) return;

  const parts = cleaned
    .split(/(?<=[.!?])\s+/)
    .map((s) => s.trim())
    .filter(Boolean);

  for (const part of parts) {
    twiml.say(SAY_OPTIONS, part);
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
  const callKey = getCallKey(req);
  return temporaryLocationsByCall.get(callKey) || null;
}

function saveTemporaryLocationForCall(req, location) {
  const callKey = getCallKey(req);
  temporaryLocationsByCall.set(callKey, location);
}

function clearPendingLocationChoice(req) {
  const callKey = getCallKey(req);
  pendingLocationChoiceByCall.delete(callKey);
}

function getActiveLocation(req) {
  return getTemporaryLocation(req);
}

function setLastPlayback(req, payload) {
  const callKey = getCallKey(req);
  lastPlaybackByCall.set(callKey, payload);
}

function getLastPlayback(req) {
  const callKey = getCallKey(req);
  return lastPlaybackByCall.get(callKey) || null;
}

function setMenuState(req, state) {
  const callKey = getCallKey(req);
  menuStateByCall.set(callKey, state);
}

function getMenuState(req) {
  const callKey = getCallKey(req);
  return menuStateByCall.get(callKey) || "";
}

function getMenuHistory(req) {
  const callKey = getCallKey(req);
  return menuHistoryByCall.get(callKey) || [];
}

function shouldTrackHistoryState(state) {
  return ["location-menu", "us-location-menu", "main-menu", "forecast-menu"].includes(state);
}

function pushMenuHistory(req, state) {
  if (!shouldTrackHistoryState(state)) return;

  const callKey = getCallKey(req);
  const history = menuHistoryByCall.get(callKey) || [];
  if (history[history.length - 1] !== state) {
    history.push(state);
  }
  menuHistoryByCall.set(callKey, history);
}

function popMenuHistory(req) {
  const callKey = getCallKey(req);
  const history = menuHistoryByCall.get(callKey) || [];
  if (history.length > 0) {
    history.pop();
  }
  menuHistoryByCall.set(callKey, history);
  return history;
}

function getUnitPreference(req) {
  const callKey = getCallKey(req);
  return unitPreferenceByCall.get(callKey) || "C";
}

function setUnitPreference(req, unit) {
  const callKey = getCallKey(req);
  unitPreferenceByCall.set(callKey, unit === "F" ? "F" : "C");
}

function toggleUnitPreference(req) {
  const nextUnit = getUnitPreference(req) === "F" ? "C" : "F";
  setUnitPreference(req, nextUnit);
  return nextUnit;
}

function clearCallState(req) {
  const callKey = getCallKey(req);
  temporaryLocationsByCall.delete(callKey);
  pendingLocationChoiceByCall.delete(callKey);
  lastPlaybackByCall.delete(callKey);
  menuStateByCall.delete(callKey);
  menuHistoryByCall.delete(callKey);
  unitPreferenceByCall.delete(callKey);
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

function getDatePartFromLocalIso(iso) {
  return String(iso || "").slice(0, 10);
}

function getHourFromLocalIso(iso) {
  const text = String(iso || "");
  const match = text.match(/T(\d{1,2}):(\d{2})/);
  if (!match) return 12;
  return Number(match[1]);
}

function getMinuteFromLocalIso(iso) {
  const text = String(iso || "");
  const match = text.match(/T(\d{1,2}):(\d{2})/);
  if (!match) return 0;
  return Number(match[2]);
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
  const text = String(iso || "");
  const datePart = getDatePartFromLocalIso(text);
  const [year, month, day] = datePart.split("-").map(Number);
  const hour = getHourFromLocalIso(text);
  const minute = getMinuteFromLocalIso(text);

  if (!year || !month || !day) return text;

  const dt = new Date(Date.UTC(year, month - 1, day, hour, minute, 0));
  dt.setUTCHours(dt.getUTCHours() + Number(hoursToAdd || 0));

  const y = dt.getUTCFullYear();
  const m = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const d = String(dt.getUTCDate()).padStart(2, "0");
  const h = String(dt.getUTCHours()).padStart(2, "0");
  const min = String(dt.getUTCMinutes()).padStart(2, "0");

  return `${y}-${m}-${d}T${h}:${min}`;
}

function timeLabel(iso) {
  return formatLocalIsoTimeLabel(iso);
}

function issuedDateLabelToday(tz) {
  return new Date().toLocaleDateString("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
    timeZone: tz || "UTC"
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

  const hourPart = parts.find((p) => p.type === "hour");
  return Number(hourPart?.value || 12);
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

function getGreetingForTime(timezone) {
  const hour = getNowHourInTz(timezone || "America/Toronto");
  if (hour < 12) return "Good morning";
  if (hour < 18) return "Good afternoon";
  return "Good evening";
}

function isBackKey(req) {
  return getDigits(req) === "*";
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
  if (/^[1-7]$/.test(digit)) return parseInt(digit, 10) - 1;
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
  const converted = tempValueForUnit(value, unit);
  const n = Math.round(converted);
  if (n < 0) return `minus ${Math.abs(n)}`;
  if (n > 0) return `${n}`;
  return "zero";
}

function formatForecastTempValue(value, unit = "C") {
  const converted = tempValueForUnit(value, unit);
  const n = Math.round(converted);
  if (n < 0) return `minus ${Math.abs(n)}`;
  if (n > 0) return `plus ${n}`;
  return "zero";
}

function buildMainMenuInto(twiml, activeLocationName) {
  const gather = twiml.gather(gatherOptions("/menu", 8, 1));

  say(
    gather,
    `${activeLocationName}. Press 1 for the 7 day forecast. Press 2 for hourly forecast. Press 3 for current weather.`
  );

  twiml.redirect({ method: "POST" }, "/main-menu");
}

function locationMenuTwiml({ allowBack = false, allowVoicemail = false } = {}) {
  const twiml = new VoiceResponse();
  const gather = twiml.gather(gatherOptions("/set-location-choice", 7, 1));

  const parts = [
    "Press 1 for Montreal.",
    "2 for Tosh.",
    "3 for Laurentians.",
    "4 for United States."
  ];

  if (allowVoicemail) {
    parts.push("Press 9 to leave a comment or suggestion.");
  }

  if (allowBack) {
    parts.push("Press star for the previous menu.");
  }

  say(gather, parts.join(" "));
  twiml.redirect({ method: "POST" }, "/location-menu-prompt");
  return twiml;
}

function usLocationMenuTwiml() {
  const twiml = new VoiceResponse();
  const gather = twiml.gather(gatherOptions("/set-us-location-choice", 7, 1));

  say(
    gather,
    `For United States, press 1 for Brooklyn. 2 for Monroe. 3 for Monsey. Press star for the previous menu.`
  );

  twiml.redirect({ method: "POST" }, "/us-location-menu-prompt");
  return twiml;
}

function afterActionTwiml(unit = "C") {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    input: "dtmf",
    action: "/after",
    method: "POST",
    timeout: 6,
    numDigits: 1,
    finishOnKey: ""
  });

  const unitText =
    unit === "F"
      ? "Press 7 to switch back to Celsius."
      : "Press 7 to hear it in Fahrenheit.";

  say(
    gather,
    `Press star to go back. Press pound to repeat. Press 5 to change location. ${unitText}`
  );

  twiml.hangup();
  return twiml;
}

function forecastDayPromptTwiml(location, forecast) {
  const twiml = new VoiceResponse();
  const gather = twiml.gather(gatherOptions("/forecast-day", 8, 1));

  const dailyTimes = forecast.daily?.time || [];
  const parts = ["For the seven day forecast, press 0."];

  if (dailyTimes[0]) parts.push("Press 1 for today.");
  if (dailyTimes[1]) parts.push("Press 2 for tomorrow.");

  for (let i = 2; i < Math.min(7, dailyTimes.length); i++) {
    parts.push(`Press ${i + 1} for ${dayName(dailyTimes[i], location.timezone)}.`);
  }

  parts.push("Press star to go back to the previous menu.");

  say(gather, parts.join(" "));
  twiml.redirect({ method: "POST" }, "/forecast-menu");
  return twiml;
}

function voicemailPromptTwiml() {
  const twiml = new VoiceResponse();

  say(
    twiml,
    "Please leave your message after the beep. You can use this for comments or suggestions. When you are finished, just hang up."
  );

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

function playbackWithStarTwiml(text) {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    input: "dtmf",
    action: "/during-playback",
    method: "POST",
    timeout: 1,
    numDigits: 1,
    finishOnKey: ""
  });

  say(gather, text);

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

  return map[n] || "mixed weather";
}

function isNightHourFromIso(iso) {
  const hour = getHourInTz(iso);
  return hour < 6 || hour >= 18;
}

function describeCurrentCondition(current, timezone) {
  const isNight = current?.time ? isNightHourFromIso(current.time, timezone) : false;
  const snow = Number(current?.snowfall || 0);
  const rain = Number(current?.rain || 0);
  const showers = Number(current?.showers || 0);
  const totalLiquid = rain + showers;
  const code = Number(current?.weather_code);

  if (snow > 0) {
    if (snow >= 1.0) return "snow";
    return "light snow";
  }

  if (totalLiquid > 0) {
    if ([80, 81, 82].includes(code)) {
      if (totalLiquid >= 1.5) return "showers";
      return "light showers";
    }
    if (totalLiquid >= 1.5) return "rain";
    return "light rain";
  }

  return weatherCodeToText(code, isNight);
}

function describeCurrentWeatherSentence(current, timezone) {
  const condition = describeCurrentCondition(current, timezone);

  if (condition === "sunny") return "It is sunny";
  if (condition === "mostly sunny") return "It is mostly sunny";
  if (condition === "a mix of sun and cloud") return "There is a mix of sun and cloud";
  if (condition === "clear") return "It is clear";
  if (condition === "mostly clear") return "It is mostly clear";
  if (condition === "partly cloudy") return "It is partly cloudy";
  if (condition === "cloudy") return "It is cloudy";
  if (condition === "foggy") return "It is foggy";
  if (condition === "freezing fog") return "There is freezing fog";
  if (condition === "thunderstorms") return "There are thunderstorms";
  if (condition === "thunderstorms with hail") return "There are thunderstorms with hail";
  if (condition === "severe thunderstorms with hail") return "There are severe thunderstorms with hail";

  return `It is ${condition}`;
}

function compassLettersToWords(direction) {
  const map = {
    N: "north",
    NE: "northeast",
    E: "east",
    SE: "southeast",
    S: "south",
    SW: "southwest",
    W: "west",
    NW: "northwest"
  };

  return map[String(direction || "").toUpperCase()] || String(direction || "").toLowerCase();
}

async function fetchEnvironmentCanadaCurrent(location) {
  if (!location || location.country !== "CA") return null;

  const coordText =
    location.ecCurrentCoords ||
    `${Number(location.latitude).toFixed(3)},${Number(location.longitude).toFixed(3)}`;

  const url = `https://weather.gc.ca/en/location/index.html?coords=${encodeURIComponent(coordText)}`;

  try {
    const response = await axios.get(url, {
      timeout: 10000,
      headers: {
        "User-Agent": "weather-line-canada-current/1.0"
      }
    });

    const html = String(response.data || "");

    function clean(text) {
      return String(text || "")
        .replace(/<[^>]+>/g, " ")
        .replace(/&nbsp;/g, " ")
        .replace(/&amp;/g, "&")
        .replace(/&quot;/g, '"')
        .replace(/&#39;/g, "'")
        .replace(/&lt;/g, "<")
        .replace(/&gt;/g, ">")
        .replace(/\s+/g, " ")
        .trim();
    }

    function match(regex) {
      const m = html.match(regex);
      return m ? clean(m[1]) : "";
    }

    const observedAt = match(/Observed at:\s*([^<\n]+(?:<[^>]+>[^<\n]+)*)/i);
    const dateText = match(/Date:\s*([^<\n]+(?:<[^>]+>[^<\n]+)*)/i);
    const condition = match(/Condition:\s*([^<\n]+(?:<[^>]+>[^<\n]+)*)/i);
    const temperatureText = match(/Temperature:\s*([\-0-9.]+)\s*°C/i);
    const windText = match(/Wind:\s*([^<\n]+(?:<[^>]+>[^<\n]+)*)/i);
    const windChillText = match(/Wind Chill[^:]*:\s*([\-0-9.]+)/i);

    const temperatureC = temperatureText ? Number(temperatureText) : null;
    const windChillC = windChillText ? Number(windChillText) : null;

    let windDirection = "";
    let windSpeedKmh = null;

    if (windText) {
      const windMatch = windText.match(/^([A-Z]{1,3})\s+([0-9.]+)/i);
      if (windMatch) {
        windDirection = String(windMatch[1] || "").toUpperCase();
        windSpeedKmh = Number(windMatch[2]);
      }
    }

    if (!condition && temperatureC === null) {
      return null;
    }

    return {
      source: "environment-canada",
      observedAt,
      dateText,
      condition,
      temperatureC,
      windDirection,
      windSpeedKmh,
      windChillC
    };
  } catch (error) {
    console.error("Environment Canada current fetch failed:", error.message);
    return null;
  }
}

function currentWeatherSpeech(location, forecast, unit = "C", canadaCurrent = null) {
  if (location?.country === "CA" && canadaCurrent) {
    const parts = [`Current weather for ${placeLabel(location)}.`];

    if (canadaCurrent.dateText) {
      parts.push(`Observed at ${canadaCurrent.dateText}.`);
    } else if (canadaCurrent.observedAt) {
      parts.push(`Observed at ${canadaCurrent.observedAt}.`);
    }

    if (canadaCurrent.condition) {
      parts.push(`It is ${String(canadaCurrent.condition).toLowerCase()}.`);
    }

    if (canadaCurrent.temperatureC !== null && Number.isFinite(canadaCurrent.temperatureC)) {
      parts.push(`Temperature ${formatSignedTemp(canadaCurrent.temperatureC, unit)} degrees.`);
    }

    if (
      canadaCurrent.windChillC !== null &&
      Number.isFinite(canadaCurrent.windChillC) &&
      canadaCurrent.temperatureC !== null &&
      Math.round(tempValueForUnit(canadaCurrent.windChillC, unit)) !==
        Math.round(tempValueForUnit(canadaCurrent.temperatureC, unit))
    ) {
      parts.push(`Wind chill ${formatSignedTemp(canadaCurrent.windChillC, unit)}.`);
    }

    if (
      canadaCurrent.windSpeedKmh !== null &&
      Number.isFinite(canadaCurrent.windSpeedKmh) &&
      canadaCurrent.windSpeedKmh > 0
    ) {
      const speed = Math.round(speedValueForUnit(canadaCurrent.windSpeedKmh, unit));
      const dir = compassLettersToWords(canadaCurrent.windDirection);
      parts.push(`Wind ${dir} ${speed} ${speedUnitLabel(unit)}.`);
    }

    return parts.join(" ");
  }

  const c = forecast.current || {};
  const parts = [
    `Current weather for ${placeLabel(location)}.`,
    `Retrieved at ${retrievedTimeLabelNow(location.timezone)}.`,
    `${describeCurrentWeatherSentence(c, location.timezone)}.`,
    `Temperature ${formatSignedTemp(c.temperature_2m, unit)} degrees.`
  ];

  return parts.join(" ");
}

function precipitationTypeFromEntries(entries) {
  const totalSnow = entries.reduce((sum, e) => sum + Number(e.snowfall || e.snow || 0), 0);
  const totalRain = entries.reduce(
    (sum, e) => sum + Number(e.rain || 0) + Number(e.showers || 0),
    0
  );
  const maxChance = Math.max(
    0,
    ...entries.map((e) => Number(e.precipitationProbability ?? e.rainChance ?? 0))
  );
  const hasStorm = entries.some((e) => [95, 96, 99].includes(Number(e.weatherCode || e.code)));
  const hasSnowCode = entries.some((e) =>
    [71, 73, 75, 77, 85, 86].includes(Number(e.weatherCode || e.code))
  );
  const hasRainCode = entries.some((e) =>
    [51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82].includes(
      Number(e.weatherCode || e.code)
    )
  );

  if (hasStorm) return "thunderstorms";
  if ((totalSnow > 0.02 || hasSnowCode) && (totalRain > 0.05 || hasRainCode)) {
    return "flurries or rain showers";
  }
  if (totalSnow > 0.02 || hasSnowCode) {
    return totalSnow >= 1.0 ? "snow" : "flurries";
  }
  if (totalRain > 0.05 || (maxChance >= 35 && hasRainCode)) {
    return totalRain >= 1.5 ? "rain showers" : "light rain";
  }

  return "";
}

function classifyHourlyBucket(item) {
  const code = Number(item.code);
  const rainAmount = Number(item.rain || 0) + Number(item.showers || 0);
  const snowAmount = Number(item.snow || 0);
  const wind = Number(item.wind || 0);
  const precipChance = Number(item.rainChance || 0);
  const isNight = isNightHourFromIso(item.time);

  let condition = weatherCodeToText(code, isNight);

  if (snowAmount > 0) {
    condition = snowAmount >= 1.0 ? "snow" : "light snow";
  } else if (rainAmount > 0) {
    if ([80, 81, 82].includes(code)) {
      condition = rainAmount >= 1.5 ? "showers" : "light showers";
    } else {
      condition = rainAmount >= 1.5 ? "rain" : "light rain";
    }
  }

  let precipTag = "dry";
  if ([95, 96, 99].includes(code)) {
    precipTag = "storm";
  } else if (snowAmount > 0.02 || [71, 73, 75, 77, 85, 86].includes(code)) {
    precipTag = snowAmount >= 1.0 ? "snow" : "light-snow";
  } else if (
    rainAmount > 0.05 ||
    precipChance >= 35 ||
    [51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82].includes(code)
  ) {
    precipTag = rainAmount >= 1.5 ? "rain" : "light-rain";
  }

  let windTag = "calm";
  if (wind >= 28) windTag = "windy";
  else if (wind >= 18) windTag = "breezy";

  const tempBand = Math.round(Number(item.temp || 0) / 2);
  return `${condition}|${precipTag}|${windTag}|${tempBand}`;
}

function summarizeHourlyBlock(block, unit = "C") {
  const first = block.items[0];
  const start = timeLabel(block.start);
  const end = formatLocalIsoTimeLabel(addHoursToLocalIso(block.end, 1));

  const avgTempC =
    block.items.reduce((sum, x) => sum + Number(x.temp || 0), 0) / block.items.length;
  const avgTemp = Math.round(tempValueForUnit(avgTempC, unit));

  const totalRain = block.items.reduce(
    (sum, x) => sum + Number(x.rain || 0) + Number(x.showers || 0),
    0
  );
  const totalSnow = block.items.reduce((sum, x) => sum + Number(x.snow || 0), 0);

  const precipSummary = precipitationTypeFromEntries(
    block.items.map((x) => ({
      snowfall: x.snow,
      rain: x.rain,
      showers: x.showers,
      precipitationProbability: x.rainChance,
      weatherCode: x.code
    }))
  );

  let spokenCondition = weatherCodeToText(first.code, isNightHourFromIso(first.time));
  if (precipSummary === "flurries") spokenCondition = "flurries";
  else if (precipSummary === "snow") spokenCondition = "snow";
  else if (precipSummary === "light rain") spokenCondition = "light rain";
  else if (precipSummary === "rain showers") spokenCondition = "showers";
  else if (precipSummary === "flurries or rain showers") spokenCondition = "a mix of flurries and showers";
  else if (precipSummary === "thunderstorms") spokenCondition = "thunderstorms";

  const parts = [`From ${start} until ${end}, ${spokenCondition}, around ${avgTemp} degrees.`];

  if (totalRain > 0.05) {
    if (totalRain >= 3) parts.push("Rain at times.");
    else parts.push("Light rain or a few showers.");
  }

  if (totalSnow > 0.02) {
    if (totalSnow >= 2) parts.push("Snow at times.");
    else parts.push("Light snow or flurries.");
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

function nextHoursSpeech(location, forecast, hours = 6, unit = "C") {
  const h = forecast.hourly || {};
  const items = [];
  const nowLocal = getCurrentLocalDateParts(location.timezone || "UTC");

  for (let i = 0; i < (h.time || []).length; i++) {
    const t = String(h.time[i] || "");
    const datePart = getDatePartFromLocalIso(t);
    const hourPart = getHourFromLocalIso(t);
    const minutePart = getMinuteFromLocalIso(t);

    const isFuture =
      datePart > nowLocal.date ||
      (datePart === nowLocal.date && hourPart > nowLocal.hour) ||
      (datePart === nowLocal.date &&
        hourPart === nowLocal.hour &&
        minutePart >= nowLocal.minute);

    if (isFuture) {
      items.push({
        time: h.time[i],
        temp: h.temperature_2m?.[i],
        rainChance: h.precipitation_probability?.[i],
        rain: h.rain?.[i],
        showers: h.showers?.[i],
        snow: h.snowfall?.[i],
        wind: h.wind_speed_10m?.[i],
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
  const body = topBlocks.map((block) => summarizeHourlyBlock(block, unit)).join(" ");

  return `Here is the next ${items.length} hours for ${placeLabel(location)}. ${body}`.trim();
}

function dailyForecastSpeech(location, forecast, index, unit = "C") {
  const d = forecast.daily || {};
  if (index < 0 || index >= (d.time || []).length) {
    return `That forecast day is not available for ${placeLabel(location)}.`;
  }

  const day = index === 0 ? "Today" : dayName(d.time[index], location.timezone);
  const high = formatForecastTempValue(d.temperature_2m_max?.[index], unit);
  const lowSource = d.temperature_2m_min?.[index + 1] ?? d.temperature_2m_min?.[index];
  const low = formatForecastTempValue(lowSource, unit);

  if (index === 0) {
    return `${day}. High ${high}.`;
  }

  return `${day}. High ${high}. ${dayName(d.time[index], location.timezone)} night. Low ${low}.`;
}

function sevenDayForecastSpeech(location, forecast, unit = "C") {
  const d = forecast.daily || {};
  const count = Math.min(7, (d.time || []).length);
  const parts = [
    `Issued on ${issuedDateLabelToday(location.timezone)}.`,
    `Retrieved at ${retrievedTimeLabelNow(location.timezone)}.`,
    `Here is the 7 day forecast for ${placeLabel(location)}.`
  ];

  for (let i = 0; i < count; i++) {
    const dayLabel = i === 0 ? "Today" : dayName(d.time[i], location.timezone);
    const high = formatForecastTempValue(d.temperature_2m_max?.[i], unit);
    parts.push(`${dayLabel}. High ${high}.`);

    if (i === 0) {
      const tonightLow = formatForecastTempValue(d.temperature_2m_min?.[1] ?? d.temperature_2m_min?.[0], unit);
      parts.push(`Tonight. Low ${tonightLow}.`);
    } else if (i < count - 1) {
      const low = formatForecastTempValue(d.temperature_2m_min?.[i + 1] ?? d.temperature_2m_min?.[i], unit);
      parts.push(`${dayName(d.time[i], location.timezone)} night. Low ${low}.`);
    }
  }

  return parts.join(" ");
}

async function buildPlaybackSpeech(location, forecast, playback, unit = "C") {
  if (!playback || !playback.type) return "";

  if (playback.type === "current") {
    let canadaCurrent = null;
    if (location?.country === "CA") {
      canadaCurrent = await fetchEnvironmentCanadaCurrent(location);
    }
    return currentWeatherSpeech(location, forecast, unit, canadaCurrent);
  }

  if (playback.type === "hourly") {
    return nextHoursSpeech(location, forecast, playback.hours || 6, unit);
  }

  if (playback.type === "daily") {
    return dailyForecastSpeech(location, forecast, playback.index, unit);
  }

  if (playback.type === "all7") {
    return sevenDayForecastSpeech(location, forecast, unit);
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

async function fetchForecast(location) {
  pruneForecastCache();

  const cached = getCachedForecast(location);
  if (cached) return cached.data;

  const key = cacheKeyForLocation(location);
  if (inFlightForecasts.has(key)) {
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

      setCachedForecast(location, data);
      return data;
    } catch (error) {
      const stale = getCachedForecast(location, { allowStale: true });
      if (stale) return stale.data;
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

    setMenuState(req, "main-menu");
    buildMainMenuInto(twiml, placeLabel(activeLocation));
  } else {
    twiml.redirect({ method: "POST" }, "/location-menu-prompt");
  }
}

async function buildStateTwiml(req, state, { push = true } = {}) {
  if (push && shouldTrackHistoryState(state)) {
    pushMenuHistory(req, state);
  }

  setMenuState(req, state);

  if (state === "location-menu") {
    const hasActiveLocation = !!getActiveLocation(req);
    return locationMenuTwiml({
      allowBack: hasActiveLocation,
      allowVoicemail: !hasActiveLocation
    });
  }

  if (state === "us-location-menu") {
    return usLocationMenuTwiml();
  }

  if (state === "main-menu") {
    const twiml = new VoiceResponse();
    await buildMainMenuResponse(req, twiml);
    return twiml;
  }

  if (state === "forecast-menu") {
    const location = getActiveLocation(req);
    if (!location) {
      return locationMenuTwiml({ allowBack: false, allowVoicemail: true });
    }

    const forecast = await fetchForecast(location);
    return forecastDayPromptTwiml(location, forecast);
  }

  if (state === "playback") {
    const location = getActiveLocation(req);
    const lastPlayback = getLastPlayback(req);
    const twiml = new VoiceResponse();

    if (!location || !lastPlayback) {
      say(twiml, "There is nothing to repeat yet.");
      twiml.redirect({ method: "POST" }, "/main-menu");
      return twiml;
    }

    const forecast = await fetchForecast(location);
    const speech = await buildPlaybackSpeech(
      location,
      forecast,
      lastPlayback,
      getUnitPreference(req)
    );

    if (!speech) {
      say(twiml, "There is nothing to repeat yet.");
      twiml.redirect({ method: "POST" }, "/main-menu");
      return twiml;
    }

    return playbackWithStarTwiml(speech);
  }

  if (state === "after-prompt") {
    return afterActionTwiml(getUnitPreference(req));
  }

  if (state === "voicemail") {
    return voicemailPromptTwiml();
  }

  return locationMenuTwiml({ allowBack: false, allowVoicemail: true });
}

async function goBackOneMenu(req) {
  const currentState = getMenuState(req);
  const history = getMenuHistory(req);

  if (currentState === "playback" || currentState === "after-prompt") {
    const previousTrackedState = history[history.length - 1] || "location-menu";
    return buildStateTwiml(req, previousTrackedState, { push: false });
  }

  if (history.length <= 1) {
    return buildStateTwiml(req, "location-menu", { push: false });
  }

  popMenuHistory(req);
  const updatedHistory = getMenuHistory(req);
  const previousState = updatedHistory[updatedHistory.length - 1] || "location-menu";

  return buildStateTwiml(req, previousState, { push: false });
}

app.get("/", (req, res) => {
  res.send("Weather phone server is running.");
});

app.get("/voice", (req, res) => {
  const twiml = new VoiceResponse();
  say(
    twiml,
    "Weather Line is running. Please call the phone number to use the interactive weather menu."
  );
  res.type("text/xml").send(twiml.toString());
});

app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    clearCallState(req);
    setUnitPreference(req, "C");

    const greeting = getGreetingForTime("America/Toronto");

    if (INTRO_AUDIO_URL) {
      twiml.play(INTRO_AUDIO_URL);
    }

    twiml.say(
      SAY_OPTIONS,
      `${greeting}, welcome to Weather Line. ` +
        `We are continuously improving our system, with many exciting new features coming in the coming days. ` +
        `you are welcome to leave comments or ideas for improvement by pressing number 9.`
    );

    twiml.redirect({ method: "POST" }, "/location-menu-prompt");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    say(twiml, "Sorry, an application error occurred. Please try again.");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/main-menu", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    await buildMainMenuResponse(req, twiml);

    const history = getMenuHistory(req);
    if (!history.length || history[history.length - 1] !== "main-menu") {
      pushMenuHistory(req, "main-menu");
    }

    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
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

app.post("/set-location-choice", async (req, res) => {
  const twiml = new VoiceResponse();
  const hasActiveLocation = !!getActiveLocation(req);

  if (isBackKey(req)) {
    if (hasActiveLocation) {
      const backTwiml = await goBackOneMenu(req);
      return res.type("text/xml").send(backTwiml.toString());
    }

    const menuTwiml = await buildStateTwiml(req, "location-menu", { push: false });
    return res.type("text/xml").send(menuTwiml.toString());
  }

  const choice = parseLocationChoice(req);

  if (choice === "9" && !hasActiveLocation) {
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

  const preset = PRESET_LOCATIONS[choice];
  saveTemporaryLocationForCall(req, preset);
  clearPendingLocationChoice(req);

  pushMenuHistory(req, "main-menu");
  setMenuState(req, "main-menu");

  buildMainMenuInto(twiml, preset.label);
  return res.type("text/xml").send(twiml.toString());
});

app.post("/set-us-location-choice", async (req, res) => {
  const twiml = new VoiceResponse();

  if (isBackKey(req)) {
    const backTwiml = await goBackOneMenu(req);
    return res.type("text/xml").send(backTwiml.toString());
  }

  const choice = parseUSLocationChoice(req);

  if (!choice || !US_LOCATIONS[choice]) {
    say(twiml, "I did not understand that location choice.");
    const menuTwiml = await buildStateTwiml(req, "us-location-menu", { push: false });
    return res.type("text/xml").send(menuTwiml.toString());
  }

  const preset = US_LOCATIONS[choice];
  saveTemporaryLocationForCall(req, preset);
  clearPendingLocationChoice(req);

  pushMenuHistory(req, "main-menu");
  setMenuState(req, "main-menu");

  buildMainMenuInto(twiml, preset.label);
  return res.type("text/xml").send(twiml.toString());
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
    speakWeatherError(twiml, error, "Sorry, I could not retrieve that forecast.");
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/menu", async (req, res) => {
  const choice = parseMainMenuChoice(req);
  const location = getActiveLocation(req);
  const twiml = new VoiceResponse();

  if (isBackKey(req)) {
    twiml.redirect({ method: "POST" }, "/location-menu-prompt");
    return res.type("text/xml").send(twiml.toString());
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
      setLastPlayback(req, { type: "hourly", hours: 6 });
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

  if (isBackKey(req)) {
    const backTwiml = await goBackOneMenu(req);
    return res.type("text/xml").send(backTwiml.toString());
  }

  if (!location) {
    const locationTwiml = await buildStateTwiml(req, "location-menu");
    return res.type("text/xml").send(locationTwiml.toString());
  }

  try {
    await fetchForecast(location);
    const selected = parseForecastDayChoice(req);

    if (selected === "all") {
      setLastPlayback(req, { type: "all7" });
      const playbackTwiml = await buildStateTwiml(req, "playback", { push: false });
      return res.type("text/xml").send(playbackTwiml.toString());
    }

    if (selected < 0 || selected > 6) {
      say(twiml, "I did not understand the forecast day.");
      const forecastTwiml = await buildStateTwiml(req, "forecast-menu", { push: false });
      return res.type("text/xml").send(forecastTwiml.toString());
    }

    setLastPlayback(req, { type: "daily", index: selected });
    const playbackTwiml = await buildStateTwiml(req, "playback", { push: false });
    return res.type("text/xml").send(playbackTwiml.toString());
  } catch (error) {
    speakWeatherError(twiml, error, "Sorry, I could not retrieve that forecast.");
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/during-playback", async (req, res) => {
  if (isBackKey(req)) {
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

  if (isBackKey(req)) {
    const backTwiml = await goBackOneMenu(req);
    return res.type("text/xml").send(backTwiml.toString());
  }

  const choice = parseAfterChoice(req);

  if (choice === "5") {
    const locationTwiml = await buildStateTwiml(req, "location-menu");
    return res.type("text/xml").send(locationTwiml.toString());
  }

  if (choice === "#") {
    const playbackTwiml = await buildStateTwiml(req, "playback", { push: false });
    return res.type("text/xml").send(playbackTwiml.toString());
  }

  if (choice === "7") {
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