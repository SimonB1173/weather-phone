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
    alertFeedUrl: "https://weather.gc.ca/rss/alerts/45.5017_-73.5673_e.xml"
  },
  "2": {
    id: "tosh",
    name: "Boisbriand, Quebec, Canada",
    latitude: 45.6192,
    longitude: -73.8396,
    timezone: "America/Toronto",
    label: "Tosh",
    country: "CA"
  },
  "3": {
    id: "laurentians",
    name: "Laurentians, Quebec, Canada",
    latitude: 46.0168,
    longitude: -74.2239,
    timezone: "America/Toronto",
    label: "Laurentians",
    country: "CA"
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
  return String(req.body.Digits || "").trim();
}

function parseAfterChoice(req) {
  return String(req.body.Digits || "").trim();
}

function parseForecastDayChoice(req) {
  const digit = String(req.body.Digits || "").trim();
  if (digit === "0") return "all";
  if (/^[1-7]$/.test(digit)) return parseInt(digit, 10) - 1;
  return -1;
}

function parseLocationChoice(req) {
  const digit = String(req.body.Digits || "").trim();
  if (/^[1-6]$/.test(digit)) return digit;
  return "";
}

function buildMainMenuInto(twiml, activeLocationName) {
  const gather = twiml.gather({
    input: "dtmf",
    action: "/menu",
    method: "POST",
    timeout: 8,
    numDigits: 1
  });

  say(
    gather,
    `Your current location is ${activeLocationName}. ` +
      `Press 1 for the 7 day forecast, ` +
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
    input: "dtmf",
    action: "/set-location-choice",
    method: "POST",
    timeout: 7,
    numDigits: 1
  });

  say(
    gather,
    `Press 1 for Montreal, ` +
      `2 for Tosh, ` +
      `3 for Laurentians, ` +
      `4 for Brooklyn, ` +
      `5 for Monsey, ` +
      `6 for Monroe. ` +
      `Press star for main menu.`
  );

  twiml.redirect({ method: "POST" }, "/location-menu-prompt");
  return twiml;
}

function afterActionTwiml() {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    input: "dtmf",
    action: "/after",
    method: "POST",
    timeout: 6,
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
    input: "dtmf",
    action: "/forecast-day",
    method: "POST",
    timeout: 8,
    numDigits: 1
  });

  const dailyTimes = forecast.daily?.time || [];
  const parts = [
    `For the 7 day forecast in ${placeLabel(location)}, press 0 to hear all 7 days.`
  ];

  if (dailyTimes[0]) parts.push(`Press 1 for today.`);
  if (dailyTimes[1]) parts.push(`Press 2 for tomorrow.`);

  for (let i = 2; i < Math.min(7, dailyTimes.length); i++) {
    parts.push(`Press ${i + 1} for ${dayName(dailyTimes[i], location.timezone)}.`);
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

function cloudCoverNightPhrase(percent) {
  const p = Number(percent || 0);
  if (p < 20) return "clear";
  if (p < 45) return "partly cloudy";
  if (p < 75) return "mainly cloudy";
  return "cloudy";
}

function weatherCodeToOfficialPhrase(code, cloudCoverLike = 60, isNight = false) {
  const n = Number(code);

  if ([0, 1, 2, 3].includes(n)) {
    return isNight ? cloudCoverNightPhrase(cloudCoverLike) : cloudCoverToPhrase(cloudCoverLike);
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
  return weatherCodeToOfficialPhrase(current.weather_code, current.cloud_cover, false);
}

function describeCurrentWeatherSentence(current) {
  const condition = describeCurrentCondition(current);

  if (condition === "sunny") return "It is sunny";
  if (condition === "mostly sunny") return "It is mostly sunny";
  if (condition === "a mix of sun and cloud") return "There is a mix of sun and cloud";
  if (condition === "mainly cloudy") return "It is mainly cloudy";
  if (condition === "cloudy") return "It is cloudy";
  if (condition === "clear") return "It is clear";
  if (condition === "partly cloudy") return "It is partly cloudy";
  if (condition === "foggy") return "It is foggy";
  if (condition === "freezing fog") return "There is freezing fog";
  if (condition === "thunderstorms") return "There are thunderstorms";
  if (condition === "thunderstorms with hail") return "There are thunderstorms with hail";
  if (condition === "severe thunderstorms with hail") return "There are severe thunderstorms with hail";

  return `It is ${condition}`;
}

function formatSignedTemp(value) {
  const n = Math.round(Number(value || 0));
  if (n < 0) return `minus ${Math.abs(n)}`;
  if (n > 0) return `${n}`;
  return "zero";
}

function formatForecastTempValue(value) {
  const n = Math.round(Number(value || 0));
  if (n < 0) return `minus ${Math.abs(n)}`;
  if (n > 0) return `plus ${n}`;
  return "zero";
}

function uvIndexPhrase(value) {
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return "";
  const rounded = Math.round(n);

  if (rounded <= 2) return `UV index ${rounded} or low.`;
  if (rounded <= 5) return `UV index ${rounded} or moderate.`;
  if (rounded <= 7) return `UV index ${rounded} or high.`;
  if (rounded <= 10) return `UV index ${rounded} or very high.`;
  return `UV index ${rounded} or extreme.`;
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

function minValue(values) {
  const nums = values.map(Number).filter((v) => Number.isFinite(v));
  if (!nums.length) return 0;
  return Math.min(...nums);
}

function firstNonNull(values) {
  for (const value of values) {
    if (value !== undefined && value !== null && Number.isFinite(Number(value))) {
      return Number(value);
    }
  }
  return null;
}

function capitalizeSentence(text) {
  const s = String(text || "").trim();
  if (!s) return "";
  return s.charAt(0).toUpperCase() + s.slice(1);
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

function splitEntries(entries) {
  const split = Math.max(1, Math.floor(entries.length / 2));
  return {
    first: entries.slice(0, split),
    second: entries.slice(split)
  };
}

function getPeriodTimingWord(entries, timezone, type = "day") {
  if (!entries.length) return "later";
  const pivot = entries[Math.max(0, Math.floor(entries.length / 2))]?.time || entries[entries.length - 1]?.time;
  const hour = getHourInTz(pivot, timezone);

  if (type === "night") {
    if (hour < 22) return "early this evening";
    if (hour < 24) return "this evening";
    if (hour < 3) return "after midnight";
    return "near morning";
  }

  if (hour < 12) return "in the morning";
  if (hour < 18) return "in the afternoon";
  return "later";
}

function precipitationTypeFromEntries(entries) {
  const totalSnow = entries.reduce((sum, e) => sum + Number(e.snowfall || 0), 0);
  const totalRain = entries.reduce((sum, e) => sum + Number(e.rain || 0) + Number(e.showers || 0), 0);
  const maxChance = maxValue(entries.map((e) => e.precipitationProbability));
  const hasStorm = entries.some((e) => [95, 96, 99].includes(Number(e.weatherCode)));

  if (hasStorm) return "thunderstorms";
  if (totalSnow > 0.2 && totalRain > 0.15) return "flurries or rain showers";
  if (totalSnow > 0.2) return "flurries";
  if (totalRain > 0.2) return "rain showers";
  if (maxChance >= 50) {
    const snowCodes = entries.some((e) => [71, 73, 75, 77, 85, 86].includes(Number(e.weatherCode)));
    const rainCodes = entries.some((e) => [51, 53, 55, 61, 63, 65, 80, 81, 82].includes(Number(e.weatherCode)));
    if (snowCodes && rainCodes) return "flurries or rain showers";
    if (snowCodes) return "flurries";
    if (rainCodes) return "rain showers";
  }

  return "";
}

function skyPhraseForEntries(entries, isNight = false) {
  if (!entries.length) return isNight ? "cloudy" : "cloudy";
  const avgCloud = average(entries.map((e) => e.cloudCover));
  const mainCode = entries[Math.floor(entries.length / 2)]?.weatherCode ?? entries[0]?.weatherCode ?? 3;
  return weatherCodeToOfficialPhrase(mainCode, avgCloud, isNight);
}

function describeTransitionSentence(entries, timezone, isNight = false) {
  if (!entries.length) return "";

  const { first, second } = splitEntries(entries);
  const firstPrecip = precipitationTypeFromEntries(first);
  const secondPrecip = precipitationTypeFromEntries(second);
  const firstSky = skyPhraseForEntries(first, isNight);
  const secondSky = skyPhraseForEntries(second, isNight);
  const timing = getPeriodTimingWord(second.length ? second : entries, timezone, isNight ? "night" : "day");

  if (firstPrecip && !secondPrecip) {
    const endSky = secondSky === "clear" ? "clearing" : secondSky;
    return `A few ${firstPrecip} ending ${timing} then ${endSky}.`;
  }

  if (!firstPrecip && secondPrecip) {
    if (firstSky === "sunny" && !isNight) {
      return `Sunny. Becoming a mix of sun and cloud and a chance of ${secondPrecip} ${timing}.`;
    }
    return `${capitalizeSentence(firstSky)}. A chance of ${secondPrecip} ${timing}.`;
  }

  if (firstPrecip && secondPrecip) {
    const chance = Math.round(maxValue(entries.map((e) => e.precipitationProbability)));
    if (chance >= 30 && chance < 70) {
      return `Cloudy with ${chance} percent chance of ${firstPrecip}.`;
    }
    return `${capitalizeSentence(firstPrecip)} at times.`;
  }

  if (!firstPrecip && !secondPrecip) {
    if (firstSky !== secondSky) {
      if (!isNight && firstSky === "sunny" && secondSky === "a mix of sun and cloud") {
        return `Sunny. Becoming a mix of sun and cloud in the afternoon.`;
      }

      if (isNight && firstSky === "partly cloudy" && secondSky === "cloudy") {
        return `Partly cloudy. Becoming cloudy near midnight.`;
      }

      if (isNight && firstSky === "cloudy" && secondSky === "partly cloudy") {
        return `Cloudy. Clearing near morning.`;
      }

      return `${capitalizeSentence(firstSky)}. Becoming ${secondSky} ${timing}.`;
    }

    return `${capitalizeSentence(firstSky)}.`;
  }

  return "";
}

function describeWindForCurrent(current) {
  const speed = Math.round(Number(current.wind_speed_10m || 0));
  const gusts = Math.round(Number(current.wind_gusts_10m || 0));
  const direction = degreesToCompass(current.wind_direction_10m);

  if (speed >= 15) {
    if (gusts >= speed + 10) {
      return `Wind ${direction} ${speed} kilometres per hour, gusting to ${gusts}.`;
    }
    return `Wind ${direction} ${speed} kilometres per hour.`;
  }

  return "";
}

function currentWeatherSpeech(location, forecast) {
  const c = forecast.current || {};
  const parts = [
    `Current weather for ${placeLabel(location)}.`,
    `${describeCurrentWeatherSentence(c)}.`,
    `Temperature ${formatSignedTemp(c.temperature_2m)} degrees.`
  ];

  const apparent = firstNonNull([c.apparent_temperature]);
  if (
    apparent !== null &&
    Math.round(apparent) !== Math.round(Number(c.temperature_2m || 0)) &&
    apparent <= Number(c.temperature_2m || 0) - 3
  ) {
    parts.push(`Wind chill ${formatSignedTemp(apparent)}.`);
  }

  const windLine = describeWindForCurrent(c);
  if (windLine) parts.push(windLine);

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
    parts.push(`${Math.round(maxRainChance)} percent chance of precipitation.`);
  }

  if (totalRain > 0) {
    if (totalRain >= 3) parts.push(`Rain at times.`);
    else if (totalRain >= 1) parts.push(`A few showers.`);
    else parts.push(`A slight chance of rain.`);
  }

  if (totalSnow > 0) {
    if (totalSnow >= 2) parts.push(`Snow at times.`);
    else if (totalSnow >= 0.8) parts.push(`Some flurries.`);
    else parts.push(`A slight chance of flurries.`);
  }

  if (maxWind >= 35 || maxGust >= 55) parts.push(`Windy.`);
  else if (maxWind >= 18) parts.push(`A light breeze.`);

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
  const body = topBlocks.map((block) => summarizeHourlyBlock(block, tz)).join(" ");

  return `Here is the next ${items.length} hours for ${placeLabel(location)}. ${body}`.trim();
}

function getDayLabel(index, dateStr, timezone) {
  if (index === 0) return "Today";
  return dayName(dateStr, timezone);
}

function getNightLabel(index, dateStr, timezone) {
  if (index === 0) return "Tonight";
  return `${dayName(dateStr, timezone)} night`;
}

function buildChanceSentence(entries) {
  const chance = Math.round(maxValue(entries.map((e) => e.precipitationProbability)));
  const precip = precipitationTypeFromEntries(entries);

  if (!precip || chance < 25) return "";

  if (chance < 70) {
    return `Cloudy with ${chance} percent chance of ${precip}.`;
  }

  return `${capitalizeSentence(precip)}.`;
}

function describeDetailedPeriodIntro(entries, timezone, isNight = false) {
  if (!entries.length) return "Cloudy.";

  const transition = describeTransitionSentence(entries, timezone, isNight);
  if (transition) return transition;

  const chanceSentence = buildChanceSentence(entries);
  if (chanceSentence) return chanceSentence;

  return `${capitalizeSentence(skyPhraseForEntries(entries, isNight))}.`;
}

function describeSimplePeriodIntro(entries, isNight = false) {
  if (!entries.length) return "Cloudy.";

  const chanceSentence = buildChanceSentence(entries);
  if (chanceSentence) return chanceSentence;

  return `${capitalizeSentence(skyPhraseForEntries(entries, isNight))}.`;
}

function describeWindLine(entries, timezone, type = "day") {
  if (!entries.length) return "";

  const { first, second } = splitEntries(entries);
  const firstWind = Math.round(maxValue(first.map((e) => e.wind)));
  const secondWind = Math.round(maxValue(second.map((e) => e.wind)));
  const firstGust = Math.round(maxValue(first.map((e) => e.windGusts)));
  const secondGust = Math.round(maxValue(second.map((e) => e.windGusts)));
  const firstDir = degreesToCompass(average(first.map((e) => e.windDirection)));
  const secondDir = degreesToCompass(average(second.map((e) => e.windDirection)));
  const timing = getPeriodTimingWord(second.length ? second : entries, timezone, type);

  const firstActive = firstWind >= 15 || firstGust >= 25;
  const secondActive = secondWind >= 15 || secondGust >= 25;

  if (!firstActive && !secondActive) return "";

  function formatWindSegment(dir, speed, gust) {
    let text = `${dir} ${speed}`;
    if (gust >= speed + 10) text += ` gusting to ${gust}`;
    return text;
  }

  if (firstActive && secondActive) {
    if (secondWind <= 15 && secondGust < 25 && firstWind >= 20) {
      return `Wind ${firstDir} ${firstWind}${firstGust >= firstWind + 10 ? ` kilometres per hour gusting to ${firstGust}` : " kilometres per hour"} diminishing to 15 kilometres per hour or less ${timing}.`;
    }

    if (firstDir !== secondDir || Math.abs(firstWind - secondWind) >= 10 || Math.abs(firstGust - secondGust) >= 15) {
      if (firstWind > secondWind || firstGust > secondGust) {
        return `Wind ${firstDir} ${firstWind}${firstGust >= firstWind + 10 ? ` kilometres per hour gusting to ${firstGust}` : " kilometres per hour"} diminishing to ${formatWindSegment(secondDir, secondWind, secondGust)} ${timing}.`;
      }

      return `Wind ${firstDir} ${firstWind}${firstGust >= firstWind + 10 ? ` kilometres per hour gusting to ${firstGust}` : " kilometres per hour"} becoming ${formatWindSegment(secondDir, secondWind, secondGust)} ${timing}.`;
    }
  }

  const maxWind = Math.round(maxValue(entries.map((e) => e.wind)));
  const maxGust = Math.round(maxValue(entries.map((e) => e.windGusts)));
  const dir = degreesToCompass(average(entries.map((e) => e.windDirection)));

  if (maxWind <= 15 && maxGust < 25) {
    return `Wind up to 15 kilometres per hour.`;
  }

  return `Wind ${dir} ${maxWind}${maxGust >= maxWind + 10 ? ` kilometres per hour gusting to ${maxGust}` : " kilometres per hour"}.`;
}

function describeDayTemperatureLine(forecast, index) {
  const high = forecast.daily?.temperature_2m_max?.[index];
  if (!Number.isFinite(Number(high))) return "";
  return `High ${formatForecastTempValue(high)}.`;
}

function describeNightTemperatureLine(forecast, index, entries) {
  const d = forecast.daily || {};
  const nextDayMin = d.temperature_2m_min?.[index + 1];
  const sameDayMin = d.temperature_2m_min?.[index];
  const lowValue = Number.isFinite(Number(nextDayMin)) ? nextDayMin : sameDayMin;

  if (!Number.isFinite(Number(lowValue))) return "";

  const minTemp = minValue(entries.map((e) => e.temperature));
  const maxTemp = maxValue(entries.map((e) => e.temperature));

  if (entries.length && Math.abs(maxTemp - minTemp) <= 2) {
    return `Temperature steady near ${formatSignedTemp(average(entries.map((e) => e.temperature)))}.`;
  }

  return `Low ${formatForecastTempValue(lowValue)}.`;
}

function describeDayWindChill(entries) {
  if (!entries.length) return "";

  const { first, second } = splitEntries(entries);
  const firstApp = Math.round(minValue(first.map((e) => Number(e.apparentTemperature ?? e.temperature ?? 0))));
  const secondApp = Math.round(minValue(second.map((e) => Number(e.apparentTemperature ?? e.temperature ?? 0))));
  const firstTemp = Math.round(minValue(first.map((e) => Number(e.temperature ?? 0))));
  const secondTemp = Math.round(minValue(second.map((e) => Number(e.temperature ?? 0))));

  const firstHas = firstApp <= firstTemp - 3;
  const secondHas = secondApp <= secondTemp - 3;

  if (firstHas && secondHas && firstApp !== secondApp) {
    return `Wind chill ${formatSignedTemp(firstApp)} in the morning and ${formatSignedTemp(secondApp)} in the afternoon.`;
  }

  if (firstHas || secondHas) {
    const value = firstHas ? firstApp : secondApp;
    const part = firstHas ? "in the morning" : "in the afternoon";
    return `Wind chill ${formatSignedTemp(value)} ${part}.`;
  }

  return "";
}

function describeNightWindChill(entries) {
  if (!entries.length) return "";

  const minApp = Math.round(minValue(entries.map((e) => Number(e.apparentTemperature ?? e.temperature ?? 0))));
  const minTemp = Math.round(minValue(entries.map((e) => Number(e.temperature ?? 0))));

  if (minApp <= minTemp - 3) {
    return `Wind chill near ${formatSignedTemp(minApp)}.`;
  }

  return "";
}

function buildDetailedDaySection(location, forecast, index) {
  const d = forecast.daily || {};
  const dateStr = d.time?.[index];
  const entries = getHourlyEntriesForDay(forecast, dateStr);
  const label = getDayLabel(index, dateStr, location.timezone);

  const parts = [
    `${label}.`,
    describeDetailedPeriodIntro(entries, location.timezone, false)
  ];

  const wind = describeWindLine(entries, location.timezone, "day");
  const high = describeDayTemperatureLine(forecast, index);
  const windChill = describeDayWindChill(entries);
  const uv = uvIndexPhrase(d.uv_index_max?.[index]);

  if (wind) parts.push(wind);
  if (high) parts.push(high);
  if (windChill) parts.push(windChill);
  if (uv) parts.push(uv);

  return parts.join(" ");
}

function buildDetailedNightSection(location, forecast, index) {
  const d = forecast.daily || {};
  const dateStr = d.time?.[index];
  const entries = getNightEntriesForDayIndex(forecast, index, location.timezone);
  const label = getNightLabel(index, dateStr, location.timezone);

  const parts = [
    `${label}.`,
    describeDetailedPeriodIntro(entries, location.timezone, true)
  ];

  const wind = describeWindLine(entries, location.timezone, "night");
  const temp = describeNightTemperatureLine(forecast, index, entries);
  const windChill = describeNightWindChill(entries);

  if (wind) parts.push(wind);
  if (temp) parts.push(temp);
  if (windChill) parts.push(windChill);

  return parts.join(" ");
}

function buildShortDaySection(location, forecast, index) {
  const d = forecast.daily || {};
  const dateStr = d.time?.[index];
  const entries = getHourlyEntriesForDay(forecast, dateStr);
  const label = dayName(dateStr, location.timezone);

  return `${label}. ${describeSimplePeriodIntro(entries, false)} High ${formatForecastTempValue(d.temperature_2m_max?.[index])}.`;
}

function buildShortNightSection(location, forecast, index) {
  const d = forecast.daily || {};
  const dateStr = d.time?.[index];
  const entries = getNightEntriesForDayIndex(forecast, index, location.timezone);
  const label = `${dayName(dateStr, location.timezone)} night`;

  const nextDayMin = d.temperature_2m_min?.[index + 1];
  const sameDayMin = d.temperature_2m_min?.[index];
  const lowValue = Number.isFinite(Number(nextDayMin)) ? nextDayMin : sameDayMin;

  return `${label}. ${describeSimplePeriodIntro(entries, true)} Low ${formatForecastTempValue(lowValue)}.`;
}

function dailyForecastSpeech(location, forecast, index) {
  const d = forecast.daily || {};
  if (index < 0 || index >= (d.time || []).length) {
    return `That forecast day is not available for ${placeLabel(location)}.`;
  }

  if (index === 0) {
    return buildDetailedDaySection(location, forecast, index);
  }

  const daySection = buildDetailedDaySection(location, forecast, index);
  const nightSection = buildDetailedNightSection(location, forecast, index);

  return `${daySection} ${nightSection}`.trim();
}

function sevenDayForecastSpeech(location, forecast) {
  const d = forecast.daily || {};
  const count = Math.min(7, (d.time || []).length);
  const parts = [`Here is the 7 day forecast for ${placeLabel(location)}.`];

  if (count > 0) {
    parts.push(buildDetailedNightSection(location, forecast, 0));
  }

  if (count > 1) {
    parts.push(buildDetailedDaySection(location, forecast, 1));
    parts.push(buildDetailedNightSection(location, forecast, 1));
  }

  for (let i = 2; i < count; i++) {
    parts.push(buildShortDaySection(location, forecast, i));
    parts.push(buildShortNightSection(location, forecast, i));
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
    twiml.redirect({ method: "POST" }, "/location-menu-prompt");
  }
}

app.get("/", (req, res) => {
  res.send("Weather phone server is running.");
});

app.get("/voice", (req, res) => {
  const twiml = new VoiceResponse();
  say(twiml, "Weather Line is running. Please call the phone number to use the interactive weather menu.");
  res.type("text/xml").send(twiml.toString());
});

app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  try {
    clearCallState(req);

    const greeting = getGreetingForTime("America/Toronto");

    console.log("VOICE CallSid:", req.body.CallSid, "From:", req.body.From);

    if (INTRO_AUDIO_URL) {
      twiml.play(INTRO_AUDIO_URL);
    }

    twiml.say(
      SAY_OPTIONS,
      `${greeting}, welcome to Weather Line.`
    );

    twiml.redirect({ method: "POST" }, "/location-menu-prompt");
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
    twiml.redirect({ method: "POST" }, "/main-menu");
    return res.type("text/xml").send(twiml.toString());
  }

  const choice = parseLocationChoice(req);

  console.log("SET-LOCATION-CHOICE choice:", choice);

  if (!choice || !PRESET_LOCATIONS[choice]) {
    say(twiml, "I did not understand that location choice.");
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  const preset = PRESET_LOCATIONS[choice];
  saveTemporaryLocationForCall(req, preset);
  clearPendingLocationChoice(req);

  say(twiml, `${preset.label}.`);
  buildMainMenuInto(twiml, preset.label);
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
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  try {
    const forecast = await fetchForecast(location);
    const selected = parseForecastDayChoice(req);

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

  console.log("AFTER Digits:", req.body.Digits);

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