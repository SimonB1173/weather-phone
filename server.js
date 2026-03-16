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

const FORECAST_CACHE_MS = 10 * 60 * 1000;
const STALE_FORECAST_MS = 60 * 60 * 1000;
const ALERT_CACHE_MS = 10 * 60 * 1000;

const DATA_FILE = path.join(__dirname, "saved-locations.json");
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

function dayName(iso, tz) {
  return new Date(iso).toLocaleDateString("en-US", {
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
  if (speech.includes("current") || speech.includes("now")) return "1";
  if (speech.includes("hour")) return "2";
  if (
    speech.includes("forecast") ||
    speech.includes("day") ||
    speech.includes("today") ||
    speech.includes("tomorrow")
  ) return "3";
  if (speech.includes("alert") || speech.includes("warning")) return "4";
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

  if (digit === "5" || digit === "6" || digit === "9") return digit;
  if (speech.includes("change") || speech.includes("location")) return "5";
  if (
    speech.includes("message") ||
    speech.includes("comment") ||
    speech.includes("advertise") ||
    speech.includes("voicemail") ||
    speech.includes("voice mail")
  ) return "6";
  if (speech.includes("repeat") || speech.includes("again") || speech.includes("current")) return "9";
  return "";
}

function parseForecastDayChoice(req, forecast, timezone) {
  const digit = String(req.body.Digits || "").trim();
  const speech = String(req.body.SpeechResult || "").toLowerCase();

  if (/^[1-7]$/.test(digit)) return parseInt(digit, 10) - 1;
  if (speech.includes("today")) return 0;
  if (speech.includes("tomorrow")) return 1;

  for (let i = 0; i < Math.min(7, forecast.daily.time.length); i++) {
    const name = dayName(forecast.daily.time[i], timezone).toLowerCase();
    if (speech.includes(name)) return i;
  }

  const match = speech.match(/\b([1-7])\b/);
  if (match) return parseInt(match[1], 10) - 1;

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

function buildMainMenuInto(twiml, savedLocationName) {
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
    `${savedLocationName}. ` +
      `Press 1 current weather. ` +
      `2 hourly forecast. ` +
      `3 daily forecast. ` +
      `4 alerts. ` +
      `5 location. ` +
      `6 voicemail.`
  );

  twiml.redirect({ method: "POST" }, "/voice");
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
    `Choose location. ` +
      `Press 1 Montreal. ` +
      `2 Tosh. ` +
      `3 Brooklyn. ` +
      `4 Monsey. ` +
      `5 Monroe. ` +
      `Star for main menu.`
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
    `Press star for main menu. ` +
      `Press 5 to change location. ` +
      `Press 6 for voicemail. ` +
      `Press 9 to hear current weather again.`
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
    timeout: 7,
    speechTimeout: "auto",
    numDigits: 1
  });

  const choices = [];
  for (let i = 0; i < Math.min(7, forecast.daily.time.length); i++) {
    const label =
      i === 0 ? "today" :
      i === 1 ? "tomorrow" :
      dayName(forecast.daily.time[i], location.timezone);

    choices.push(`${i + 1} for ${label}`);
  }

  say(
    gather,
    `Choose a day for ${location.label || location.name}. ` +
      `${choices.join(". ")}. ` +
      `Star for main menu.`
  );

  twiml.redirect({ method: "POST" }, "/voice");
  return twiml;
}

function voicemailPromptTwiml() {
  const twiml = new VoiceResponse();

  say(
    twiml,
    "Please leave your message after the beep. " +
      "You can use this for advertising requests or comments. " +
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
    3: "overcast",
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
  if (p < 15) return "sunny";
  if (p < 40) return "mostly sunny";
  if (p < 65) return "partly cloudy";
  if (p < 85) return "mostly cloudy";
  return "overcast";
}

function describeCurrentCondition(current) {
  const codeText = weatherCodeToText(current.weather_code);
  const cloudText = cloudCoverToPhrase(current.cloud_cover);

  if ([0, 1, 2, 3].includes(Number(current.weather_code))) {
    return cloudText;
  }

  return codeText;
}

function describeDailyCondition(code, maxCloudCoverLike) {
  if ([0, 1, 2, 3].includes(Number(code))) {
    return cloudCoverToPhrase(maxCloudCoverLike);
  }
  return weatherCodeToText(code);
}

function formatTemp(value) {
  return `${Math.round(value)} degrees`;
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
        precipitationProbability: h.precipitation_probability?.[i],
        rain: h.rain?.[i] || 0,
        showers: h.showers?.[i] || 0,
        snowfall: h.snowfall?.[i] || 0,
        cloudCover: h.cloud_cover?.[i] || 0,
        wind: h.wind_speed_10m?.[i] || 0,
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

      const totalRain = g.items.reduce((sum, item) => sum + Number(item.rain || 0) + Number(item.showers || 0), 0);
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

function buildHourlyEventSummary(entries, tz) {
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

  const parts = [];

  const stormWindow = pickMainWindow(stormGroups);
  if (stormWindow) {
    parts.push(
      `Thunderstorms are most likely from ${formatTimeRange(stormWindow.start, stormWindow.end, tz)}.`
    );
  }

  const snowWindow = pickMainWindow(snowGroups);
  if (snowWindow) {
    const maxSnow = Math.max(...snowWindow.items.map((x) => Number(x.snowfall || 0)));
    const snowLabel = maxSnow >= 1 ? "Snow" : "Light snow";
    parts.push(
      `${snowLabel} is most likely from ${formatTimeRange(snowWindow.start, snowWindow.end, tz)}.`
    );
  }

  const rainWindow = pickMainWindow(rainGroups);
  if (rainWindow) {
    const maxRain = Math.max(
      ...rainWindow.items.map((x) => Number(x.rain || 0) + Number(x.showers || 0))
    );
    const rainLabel = maxRain >= 2 ? "Heavier rain" : "Rain or showers";
    parts.push(
      `${rainLabel} is most likely from ${formatTimeRange(rainWindow.start, rainWindow.end, tz)}.`
    );
  }

  return parts.join(" ");
}

function currentWeatherSpeech(location, forecast) {
  const c = forecast.current;
  const condition = describeCurrentCondition(c);

  const parts = [
    `Current weather for ${location.label || location.name}.`,
    `It is ${condition}.`,
    `The temperature is ${formatTemp(c.temperature_2m)}.`
  ];

  if ((c.wind_speed_10m || 0) >= 15) {
    parts.push(`Wind is around ${Math.round(c.wind_speed_10m)} kilometres per hour.`);
  }

  if ((c.rain || 0) > 0) {
    parts.push(`Rain is falling now.`);
  }

  if ((c.showers || 0) > 0) {
    parts.push(`There are showers right now.`);
  }

  if ((c.snowfall || 0) > 0) {
    parts.push(`Snow is falling now.`);
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
  const totalRain = block.items.reduce((sum, x) => sum + Number(x.rain || 0) + Number(x.showers || 0), 0);
  const totalSnow = block.items.reduce((sum, x) => sum + Number(x.snow || 0), 0);

  const condition = [0, 1, 2, 3].includes(Number(first.code))
    ? cloudCoverToPhrase(first.clouds)
    : weatherCodeToText(first.code);

  const parts = [
    `From ${start} until ${end}, expect ${condition}.`,
    `Around ${Math.round(avgTemp)} degrees.`
  ];

  if (maxRainChance >= 40) {
    parts.push(`Chance of precipitation up to ${Math.round(maxRainChance)} percent.`);
  }

  if (totalRain > 0) {
    if (totalRain >= 2) {
      parts.push(`Rain or showers are likely.`);
    } else {
      parts.push(`A little rain or a few showers are possible.`);
    }
  }

  if (totalSnow > 0) {
    if (totalSnow >= 1) {
      parts.push(`Snow is likely.`);
    } else {
      parts.push(`A little snow is possible.`);
    }
  }

  if (maxWind >= 28) {
    parts.push(`It may be windy.`);
  } else if (maxWind >= 18) {
    parts.push(`A breeze is expected.`);
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
  const h = forecast.hourly;
  const tz = location.timezone || "UTC";
  const items = [];

  for (let i = 0; i < (h.time || []).length; i++) {
    const t = new Date(h.time[i]).getTime();
    if (t >= now) {
      items.push({
        time: h.time[i],
        temp: h.temperature_2m[i],
        rainChance: h.precipitation_probability[i],
        rain: h.rain[i],
        showers: h.showers[i],
        snow: h.snowfall[i],
        clouds: h.cloud_cover[i],
        wind: h.wind_speed_10m[i],
        code: h.weather_code[i]
      });
    }
    if (items.length >= hours) break;
  }

  if (!items.length) {
    return `I could not find future hourly forecast data for ${location.label || location.name}.`;
  }

  const blocks = buildSmartHourlyBlocks(items);
  const topBlocks = blocks.slice(0, 4);

  const opening = `Here is the next ${items.length} hours for ${location.label || location.name}.`;
  const body = topBlocks.map((block) => summarizeHourlyBlock(block, tz)).join(" ");

  return `${opening} ${body}`.trim();
}

function dailyForecastSpeech(location, forecast, index) {
  const d = forecast.daily;
  if (index < 0 || index >= d.time.length) {
    return `That forecast day is not available for ${location.label || location.name}.`;
  }

  const dateStr = d.time[index];
  const hourlyEntries = getHourlyEntriesForDay(forecast, dateStr);

  const label =
    index === 0 ? "today" :
    index === 1 ? "tomorrow" :
    dayName(dateStr, location.timezone);

  const middayCloudSample = hourlyEntries.length
    ? hourlyEntries[Math.floor(hourlyEntries.length / 2)].cloudCover
    : 50;

  const condition = describeDailyCondition(d.weather_code[index], middayCloudSample);

  const parts = [
    `Forecast for ${label} in ${location.label || location.name}.`,
    `Expect ${condition}.`,
    `The high will be ${Math.round(d.temperature_2m_max[index])} degrees.`,
    `The low will be ${Math.round(d.temperature_2m_min[index])} degrees.`
  ];

  if ((d.precipitation_probability_max[index] || 0) >= 35) {
    parts.push(
      `The highest chance of precipitation is around ${Math.round(d.precipitation_probability_max[index])} percent.`
    );
  }

  if ((d.wind_speed_10m_max[index] || 0) >= 20) {
    parts.push(
      `Winds may reach about ${Math.round(d.wind_speed_10m_max[index])} kilometres per hour.`
    );
  }

  if ((d.rain_sum[index] || 0) > 0) {
    parts.push(`Total rain may be around ${Number(d.rain_sum[index]).toFixed(1)} millimetres.`);
  }

  if ((d.showers_sum[index] || 0) > 0) {
    parts.push(`Showers are possible.`);
  }

  if ((d.snowfall_sum[index] || 0) > 0) {
    parts.push(`Total snowfall may be around ${Number(d.snowfall_sum[index]).toFixed(1)} centimetres.`);
  }

  const timingSummary = buildHourlyEventSummary(hourlyEntries, location.timezone);
  if (timingSummary) {
    parts.push(timingSummary);
  }

  return parts.join(" ");
}

function alertsSpeech(location, forecast) {
  const d = forecast.daily;
  const alerts = [];

  for (let i = 0; i < Math.min(7, d.time.length); i++) {
    const label =
      i === 0 ? "today" :
      i === 1 ? "tomorrow" :
      dayName(d.time[i], location.timezone);

    if ((d.wind_speed_10m_max[i] || 0) >= 50) alerts.push(`Strong wind is possible ${label}.`);
    if ((d.snowfall_sum[i] || 0) >= 5) alerts.push(`Significant snow is possible ${label}.`);
    if ((d.rain_sum[i] || 0) >= 10) alerts.push(`Heavy rain is possible ${label}.`);
    if ([95, 96, 99].includes(d.weather_code[i])) alerts.push(`Thunderstorms are possible ${label}.`);
  }

  if (!alerts.length) {
    return `No major forecast alerts were found for ${location.label || location.name} in the next seven days.`;
  }

  return `Important forecast alerts for ${location.label || location.name}. ${alerts.join(" ")}`;
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
  const parts = [`Environment Canada alert for ${location.label || location.name}.`];

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
    current: [
      "temperature_2m",
      "apparent_temperature",
      "rain",
      "showers",
      "snowfall",
      "cloud_cover",
      "wind_speed_10m",
      "weather_code"
    ].join(","),
    hourly: [
      "temperature_2m",
      "precipitation_probability",
      "rain",
      "showers",
      "snowfall",
      "cloud_cover",
      "wind_speed_10m",
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
      "wind_speed_10m_max"
    ].join(","),
    timezone: location.timezone || "America/Toronto",
    forecast_days: 7
  };

  const requestPromise = (async () => {
    try {
      const response = await axios.get("https://api.open-meteo.com/v1/forecast", {
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

app.get("/", (req, res) => {
  res.send("Weather phone server is running.");
});

app.get("/debug-weather", async (req, res) => {
  try {
    const location = PRESET_LOCATIONS["1"];
    const before = getCachedForecast(location, { allowStale: true });
    const forecast = await fetchForecast(location);
    const after = getCachedForecast(location, { allowStale: true });
    const canadaAlert = await fetchCanadianAlert(location);

    res.json({
      ok: true,
      cache_key: cacheKeyForLocation(location),
      cache_hit_before: !!before,
      cache_hit_after: !!after,
      cached_locations: forecastCache.size,
      in_flight_requests: inFlightForecasts.size,
      current: forecast.current,
      hourly_keys: Object.keys(forecast.hourly || {}),
      daily_keys: Object.keys(forecast.daily || {}),
      canada_alert: canadaAlert
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
  const saved = getSavedLocation(req);
  const timezone = saved?.timezone || "America/Toronto";
  const greeting = getGreetingForTime(timezone);

  console.log("VOICE CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("VOICE saved location:", saved);

  say(
    twiml,
    `${greeting}. Welcome to Weather Line. This service is sponsored by Lipa Supermarket.`
  );

  if (saved) {
    const canadaAlert = await fetchCanadianAlert(saved);
    if (canadaAlert) {
      say(twiml, buildCanadianAlertSpeech(saved, canadaAlert));
    }

    buildMainMenuInto(twiml, saved.label || saved.name);
  } else {
    say(twiml, "No saved location was found for this number.");
    twiml.redirect({ method: "POST" }, "/location-menu-prompt");
  }

  res.type("text/xml").send(twiml.toString());
});

app.post("/location-menu-prompt", (req, res) => {
  res.type("text/xml").send(locationMenuTwiml().toString());
});

app.post("/set-location-choice", (req, res) => {
  const twiml = new VoiceResponse();

  if (isBackKey(req)) {
    twiml.redirect({ method: "POST" }, "/voice");
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

  saveLocationForCaller(req, preset);

  say(twiml, `Your location has been saved as ${preset.label}.`);
  buildMainMenuInto(twiml, preset.label);

  return res.type("text/xml").send(twiml.toString());
});

app.post("/menu", async (req, res) => {
  const choice = parseMainMenuChoice(req);
  const location = getSavedLocation(req);
  const twiml = new VoiceResponse();

  console.log("MENU CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("MENU choice:", choice);
  console.log("MENU saved location:", location);

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
      say(twiml, currentWeatherSpeech(location, forecast));
      twiml.redirect({ method: "POST" }, "/after-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    if (choice === "2") {
      say(twiml, nextHoursSpeech(location, forecast, 6));
      twiml.redirect({ method: "POST" }, "/after-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    if (choice === "3") {
      return res.type("text/xml").send(forecastDayPromptTwiml(location, forecast).toString());
    }

    if (choice === "4") {
      say(twiml, alertsSpeech(location, forecast));
      twiml.redirect({ method: "POST" }, "/after-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    say(twiml, "I did not understand that choice.");
    twiml.redirect({ method: "POST" }, "/voice");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("MENU weather error:", error.message);
    console.error("MENU weather details:", error.response?.data || null);
    speakWeatherError(
      twiml,
      error,
      "Sorry, I could not retrieve the weather right now. Please try again later."
    );
    twiml.redirect({ method: "POST" }, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/forecast-day", async (req, res) => {
  const location = getSavedLocation(req);
  const twiml = new VoiceResponse();

  console.log("FORECAST-DAY CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("FORECAST-DAY saved location:", location);

  if (isBackKey(req)) {
    twiml.redirect({ method: "POST" }, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  if (!location) {
    say(twiml, "You need to choose a location first.");
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  try {
    const forecast = await fetchForecast(location);
    const idx = parseForecastDayChoice(req, forecast, location.timezone);

    if (idx < 0 || idx > 6) {
      say(twiml, "I did not understand the forecast day.");
      return res.type("text/xml").send(forecastDayPromptTwiml(location, forecast).toString());
    }

    say(twiml, dailyForecastSpeech(location, forecast, idx));
    twiml.redirect({ method: "POST" }, "/after-prompt");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("FORECAST-DAY error:", error.message);
    console.error("FORECAST-DAY details:", error.response?.data || null);
    speakWeatherError(twiml, error, "Sorry, I could not retrieve that forecast.");
    twiml.redirect({ method: "POST" }, "/voice");
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
    twiml.redirect({ method: "POST" }, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  const choice = parseAfterChoice(req);

  if (choice === "5") {
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  if (choice === "6") {
    return res.type("text/xml").send(voicemailPromptTwiml().toString());
  }

  if (choice === "9") {
    const location = getSavedLocation(req);

    if (!location) {
      say(twiml, "You need to choose a location first.");
      return res.type("text/xml").send(locationMenuTwiml().toString());
    }

    try {
      const forecast = await fetchForecast(location);
      say(twiml, currentWeatherSpeech(location, forecast));
      twiml.redirect({ method: "POST" }, "/after-prompt");
      return res.type("text/xml").send(twiml.toString());
    } catch (error) {
      console.error("AFTER repeat current error:", error.message);
      console.error("AFTER repeat current details:", error.response?.data || null);
      speakWeatherError(twiml, error, "Sorry, I could not retrieve the weather.");
      twiml.redirect({ method: "POST" }, "/voice");
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

  res.status(204).send();
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Weather phone server running on port ${port}`);
});