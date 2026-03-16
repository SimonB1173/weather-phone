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

const FORECAST_CACHE_MS = 10 * 60 * 1000; // 10 minutes fresh
const STALE_FORECAST_MS = 60 * 60 * 1000; // 1 hour stale fallback
const ALERT_CACHE_MS = 10 * 60 * 1000; // 10 minutes

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
    longitude: -74.1860,
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

function pushIf(parts, condition, text) {
  if (condition) parts.push(text);
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
    if (!value || (now - value.timestamp) > STALE_FORECAST_MS) {
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

function weatherCodeToText(code) {
  const map = {
    0: "clear sky",
    1: "mostly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "freezing fog",
    51: "light drizzle",
    53: "moderate drizzle",
    55: "heavy drizzle",
    56: "light freezing drizzle",
    57: "heavy freezing drizzle",
    61: "light rain",
    63: "moderate rain",
    65: "heavy rain",
    66: "light freezing rain",
    67: "heavy freezing rain",
    71: "light snow",
    73: "moderate snow",
    75: "heavy snow",
    77: "snow grains",
    80: "light rain showers",
    81: "moderate rain showers",
    82: "heavy rain showers",
    85: "light snow showers",
    86: "heavy snow showers",
    95: "thunderstorm",
    96: "thunderstorm with hail",
    99: "severe thunderstorm with hail"
  };
  return map[code] || "mixed weather";
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

  if (digit) return digit;
  if (speech.includes("menu")) return "1";
  if (speech.includes("change") || speech.includes("location")) return "2";
  if (speech.includes("repeat") || speech.includes("again") || speech.includes("current")) return "3";
  if (
    speech.includes("message") ||
    speech.includes("comment") ||
    speech.includes("advertise") ||
    speech.includes("voicemail") ||
    speech.includes("voice mail")
  ) return "4";
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
    `Your saved location is ${savedLocationName}. ` +
      `Press or say 1 for current weather. ` +
      `Press or say 2 for the next 6 hours. ` +
      `Press or say 3 for the 7 day forecast menu. ` +
      `Press or say 4 for important alerts. ` +
      `Press or say 5 to change location. ` +
      `Press or say 6 to leave a voice message for advertising or comments.`
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
    `Choose your location. ` +
      `Press or say 1 for Montreal. ` +
      `Press or say 2 for Tosh. ` +
      `Press or say 3 for Brooklyn New York. ` +
      `Press or say 4 for Monsey. ` +
      `Press or say 5 for Monroe.`
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
    `Press or say 1 for the main menu. ` +
      `Press or say 2 to change location. ` +
      `Press or say 3 to hear current weather again. ` +
      `Press or say 4 to leave a voice message.`
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
    `Choose a forecast day for ${location.name}. ` +
      `Press or say ${choices.join(". ")}.`
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

function currentWeatherSpeech(location, forecast) {
  const c = forecast.current;
  const parts = [
    `Current weather for ${location.name}.`,
    `Conditions: ${weatherCodeToText(c.weather_code)}.`,
    `Temperature ${Math.round(c.temperature_2m)} degrees Celsius.`,
    `Feels like ${Math.round(c.apparent_temperature)} degrees.`
  ];

  pushIf(parts, (c.wind_speed_10m || 0) > 0, `Wind ${Math.round(c.wind_speed_10m)} kilometres per hour.`);
  pushIf(parts, (c.cloud_cover || 0) > 0, `Cloud cover ${Math.round(c.cloud_cover)} percent.`);
  pushIf(parts, (c.rain || 0) > 0, `Rain ${c.rain} millimetres.`);
  pushIf(parts, (c.showers || 0) > 0, `Showers ${c.showers} millimetres.`);
  pushIf(parts, (c.snowfall || 0) > 0, `Snowfall ${c.snowfall} centimetres.`);

  return parts.join(" ");
}

function nextHoursSpeech(location, forecast, hours = 6) {
  const now = Date.now();
  const h = forecast.hourly;
  const tz = location.timezone || "UTC";
  const items = [];

  for (let i = 0; i < h.time.length; i++) {
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
    return `I could not find future hourly forecast data for ${location.name}.`;
  }

  let speech = `Next ${items.length} forecast hours for ${location.name}. `;

  for (const item of items) {
    const parts = [
      `At ${timeLabel(item.time, tz)},`,
      `${weatherCodeToText(item.code)},`,
      `${Math.round(item.temp)} degrees.`
    ];

    pushIf(parts, (item.rainChance || 0) > 0, `Rain chance ${Math.round(item.rainChance)} percent.`);
    pushIf(parts, (item.wind || 0) > 0, `Wind ${Math.round(item.wind)} kilometres per hour.`);
    pushIf(parts, (item.clouds || 0) > 0, `Cloud cover ${Math.round(item.clouds)} percent.`);
    pushIf(parts, (item.rain || 0) > 0, `Rain ${item.rain} millimetres.`);
    pushIf(parts, (item.showers || 0) > 0, `Showers ${item.showers} millimetres.`);
    pushIf(parts, (item.snow || 0) > 0, `Snow ${item.snow} centimetres.`);

    speech += parts.join(" ") + " ";
  }

  return speech.trim();
}

function dailyForecastSpeech(location, forecast, index) {
  const d = forecast.daily;
  if (index < 0 || index >= d.time.length) {
    return `That forecast day is not available for ${location.name}.`;
  }

  const label =
    index === 0 ? "today" :
    index === 1 ? "tomorrow" :
    dayName(d.time[index], location.timezone);

  const parts = [
    `Forecast for ${label} in ${location.name}.`,
    `Conditions: ${weatherCodeToText(d.weather_code[index])}.`,
    `High ${Math.round(d.temperature_2m_max[index])} degrees.`,
    `Low ${Math.round(d.temperature_2m_min[index])} degrees.`
  ];

  pushIf(
    parts,
    (d.precipitation_probability_max[index] || 0) > 0,
    `Maximum precipitation chance ${Math.round(d.precipitation_probability_max[index])} percent.`
  );
  pushIf(
    parts,
    (d.wind_speed_10m_max[index] || 0) > 0,
    `Maximum wind ${Math.round(d.wind_speed_10m_max[index])} kilometres per hour.`
  );
  pushIf(parts, (d.rain_sum[index] || 0) > 0, `Rain ${d.rain_sum[index]} millimetres.`);
  pushIf(parts, (d.showers_sum[index] || 0) > 0, `Showers ${d.showers_sum[index]} millimetres.`);
  pushIf(parts, (d.snowfall_sum[index] || 0) > 0, `Snowfall ${d.snowfall_sum[index]} centimetres.`);

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

    if ((d.wind_speed_10m_max[i] || 0) >= 50) alerts.push(`Strong wind possible ${label}.`);
    if ((d.snowfall_sum[i] || 0) >= 5) alerts.push(`Significant snow possible ${label}.`);
    if ((d.rain_sum[i] || 0) >= 10) alerts.push(`Heavy rain possible ${label}.`);
    if ([95, 96, 99].includes(d.weather_code[i])) alerts.push(`Thunderstorm risk ${label}.`);
  }

  if (!alerts.length) {
    return `No major forecast alerts found for ${location.name} in the next seven days.`;
  }

  return `Important forecast alerts for ${location.name}. ${alerts.join(" ")}`;
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

  if (cached && (now - cached.timestamp) < ALERT_CACHE_MS) {
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

  console.log("VOICE CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("VOICE saved location:", saved);

  say(
    twiml,
    "Welcome to Weather Line. This service is sponsored by Lipa Supermarket."
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
  const choice = parseLocationChoice(req);
  const twiml = new VoiceResponse();

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
  const choice = parseAfterChoice(req);
  const twiml = new VoiceResponse();

  console.log("AFTER choice:", choice);

  if (choice === "1") {
    twiml.redirect({ method: "POST" }, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  if (choice === "2") {
    return res.type("text/xml").send(locationMenuTwiml().toString());
  }

  if (choice === "3") {
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

  if (choice === "4") {
    return res.type("text/xml").send(voicemailPromptTwiml().toString());
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