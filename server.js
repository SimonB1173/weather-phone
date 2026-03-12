const express = require("express");
const axios = require("axios");
const twilio = require("twilio");

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

const VoiceResponse = twilio.twiml.VoiceResponse;

const SAY_OPTIONS = {
  voice: "alice",
  language: "en-CA"
};

// Store by CallSid so the same phone call keeps its state across Twilio webhooks
const callSessions = new Map();

const CUSTOM_POSTAL_ALIASES = {
  "428427": "H2V4B7"
};

function say(twiml, text) {
  twiml.say(SAY_OPTIONS, text);
}

function sessionKey(req) {
  return req.body.CallSid || req.body.From || "unknown";
}

function getSession(req) {
  const key = sessionKey(req);
  if (!callSessions.has(key)) {
    callSessions.set(key, {});
  }
  return callSessions.get(key);
}

function getSavedLocation(req) {
  return getSession(req).location || null;
}

function saveLocation(req, location) {
  getSession(req).location = location;
}

function normalizeText(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function cleanPostalCode(v) {
  return String(v || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
}

function pushIf(parts, condition, text) {
  if (condition) parts.push(text);
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
  return new Date(iso).toLocaleDateString("en-CA", {
    weekday: "long",
    timeZone: tz || "UTC"
  });
}

function timeLabel(iso, tz) {
  return new Date(iso).toLocaleTimeString("en-CA", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
    timeZone: tz || "UTC"
  });
}

function parseMenuChoice(req) {
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
  return "";
}

function parseAfterChoice(req) {
  const digit = String(req.body.Digits || "").trim();
  const speech = String(req.body.SpeechResult || "").toLowerCase();

  if (digit) return digit;
  if (speech.includes("menu")) return "1";
  if (speech.includes("change") || speech.includes("location")) return "2";
  if (speech.includes("repeat") || speech.includes("again") || speech.includes("current")) return "3";
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

function buildMenuInto(twiml, savedLocationName) {
  const gather = twiml.gather({
    input: "speech dtmf",
    action: "/menu",
    method: "POST",
    timeout: 6,
    speechTimeout: "auto",
    numDigits: 1
  });

  if (savedLocationName) {
    say(
      gather,
      `Your saved location is ${savedLocationName}. ` +
      `Press or say 1 for current weather. ` +
      `2 for the next 6 hours. ` +
      `3 for the 7 day forecast menu. ` +
      `4 for important alerts. ` +
      `5 to change location.`
    );
  } else {
    say(
      gather,
      `No location is saved yet. ` +
      `Press or say 5 to set your location.`
    );
  }

  twiml.redirect({ method: "POST" }, "/voice");
}

function locationPromptTwiml() {
  const twiml = new VoiceResponse();
  const gather = twiml.gather({
    input: "speech dtmf",
    action: "/set-location",
    method: "POST",
    timeout: 7,
    speechTimeout: "auto"
  });

  say(
    gather,
    `Say a city, province, or postal code in Canada. ` +
    `You can also type a 6 character postal code on the keypad.`
  );

  twiml.redirect({ method: "POST" }, "/voice");
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
    `2 to change location. ` +
    `3 to hear current weather again.`
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

const KEYMAP = {
  "2": ["A", "B", "C"],
  "3": ["D", "E", "F"],
  "4": ["G", "H", "I"],
  "5": ["J", "K", "L"],
  "6": ["M", "N", "O"],
  "7": ["P", "Q", "R", "S"],
  "8": ["T", "U", "V"],
  "9": ["W", "X", "Y", "Z"]
};

function expandPostalDigits(digits) {
  const d = String(digits || "").replace(/\D/g, "");
  if (d.length !== 6) return [];

  const p1 = KEYMAP[d[0]] || [];
  const p2 = KEYMAP[d[2]] || [];
  const p3 = KEYMAP[d[4]] || [];
  const results = [];

  for (const a of p1) {
    for (const b of p2) {
      for (const c of p3) {
        results.push(`${a}${d[1]}${b}${d[3]}${c}${d[5]}`);
      }
    }
  }

  return results;
}

async function searchNominatim(params) {
  const response = await axios.get("https://nominatim.openstreetmap.org/search", {
    params: {
      format: "jsonv2",
      limit: 5,
      addressdetails: 1,
      countrycodes: "ca",
      ...params
    },
    timeout: 12000,
    headers: {
      "User-Agent": "weather-line-canada/1.0",
      "Accept-Language": "en-CA,en;q=0.9"
    }
  });

  return response.data || [];
}

function formatNominatimResult(r) {
  const a = r.address || {};

  const cityName =
    a.city ||
    a.town ||
    a.village ||
    a.municipality ||
    a.hamlet ||
    a.suburb ||
    a.county ||
    a.state_district ||
    r.display_name;

  const province =
    a.state ||
    a.province ||
    a.region ||
    "";

  const country =
    a.country || "Canada";

  return {
    name: `${cityName}${province ? ", " + province : ""}, ${country}`,
    latitude: parseFloat(r.lat),
    longitude: parseFloat(r.lon),
    timezone: "America/Toronto"
  };
}

async function resolveLocation(input) {
  const raw = normalizeText(input);
  if (!raw) return null;

  const cleaned = cleanPostalCode(raw);
  const aliasPostal = CUSTOM_POSTAL_ALIASES[cleaned];
  const postal = aliasPostal || cleaned;

  const manualMap = {
    "MONTREAL": {
      name: "Montreal, Quebec, Canada",
      latitude: 45.5019,
      longitude: -73.5674,
      timezone: "America/Toronto"
    },
    "MONTREALCANADA": {
      name: "Montreal, Quebec, Canada",
      latitude: 45.5019,
      longitude: -73.5674,
      timezone: "America/Toronto"
    },
    "MONTREALQUEBEC": {
      name: "Montreal, Quebec, Canada",
      latitude: 45.5019,
      longitude: -73.5674,
      timezone: "America/Toronto"
    },
    "H2V4B7": {
      name: "Montreal, Quebec, Canada",
      latitude: 45.5239,
      longitude: -73.5997,
      timezone: "America/Toronto"
    }
  };

  if (manualMap[postal]) return manualMap[postal];
  if (manualMap[cleaned]) return manualMap[cleaned];

  const collapsedRaw = raw.toUpperCase().replace(/\s+/g, "");
  if (manualMap[collapsedRaw]) return manualMap[collapsedRaw];

  if (/^[A-Z]\d[A-Z]\d[A-Z]\d$/.test(postal)) {
    let results = await searchNominatim({ postalcode: postal });
    if (results.length) return formatNominatimResult(results[0]);

    results = await searchNominatim({ q: `${postal}, Canada` });
    if (results.length) return formatNominatimResult(results[0]);
  }

  if (/^\d{6}$/.test(cleaned)) {
    const candidates = expandPostalDigits(cleaned);

    for (const c of candidates) {
      if (manualMap[c]) return manualMap[c];

      const results = await searchNominatim({ postalcode: c });
      if (results.length) return formatNominatimResult(results[0]);
    }
  }

  let results = await searchNominatim({ q: raw });
  if (results.length) return formatNominatimResult(results[0]);

  results = await searchNominatim({ q: `${raw}, Canada` });
  if (results.length) return formatNominatimResult(results[0]);

  results = await searchNominatim({ city: raw });
  if (results.length) return formatNominatimResult(results[0]);

  return null;
}

async function fetchForecast(location) {
  const response = await axios.get("https://api.open-meteo.com/v1/forecast", {
    params: {
      latitude: location.latitude,
      longitude: location.longitude,
      current: "temperature_2m,apparent_temperature,rain,showers,snowfall,cloud_cover,wind_speed_10m,weather_code",
      hourly: "temperature_2m,precipitation_probability,rain,showers,snowfall,cloud_cover,wind_speed_10m,weather_code",
      daily: "weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max,rain_sum,showers_sum,snowfall_sum,wind_speed_10m_max",
      timezone: "auto",
      forecast_days: 7
    },
    timeout: 15000
  });

  return response.data;
}

app.get("/", (req, res) => {
  res.send("Weather phone server is running.");
});

app.get("/debug-weather", async (req, res) => {
  try {
    const location = {
      name: "Montreal, Quebec, Canada",
      latitude: 45.5019,
      longitude: -73.5674,
      timezone: "America/Toronto"
    };

    const forecast = await fetchForecast(location);
    res.json(forecast);
  } catch (error) {
    console.error("DEBUG weather error:", error.message);
    console.error("DEBUG weather details:", error.response?.data || null);

    res.status(500).json({
      error: error.message,
      details: error.response?.data || null
    });
  }
});

app.get("/voice", (req, res) => {
  const twiml = new VoiceResponse();
  say(twiml, "Weather Line is running. Please call the phone number to use the interactive weather menu.");
  res.type("text/xml").send(twiml.toString());
});

app.post("/voice", (req, res) => {
  const twiml = new VoiceResponse();

  console.log("VOICE CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("VOICE saved location:", getSavedLocation(req));

  say(
    twiml,
    "Welcome to Weather Line. This service is sponsored by Lipa Supermarket."
  );

  const saved = getSavedLocation(req);
  buildMenuInto(twiml, saved ? saved.name : null);

  res.type("text/xml").send(twiml.toString());
});

app.post("/menu", async (req, res) => {
  const choice = parseMenuChoice(req);
  const location = getSavedLocation(req);
  const twiml = new VoiceResponse();

  console.log("MENU CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("MENU choice:", choice);
  console.log("MENU saved location:", location);

  if (choice === "5") {
    return res.type("text/xml").send(locationPromptTwiml().toString());
  }

  if (!location) {
    say(twiml, "You need to set your location first.");
    twiml.redirect({ method: "POST" }, "/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
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
    say(twiml, "Sorry, I could not retrieve the weather right now. Please try again later.");
    twiml.redirect({ method: "POST" }, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/set-location-prompt", (req, res) => {
  res.type("text/xml").send(locationPromptTwiml().toString());
});

app.post("/set-location", async (req, res) => {
  const input = normalizeText(req.body.SpeechResult || req.body.Digits || "");
  const twiml = new VoiceResponse();

  console.log("SET-LOCATION CallSid:", req.body.CallSid, "From:", req.body.From);
  console.log("SET-LOCATION raw input:", input);
  console.log("SpeechResult:", req.body.SpeechResult, "Digits:", req.body.Digits);

  if (!input) {
    say(twiml, "I did not get a location.");
    twiml.redirect({ method: "POST" }, "/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const location = await resolveLocation(input);

    if (!location) {
      say(twiml, `I could not find ${input}. Please try again.`);
      twiml.redirect({ method: "POST" }, "/set-location-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    saveLocation(req, location);

    console.log("SET-LOCATION saved:", location);
    console.log("SET-LOCATION session now:", getSession(req));

    say(twiml, `Location saved as ${location.name}.`);

    // Go directly to the menu, not back through a fresh "start" path
    buildMenuInto(twiml, location.name);

    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    console.error("Location lookup error:", error.message);
    console.error("Location lookup details:", error.response?.data || null);
    say(twiml, "There was a problem finding that location.");
    twiml.redirect({ method: "POST" }, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/forecast-day", async (req, res) => {
  const location = getSavedLocation(req);
  const twiml = new VoiceResponse();

  if (!location) {
    say(twiml, "You need to set your location first.");
    twiml.redirect({ method: "POST" }, "/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
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
    say(twiml, "Sorry, I could not retrieve that forecast.");
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

  if (choice === "1") {
    twiml.redirect({ method: "POST" }, "/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  if (choice === "2") {
    twiml.redirect({ method: "POST" }, "/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
  }

  if (choice === "3") {
    const location = getSavedLocation(req);

    if (!location) {
      say(twiml, "You need to set your location first.");
      twiml.redirect({ method: "POST" }, "/set-location-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    try {
      const forecast = await fetchForecast(location);
      say(twiml, currentWeatherSpeech(location, forecast));
      twiml.redirect({ method: "POST" }, "/after-prompt");
      return res.type("text/xml").send(twiml.toString());
    } catch (error) {
      console.error("AFTER repeat current error:", error.message);
      console.error("AFTER repeat current details:", error.response?.data || null);
      say(twiml, "Sorry, I could not retrieve the weather.");
      twiml.redirect({ method: "POST" }, "/voice");
      return res.type("text/xml").send(twiml.toString());
    }
  }

  say(twiml, "Goodbye.");
  twiml.hangup();
  return res.type("text/xml").send(twiml.toString());
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Weather phone server running on port ${port}`);
});