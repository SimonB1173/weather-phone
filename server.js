const express = require("express");
const axios = require("axios");
const twilio = require("twilio");

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

const VoiceResponse = twilio.twiml.VoiceResponse;

// Better: set account-wide TTS in Twilio Console to Standard/Neural/Generative.
// Optional env vars if you want to force a voice/language in code.
const SAY_OPTIONS = {
  language: process.env.TWILIO_TTS_LANG || "en-CA"
};
if (process.env.TWILIO_TTS_VOICE) {
  SAY_OPTIONS.voice = process.env.TWILIO_TTS_VOICE;
}

// In-memory location store by caller number.
// Good for testing. If Render restarts, locations reset.
const callerLocations = new Map();

function say(twiml, text) {
  twiml.say(SAY_OPTIONS, text);
}

function callerKey(req) {
  return req.body.From || "unknown";
}

function getSavedLocation(req) {
  return callerLocations.get(callerKey(req)) || null;
}

function saveLocation(req, loc) {
  callerLocations.set(callerKey(req), loc);
}

function normalizeText(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
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

function cleanPostalCode(input) {
  return String(input || "").toUpperCase().replace(/[^A-Z0-9]/g, "");
}

// Standard phone keypad expansion for Canadian postal code letters.
const KEYPAD = {
  "2": ["A", "B", "C"],
  "3": ["D", "E", "F"],
  "4": ["G", "H", "I"],
  "5": ["J", "K", "L"],
  "6": ["M", "N", "O"],
  "7": ["P", "Q", "R", "S"],
  "8": ["T", "U", "V"],
  "9": ["W", "X", "Y", "Z"]
};

function expandCanadianPostalDigits(digits) {
  const d = String(digits || "").replace(/\D/g, "");
  if (d.length !== 6) return [];

  // Canadian pattern A1A1A1
  const a1 = KEYPAD[d[0]] || [];
  const a2 = KEYPAD[d[2]] || [];
  const a3 = KEYPAD[d[4]] || [];

  const out = [];
  for (const x of a1) {
    for (const y of a2) {
      for (const z of a3) {
        out.push(`${x}${d[1]}${y}${d[3]}${z}${d[5]}`);
      }
    }
  }
  return out;
}

async function geocodeOpenMeteo(query) {
  const response = await axios.get("https://geocoding-api.open-meteo.com/v1/search", {
    params: {
      name: query,
      count: 5,
      language: "en",
      format: "json"
    },
    timeout: 10000
  });

  const results = response.data.results || [];
  if (!results.length) return null;

  const best = results[0];
  return {
    name: `${best.name}${best.admin1 ? ", " + best.admin1 : ""}${best.country ? ", " + best.country : ""}`,
    latitude: best.latitude,
    longitude: best.longitude,
    timezone: best.timezone || "auto"
  };
}

async function resolveLocation(input) {
  const raw = normalizeText(input);
  if (!raw) return null;

  // 1) Try exact user input first
  let loc = await geocodeOpenMeteo(raw);
  if (loc) return loc;

  // 2) Try cleaned postal code
  const postal = cleanPostalCode(raw);
  if (/^[A-Z]\d[A-Z]\d[A-Z]\d$/.test(postal)) {
    loc = await geocodeOpenMeteo(`${postal}, Canada`);
    if (loc) return loc;
  }

  // 3) Try 6-digit keypad-style postal code expansion
  // Example standard keypad H2V3G7 -> 428347
  if (/^\d{6}$/.test(postal)) {
    const candidates = expandCanadianPostalDigits(postal);
    for (const c of candidates) {
      loc = await geocodeOpenMeteo(`${c}, Canada`);
      if (loc) return loc;
    }
  }

  return null;
}

async function fetchForecast(loc) {
  const response = await axios.get("https://api.open-meteo.com/v1/forecast", {
    params: {
      latitude: loc.latitude,
      longitude: loc.longitude,
      timezone: loc.timezone || "auto",
      forecast_days: 7,
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
      ].join(",")
    },
    timeout: 12000
  });

  return response.data;
}

function pushIf(parts, condition, text) {
  if (condition) parts.push(text);
}

function currentWeatherSpeech(loc, f) {
  const c = f.current;
  const parts = [
    `Current forecast for ${loc.name}.`,
    `It is ${weatherCodeToText(c.weather_code)}.`,
    `Temperature ${Math.round(c.temperature_2m)} degrees.`,
    `Feels like ${Math.round(c.apparent_temperature)} degrees.`
  ];

  pushIf(parts, (c.wind_speed_10m || 0) > 0, `Wind ${Math.round(c.wind_speed_10m)} kilometres per hour.`);
  pushIf(parts, (c.cloud_cover || 0) > 0, `Cloud cover ${Math.round(c.cloud_cover)} percent.`);
  pushIf(parts, (c.rain || 0) > 0, `Rain ${c.rain} millimetres.`);
  pushIf(parts, (c.showers || 0) > 0, `Showers ${c.showers} millimetres.`);
  pushIf(parts, (c.snowfall || 0) > 0, `Snowfall ${c.snowfall} centimetres.`);

  return parts.join(" ");
}

function nextHoursSpeech(loc, f, count = 6) {
  const now = Date.now();
  const tz = loc.timezone || "UTC";
  const h = f.hourly;
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
    if (items.length >= count) break;
  }

  if (!items.length) {
    return `I could not find future hourly forecast data for ${loc.name}.`;
  }

  let out = `Next ${items.length} forecast hours for ${loc.name}. `;
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

    out += parts.join(" ") + " ";
  }

  return out.trim();
}

function dailyForecastSpeech(loc, f, idx) {
  const d = f.daily;
  if (idx < 0 || idx >= d.time.length) {
    return `That forecast day is not available for ${loc.name}.`;
  }

  const day = idx === 0 ? "today" : idx === 1 ? "tomorrow" : dayName(d.time[idx], loc.timezone);
  const parts = [
    `Forecast for ${day} in ${loc.name}.`,
    `Conditions ${weatherCodeToText(d.weather_code[idx])}.`,
    `High ${Math.round(d.temperature_2m_max[idx])} degrees.`,
    `Low ${Math.round(d.temperature_2m_min[idx])} degrees.`
  ];

  pushIf(parts, (d.precipitation_probability_max[idx] || 0) > 0,
    `Maximum precipitation chance ${Math.round(d.precipitation_probability_max[idx])} percent.`);
  pushIf(parts, (d.wind_speed_10m_max[idx] || 0) > 0,
    `Maximum wind ${Math.round(d.wind_speed_10m_max[idx])} kilometres per hour.`);
  pushIf(parts, (d.rain_sum[idx] || 0) > 0, `Rain ${d.rain_sum[idx]} millimetres.`);
  pushIf(parts, (d.showers_sum[idx] || 0) > 0, `Showers ${d.showers_sum[idx]} millimetres.`);
  pushIf(parts, (d.snowfall_sum[idx] || 0) > 0, `Snowfall ${d.snowfall_sum[idx]} centimetres.`);

  return parts.join(" ");
}

function alertsSpeech(loc, f) {
  const d = f.daily;
  const alerts = [];

  for (let i = 0; i < Math.min(7, d.time.length); i++) {
    const label = i === 0 ? "today" : i === 1 ? "tomorrow" : dayName(d.time[i], loc.timezone);

    if ((d.wind_speed_10m_max[i] || 0) >= 50) alerts.push(`Strong wind possible ${label}.`);
    if ((d.snowfall_sum[i] || 0) >= 5) alerts.push(`Significant snow possible ${label}.`);
    if ((d.rain_sum[i] || 0) >= 10) alerts.push(`Heavy rain possible ${label}.`);
    if ([95, 96, 99].includes(d.weather_code[i])) alerts.push(`Thunderstorm risk ${label}.`);
  }

  return alerts.length
    ? `Important forecast alerts for ${loc.name}. ${alerts.join(" ")}`
    : `No major forecast alerts found for ${loc.name} in the next seven days.`;
}

function parseMenuChoice(req) {
  const d = String(req.body.Digits || "").trim();
  const s = String(req.body.SpeechResult || "").toLowerCase();

  if (d) return d;
  if (s.includes("current") || s.includes("now")) return "1";
  if (s.includes("hour")) return "2";
  if (s.includes("day") || s.includes("today") || s.includes("tomorrow") || s.includes("forecast")) return "3";
  if (s.includes("alert") || s.includes("warning")) return "4";
  if (s.includes("location") || s.includes("change")) return "5";

  return "";
}

function parseDayChoice(req, forecast) {
  const d = String(req.body.Digits || "").trim();
  const s = String(req.body.SpeechResult || "").toLowerCase();

  if (/^[1-7]$/.test(d)) return parseInt(d, 10) - 1;

  if (s.includes("today")) return 0;
  if (s.includes("tomorrow")) return 1;

  for (let i = 0; i < forecast.daily.time.length; i++) {
    const name = dayName(forecast.daily.time[i], forecast.timezone).toLowerCase();
    if (s.includes(name)) return i;
  }

  const m = s.match(/\b([1-7])\b/);
  if (m) return parseInt(m[1], 10) - 1;

  return -1;
}

function mainMenu(loc) {
  const twiml = new VoiceResponse();
  const gather = twiml.gather({
    input: "speech dtmf",
    action: "/menu",
    method: "POST",
    timeout: 6,
    speechTimeout: "auto",
    numDigits: 1
  });

  if (loc) {
    say(
      gather,
      `Welcome to Weather Line. Saved location: ${loc.name}. ` +
      `Press or say 1 for current weather. ` +
      `2 for the next six hours. ` +
      `3 for any of the next seven forecast days. ` +
      `4 for important alerts. ` +
      `5 to change location.`
    );
  } else {
    say(
      gather,
      `Welcome to Weather Line. No location is saved yet. ` +
      `Press or say 5 to set your location.`
    );
  }

  twiml.redirect("/voice");
  return twiml;
}

function locationPrompt() {
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
    `Say a city, town, or postal code. ` +
    `You can also type a six-character Canadian postal code using the phone keypad.`
  );

  twiml.redirect("/voice");
  return twiml;
}

function dayPrompt(forecast, loc) {
  const twiml = new VoiceResponse();
  const gather = twiml.gather({
    input: "speech dtmf",
    action: "/day-choice",
    method: "POST",
    timeout: 7,
    speechTimeout: "auto",
    numDigits: 1
  });

  const names = forecast.daily.time
    .slice(0, 7)
    .map((t, i) => `${i + 1} for ${i === 0 ? "today" : i === 1 ? "tomorrow" : dayName(t, loc.timezone)}`)
    .join(". ");

  say(gather, `Choose a forecast day for ${loc.name}. Press or say ${names}.`);
  twiml.redirect("/voice");
  return twiml;
}

function afterPrompt() {
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
    `3 to repeat current weather.`
  );

  twiml.hangup();
  return twiml;
}

function parseAfterChoice(req) {
  const d = String(req.body.Digits || "").trim();
  const s = String(req.body.SpeechResult || "").toLowerCase();
  if (d) return d;
  if (s.includes("menu")) return "1";
  if (s.includes("change") || s.includes("location")) return "2";
  if (s.includes("repeat") || s.includes("again") || s.includes("current")) return "3";
  return "";
}

app.get("/", (req, res) => {
  res.send("Weather phone server is running.");
});

// Helpful for browser testing too:
app.get("/voice", (req, res) => {
  const twiml = new VoiceResponse();
  say(twiml, "Weather Line is running. Please call the phone number to use the interactive forecast.");
  res.type("text/xml").send(twiml.toString());
});

app.post("/voice", (req, res) => {
  const loc = getSavedLocation(req);
  res.type("text/xml").send(mainMenu(loc).toString());
});

app.post("/menu", async (req, res) => {
  const choice = parseMenuChoice(req);
  const loc = getSavedLocation(req);
  const twiml = new VoiceResponse();

  if (choice === "5") {
    return res.type("text/xml").send(locationPrompt().toString());
  }

  if (!loc) {
    say(twiml, "You need to set your location first.");
    twiml.redirect("/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const forecast = await fetchForecast(loc);

    if (choice === "1") {
      say(twiml, currentWeatherSpeech(loc, forecast));
      twiml.redirect("/after-prompt");
    } else if (choice === "2") {
      say(twiml, nextHoursSpeech(loc, forecast, 6));
      twiml.redirect("/after-prompt");
    } else if (choice === "3") {
      return res.type("text/xml").send(dayPrompt(forecast, loc).toString());
    } else if (choice === "4") {
      say(twiml, alertsSpeech(loc, forecast));
      twiml.redirect("/after-prompt");
    } else {
      say(twiml, "I did not understand that choice.");
      twiml.redirect("/voice");
    }

    res.type("text/xml").send(twiml.toString());
  } catch (e) {
    say(twiml, "Sorry, I could not retrieve the forecast right now.");
    twiml.redirect("/voice");
    res.type("text/xml").send(twiml.toString());
  }
});

app.post("/set-location-prompt", (req, res) => {
  res.type("text/xml").send(locationPrompt().toString());
});

app.post("/set-location", async (req, res) => {
  const input = normalizeText(req.body.SpeechResult || req.body.Digits || "");
  const twiml = new VoiceResponse();

  if (!input) {
    say(twiml, "I did not get a location.");
    twiml.redirect("/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const loc = await resolveLocation(input);
    if (!loc) {
      say(twiml, `I could not find ${input}. Please try again.`);
      twiml.redirect("/set-location-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    saveLocation(req, loc);
    say(twiml, `Location saved as ${loc.name}.`);
    twiml.redirect("/voice");
    res.type("text/xml").send(twiml.toString());
  } catch (e) {
    say(twiml, "There was a problem finding that location.");
    twiml.redirect("/voice");
    res.type("text/xml").send(twiml.toString());
  }
});

app.post("/day-choice", async (req, res) => {
  const loc = getSavedLocation(req);
  const twiml = new VoiceResponse();

  if (!loc) {
    say(twiml, "You need to set your location first.");
    twiml.redirect("/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const forecast = await fetchForecast(loc);
    const idx = parseDayChoice(req, forecast);

    if (idx < 0 || idx > 6) {
      say(twiml, "I did not understand the forecast day.");
      return res.type("text/xml").send(dayPrompt(forecast, loc).toString());
    }

    say(twiml, dailyForecastSpeech(loc, forecast, idx));
    twiml.redirect("/after-prompt");
    res.type("text/xml").send(twiml.toString());
  } catch (e) {
    say(twiml, "Sorry, I could not retrieve that forecast.");
    twiml.redirect("/voice");
    res.type("text/xml").send(twiml.toString());
  }
});

app.post("/after-prompt", (req, res) => {
  res.type("text/xml").send(afterPrompt().toString());
});

app.post("/after", async (req, res) => {
  const choice = parseAfterChoice(req);
  const twiml = new VoiceResponse();

  if (choice === "1") {
    twiml.redirect("/voice");
  } else if (choice === "2") {
    twiml.redirect("/set-location-prompt");
  } else if (choice === "3") {
    const loc = getSavedLocation(req);
    if (!loc) {
      say(twiml, "You need to set your location first.");
      twiml.redirect("/set-location-prompt");
    } else {
      try {
        const forecast = await fetchForecast(loc);
        say(twiml, currentWeatherSpeech(loc, forecast));
        twiml.redirect("/after-prompt");
      } catch (e) {
        say(twiml, "Sorry, I could not retrieve the weather.");
        twiml.redirect("/voice");
      }
    }
  } else {
    say(twiml, "Goodbye.");
    twiml.hangup();
  }

  res.type("text/xml").send(twiml.toString());
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Weather phone server running on port ${port}`);
});