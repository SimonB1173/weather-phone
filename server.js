const express = require("express");
const axios = require("axios");
const twilio = require("twilio");

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

const VoiceResponse = twilio.twiml.VoiceResponse;

// In-memory caller preferences for demo purposes.
// For production, replace with a real database.
const callerPrefs = new Map();

// ---------- Helpers ----------

async function geocodeLocation(query) {
  const url = "https://geocoding-api.open-meteo.com/v1/search";
  const { data } = await axios.get(url, {
    params: {
      name: query,
      count: 1,
      language: "en",
      format: "json",
    },
    timeout: 10000,
  });

  if (!data.results || !data.results.length) return null;

  const r = data.results[0];
  return {
    name: `${r.name}${r.admin1 ? ", " + r.admin1 : ""}${r.country ? ", " + r.country : ""}`,
    latitude: r.latitude,
    longitude: r.longitude,
    timezone: r.timezone || "auto",
  };
}

async function getForecast(lat, lon, timezone = "auto") {
  const url = "https://api.open-meteo.com/v1/forecast";
  const { data } = await axios.get(url, {
    params: {
      latitude: lat,
      longitude: lon,
      timezone,
      current: [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "wind_speed_10m",
        "wind_direction_10m",
        "weather_code",
      ].join(","),
      hourly: [
        "temperature_2m",
        "precipitation_probability",
        "wind_speed_10m",
      ].join(","),
      daily: [
        "weather_code",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_probability_max",
        "wind_speed_10m_max",
      ].join(","),
      forecast_days: 3,
    },
    timeout: 10000,
  });

  return data;
}

function safeSay(text) {
  return String(text)
    .replace(/&/g, "and")
    .replace(/\s+/g, " ")
    .trim();
}

function buildCurrentWeatherSpeech(locationName, f) {
  const c = f.current || {};
  const temp = c.temperature_2m;
  const feels = c.apparent_temperature;
  const wind = c.wind_speed_10m;
  const humidity = c.relative_humidity_2m;

  return safeSay(
    `Current weather for ${locationName}. ` +
    `Temperature ${temp} degrees. ` +
    `Feels like ${feels}. ` +
    `Humidity ${humidity} percent. ` +
    `Wind speed ${wind} kilometers per hour.`
  );
}

function buildHourlySpeech(locationName, f) {
  const h = f.hourly || {};
  const times = h.time || [];
  const temps = h.temperature_2m || [];
  const rain = h.precipitation_probability || [];
  const wind = h.wind_speed_10m || [];

  let speech = `Hourly forecast for ${locationName}. `;
  const count = Math.min(6, times.length);

  for (let i = 0; i < count; i++) {
    const t = new Date(times[i]);
    const hour = t.getHours();
    speech += `At ${hour} hundred, temperature ${temps[i]} degrees, rain chance ${rain[i] ?? 0} percent, wind ${wind[i]} kilometers per hour. `;
  }
  return safeSay(speech);
}

function buildDailySpeech(locationName, f) {
  const d = f.daily || {};
  const times = d.time || [];
  const maxes = d.temperature_2m_max || [];
  const mins = d.temperature_2m_min || [];
  const rainMax = d.precipitation_probability_max || [];
  const windMax = d.wind_speed_10m_max || [];

  let speech = `Three day forecast for ${locationName}. `;
  const count = Math.min(3, times.length);

  for (let i = 0; i < count; i++) {
    const dt = new Date(times[i]);
    const weekday = dt.toLocaleDateString("en-US", { weekday: "long" });
    speech += `${weekday}, high ${maxes[i]} degrees, low ${mins[i]} degrees, rain chance up to ${rainMax[i] ?? 0} percent, wind up to ${windMax[i]} kilometers per hour. `;
  }
  return safeSay(speech);
}

function getCallerKey(req) {
  return req.body.From || req.ip || "unknown";
}

function getSavedLocation(req) {
  const key = getCallerKey(req);
  return callerPrefs.get(key) || null;
}

function saveLocation(req, locationObj) {
  const key = getCallerKey(req);
  callerPrefs.set(key, locationObj);
}

// ---------- Routes ----------

app.post("/voice", async (req, res) => {
  const twiml = new VoiceResponse();

  // Optional ad intro
  twiml.say(
    { voice: "alice" },
    "This forecast is sponsored by your local business. For sponsor information, stay on the line after the forecast."
  );

  const saved = getSavedLocation(req);

  if (saved) {
    twiml.say(
      { voice: "alice" },
      `Your saved location is ${saved.name}.`
    );
  } else {
    twiml.say(
      { voice: "alice" },
      "No location is saved yet."
    );
  }

  const gather = twiml.gather({
    numDigits: 1,
    action: "/menu",
    method: "POST",
    timeout: 6,
  });

  gather.say(
    { voice: "alice" },
    "Press 1 for current weather. Press 2 for hourly forecast. Press 3 for 3 day forecast. Press 4 to change location. Press 5 for weather radio mode."
  );

  twiml.redirect("/voice");

  res.type("text/xml").send(twiml.toString());
});

app.post("/menu", async (req, res) => {
  const digit = req.body.Digits;
  const saved = getSavedLocation(req);
  const twiml = new VoiceResponse();

  if (digit === "4") {
    const gather = twiml.gather({
      input: "speech dtmf",
      action: "/set-location",
      method: "POST",
      timeout: 5,
      speechTimeout: "auto",
      numDigits: 10,
    });
    gather.say(
      { voice: "alice" },
      "Say your city and country, or enter your zip or postal code followed by the pound key."
    );
    return res.type("text/xml").send(twiml.toString());
  }

  if (!saved) {
    twiml.say({ voice: "alice" }, "You need to set a location first.");
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const forecast = await getForecast(saved.latitude, saved.longitude, saved.timezone);

    if (digit === "1") {
      twiml.say({ voice: "alice" }, buildCurrentWeatherSpeech(saved.name, forecast));
    } else if (digit === "2") {
      twiml.say({ voice: "alice" }, buildHourlySpeech(saved.name, forecast));
    } else if (digit === "3") {
      twiml.say({ voice: "alice" }, buildDailySpeech(saved.name, forecast));
    } else if (digit === "5") {
      twiml.say({ voice: "alice" }, buildCurrentWeatherSpeech(saved.name, forecast));
      twiml.pause({ length: 1 });
      twiml.say({ voice: "alice" }, buildHourlySpeech(saved.name, forecast));
      twiml.pause({ length: 1 });
      twiml.say({ voice: "alice" }, buildDailySpeech(saved.name, forecast));
    } else {
      twiml.say({ voice: "alice" }, "Invalid choice.");
    }

    const gather = twiml.gather({
      numDigits: 1,
      action: "/post-forecast",
      method: "POST",
      timeout: 6,
    });

    gather.say(
      { voice: "alice" },
      "Press 1 to hear the menu again. Press 2 to change location. Press 3 to repeat this forecast."
    );

    twiml.hangup();
    return res.type("text/xml").send(twiml.toString());
  } catch (err) {
    twiml.say({ voice: "alice" }, "Sorry, weather data is temporarily unavailable.");
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/set-location", async (req, res) => {
  const speech = req.body.SpeechResult;
  const digits = req.body.Digits;
  const input = (speech || digits || "").trim();
  const twiml = new VoiceResponse();

  if (!input) {
    twiml.say({ voice: "alice" }, "I did not get the location.");
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const loc = await geocodeLocation(input);

    if (!loc) {
      twiml.say({ voice: "alice" }, `I could not find ${input}. Please try again.`);
      twiml.redirect("/voice");
      return res.type("text/xml").send(twiml.toString());
    }

    saveLocation(req, loc);

    twiml.say({ voice: "alice" }, `Location saved as ${loc.name}.`);
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  } catch (err) {
    twiml.say({ voice: "alice" }, "There was a problem saving your location.");
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/post-forecast", (req, res) => {
  const digit = req.body.Digits;
  const twiml = new VoiceResponse();

  if (digit === "1") {
    twiml.redirect("/voice");
  } else if (digit === "2") {
    const gather = twiml.gather({
      input: "speech dtmf",
      action: "/set-location",
      method: "POST",
      timeout: 5,
      speechTimeout: "auto",
      numDigits: 10,
    });
    gather.say(
      { voice: "alice" },
      "Say your city and country, or enter your zip or postal code followed by the pound key."
    );
  } else if (digit === "3") {
    twiml.redirect("/voice");
  } else {
    twiml.say({ voice: "alice" }, "Goodbye.");
    twiml.hangup();
  }

  res.type("text/xml").send(twiml.toString());
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Weather phone line running on port ${PORT}`);
});