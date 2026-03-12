const express = require("express");
const axios = require("axios");
const twilio = require("twilio");

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

const VoiceResponse = twilio.twiml.VoiceResponse;

// In-memory caller location store.
// Good for testing; resets when the app restarts/redeploys.
const callerLocations = new Map();

function getCallerKey(req) {
  return req.body.From || "unknown";
}

function getSavedLocation(req) {
  return callerLocations.get(getCallerKey(req)) || null;
}

function saveLocation(req, location) {
  callerLocations.set(getCallerKey(req), location);
}

function weatherCodeToText(code) {
  const map = {
    0: "clear sky",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "freezing fog",
    51: "light drizzle",
    53: "moderate drizzle",
    55: "dense drizzle",
    56: "light freezing drizzle",
    57: "dense freezing drizzle",
    61: "slight rain",
    63: "moderate rain",
    65: "heavy rain",
    66: "light freezing rain",
    67: "heavy freezing rain",
    71: "slight snow",
    73: "moderate snow",
    75: "heavy snow",
    77: "snow grains",
    80: "slight rain showers",
    81: "moderate rain showers",
    82: "violent rain showers",
    85: "slight snow showers",
    86: "heavy snow showers",
    95: "thunderstorm",
    96: "thunderstorm with hail",
    99: "severe thunderstorm with hail"
  };
  return map[code] || "mixed weather";
}

function dayLabel(index) {
  if (index === 0) return "today";
  if (index === 1) return "tomorrow";
  return `day ${index + 1}`;
}

function hourLabel(iso) {
  const d = new Date(iso);
  return d.toLocaleTimeString("en-US", {
    hour: "numeric",
    hour12: true
  });
}

async function geocodeLocation(query) {
  const url = "https://geocoding-api.open-meteo.com/v1/search";
  const response = await axios.get(url, {
    params: {
      name: query,
      count: 1,
      language: "en",
      format: "json"
    },
    timeout: 10000
  });

  const results = response.data.results;
  if (!results || !results.length) return null;

  const r = results[0];
  return {
    name: `${r.name}${r.admin1 ? ", " + r.admin1 : ""}${r.country ? ", " + r.country : ""}`,
    latitude: r.latitude,
    longitude: r.longitude,
    timezone: r.timezone || "auto"
  };
}

async function fetchForecast(location) {
  const url = "https://api.open-meteo.com/v1/forecast";
  const response = await axios.get(url, {
    params: {
      latitude: location.latitude,
      longitude: location.longitude,
      timezone: location.timezone || "auto",
      current: [
        "temperature_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "showers",
        "snowfall",
        "cloud_cover",
        "wind_speed_10m",
        "weather_code",
        "is_day"
      ].join(","),
      hourly: [
        "temperature_2m",
        "precipitation_probability",
        "precipitation",
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
        "precipitation_sum",
        "rain_sum",
        "showers_sum",
        "snowfall_sum",
        "precipitation_probability_max",
        "wind_speed_10m_max",
        "sunrise",
        "sunset"
      ].join(","),
      forecast_days: 7
    },
    timeout: 10000
  });

  return response.data;
}

function currentWeatherSpeech(location, forecast) {
  const c = forecast.current;

  const sky = weatherCodeToText(c.weather_code);
  const rain = c.rain || 0;
  const snow = c.snowfall || 0;
  const precip = c.precipitation || 0;
  const cloud = c.cloud_cover || 0;

  return [
    `Current weather for ${location.name}.`,
    `It is ${sky}.`,
    `Temperature is ${c.temperature_2m} degrees Celsius.`,
    `Feels like ${c.apparent_temperature} degrees.`,
    `Wind speed is ${c.wind_speed_10m} kilometers per hour.`,
    `Cloud cover is ${cloud} percent.`,
    `Total precipitation right now is ${precip} millimeters.`,
    `Rain is ${rain} millimeters.`,
    `Snowfall is ${snow} centimeters.`
  ].join(" ");
}

function nextHoursSpeech(location, forecast, hours = 6) {
  const h = forecast.hourly;
  const count = Math.min(hours, h.time.length);

  let speech = `Next ${count} hours for ${location.name}. `;

  for (let i = 0; i < count; i++) {
    speech += [
      `At ${hourLabel(h.time[i])},`,
      `${weatherCodeToText(h.weather_code[i])},`,
      `temperature ${h.temperature_2m[i]} degrees,`,
      `rain chance ${h.precipitation_probability[i] || 0} percent,`,
      `rain ${h.rain[i] || 0} millimeters,`,
      `snow ${h.snowfall[i] || 0} centimeters,`,
      `wind ${h.wind_speed_10m[i] || 0} kilometers per hour,`,
      `cloud cover ${h.cloud_cover[i] || 0} percent.`
    ].join(" ");
  }

  return speech;
}

function dayForecastSpeech(location, forecast, index) {
  const d = forecast.daily;

  if (index < 0 || index >= d.time.length) {
    return `That forecast day is not available for ${location.name}.`;
  }

  const label = dayLabel(index);
  const weather = weatherCodeToText(d.weather_code[index]);
  const high = d.temperature_2m_max[index];
  const low = d.temperature_2m_min[index];
  const rainChance = d.precipitation_probability_max[index] || 0;
  const rain = d.rain_sum[index] || 0;
  const showers = d.showers_sum[index] || 0;
  const snow = d.snowfall_sum[index] || 0;
  const wind = d.wind_speed_10m_max[index] || 0;
  const precip = d.precipitation_sum[index] || 0;

  return [
    `Forecast for ${label} in ${location.name}.`,
    `Conditions: ${weather}.`,
    `High ${high} degrees Celsius.`,
    `Low ${low} degrees.`,
    `Maximum rain chance ${rainChance} percent.`,
    `Total precipitation ${precip} millimeters.`,
    `Rain ${rain} millimeters.`,
    `Showers ${showers} millimeters.`,
    `Snowfall ${snow} centimeters.`,
    `Maximum wind speed ${wind} kilometers per hour.`
  ].join(" ");
}

function alertsSpeech(location, forecast) {
  const d = forecast.daily;
  const alerts = [];

  for (let i = 0; i < Math.min(3, d.time.length); i++) {
    const label = dayLabel(i);

    if ((d.wind_speed_10m_max[i] || 0) >= 50) {
      alerts.push(`Strong wind possible ${label}.`);
    }
    if ((d.snowfall_sum[i] || 0) >= 5) {
      alerts.push(`Snow accumulation possible ${label}.`);
    }
    if ((d.precipitation_probability_max[i] || 0) >= 70 && (d.rain_sum[i] || 0) >= 10) {
      alerts.push(`Heavy rain risk ${label}.`);
    }
    if ((d.temperature_2m_min[i] || 0) <= -15) {
      alerts.push(`Dangerous cold possible ${label}.`);
    }
    if ((d.temperature_2m_max[i] || 0) >= 30) {
      alerts.push(`Heat risk possible ${label}.`);
    }
    if ([95, 96, 99].includes(d.weather_code[i])) {
      alerts.push(`Thunderstorm risk ${label}.`);
    }
  }

  if (!alerts.length) {
    return `No major forecast warnings were found for ${location.name} in the next few days.`;
  }

  return `Important forecast alerts for ${location.name}. ${alerts.join(" ")}`;
}

function mainMenuTwiml(locationName) {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    numDigits: 1,
    action: "/menu",
    method: "POST",
    timeout: 8
  });

  if (locationName) {
    gather.say(
      { voice: "alice" },
      `Welcome to Weather Line. Your saved location is ${locationName}. ` +
        `Press 1 for current weather. ` +
        `Press 2 for the next 6 hours. ` +
        `Press 3 for a forecast day. ` +
        `Press 4 for important alerts. ` +
        `Press 5 to change location.`
    );
  } else {
    gather.say(
      { voice: "alice" },
      `Welcome to Weather Line. No location is saved yet. ` +
        `Press 5 now to set your location.`
    );
  }

  twiml.redirect("/voice");
  return twiml;
}

function locationPromptTwiml() {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    input: "speech dtmf",
    action: "/set-location",
    method: "POST",
    timeout: 6,
    speechTimeout: "auto",
    numDigits: 10
  });

  gather.say(
    { voice: "alice" },
    "Please say your city and country, or enter a zip code followed by the pound key."
  );

  twiml.redirect("/voice");
  return twiml;
}

function forecastDayPromptTwiml() {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    numDigits: 1,
    action: "/forecast-day",
    method: "POST",
    timeout: 8
  });

  gather.say(
    { voice: "alice" },
    "Choose a forecast day. Press 1 for today. " +
      "Press 2 for tomorrow. " +
      "Press 3 for day 3. " +
      "Press 4 for day 4. " +
      "Press 5 for day 5. " +
      "Press 6 for day 6. " +
      "Press 7 for day 7."
  );

  twiml.redirect("/voice");
  return twiml;
}

function afterActionTwiml() {
  const twiml = new VoiceResponse();

  const gather = twiml.gather({
    numDigits: 1,
    action: "/after-action",
    method: "POST",
    timeout: 8
  });

  gather.say(
    { voice: "alice" },
    "Press 1 for the main menu. Press 2 to change location. Press 3 to hear current weather again."
  );

  twiml.hangup();
  return twiml;
}

app.get("/", (req, res) => {
  res.send("Weather phone server is running.");
});

app.post("/voice", (req, res) => {
  const saved = getSavedLocation(req);
  const twiml = mainMenuTwiml(saved ? saved.name : null);
  res.type("text/xml").send(twiml.toString());
});

app.post("/menu", async (req, res) => {
  const digit = req.body.Digits;
  const location = getSavedLocation(req);
  const twiml = new VoiceResponse();

  if (digit === "5") {
    return res.type("text/xml").send(locationPromptTwiml().toString());
  }

  if (!location) {
    twiml.say({ voice: "alice" }, "You need to set your location first.");
    twiml.redirect("/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const forecast = await fetchForecast(location);

    if (digit === "1") {
      twiml.say({ voice: "alice" }, currentWeatherSpeech(location, forecast));
      twiml.redirect("/after-action-prompt");
    } else if (digit === "2") {
      twiml.say({ voice: "alice" }, nextHoursSpeech(location, forecast, 6));
      twiml.redirect("/after-action-prompt");
    } else if (digit === "3") {
      return res.type("text/xml").send(forecastDayPromptTwiml().toString());
    } else if (digit === "4") {
      twiml.say({ voice: "alice" }, alertsSpeech(location, forecast));
      twiml.redirect("/after-action-prompt");
    } else {
      twiml.say({ voice: "alice" }, "Invalid choice.");
      twiml.redirect("/voice");
    }

    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    twiml.say({ voice: "alice" }, "Sorry. I could not retrieve the weather right now.");
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/set-location-prompt", (req, res) => {
  res.type("text/xml").send(locationPromptTwiml().toString());
});

app.post("/set-location", async (req, res) => {
  const input = (req.body.SpeechResult || req.body.Digits || "").trim();
  const twiml = new VoiceResponse();

  if (!input) {
    twiml.say({ voice: "alice" }, "I did not get a location.");
    twiml.redirect("/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const location = await geocodeLocation(input);

    if (!location) {
      twiml.say({ voice: "alice" }, `I could not find ${input}. Please try again.`);
      twiml.redirect("/set-location-prompt");
      return res.type("text/xml").send(twiml.toString());
    }

    saveLocation(req, location);
    twiml.say({ voice: "alice" }, `Location saved as ${location.name}.`);
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    twiml.say({ voice: "alice" }, "There was a problem saving your location.");
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/forecast-day", async (req, res) => {
  const location = getSavedLocation(req);
  const digit = parseInt(req.body.Digits || "", 10);
  const twiml = new VoiceResponse();

  if (!location) {
    twiml.say({ voice: "alice" }, "You need to set your location first.");
    twiml.redirect("/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
  }

  if (!digit || digit < 1 || digit > 7) {
    twiml.say({ voice: "alice" }, "That forecast day is not valid.");
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const forecast = await fetchForecast(location);
    twiml.say({ voice: "alice" }, dayForecastSpeech(location, forecast, digit - 1));
    twiml.redirect("/after-action-prompt");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    twiml.say({ voice: "alice" }, "Sorry. I could not retrieve that forecast.");
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  }
});

app.post("/after-action-prompt", (req, res) => {
  res.type("text/xml").send(afterActionTwiml().toString());
});

app.post("/after-action", (req, res) => {
  const digit = req.body.Digits;
  const twiml = new VoiceResponse();

  if (digit === "1") {
    twiml.redirect("/voice");
  } else if (digit === "2") {
    twiml.redirect("/set-location-prompt");
  } else if (digit === "3") {
    twiml.redirect("/menu-repeat-current");
  } else {
    twiml.say({ voice: "alice" }, "Goodbye.");
    twiml.hangup();
  }

  res.type("text/xml").send(twiml.toString());
});

app.post("/menu-repeat-current", async (req, res) => {
  const location = getSavedLocation(req);
  const twiml = new VoiceResponse();

  if (!location) {
    twiml.say({ voice: "alice" }, "You need to set your location first.");
    twiml.redirect("/set-location-prompt");
    return res.type("text/xml").send(twiml.toString());
  }

  try {
    const forecast = await fetchForecast(location);
    twiml.say({ voice: "alice" }, currentWeatherSpeech(location, forecast));
    twiml.redirect("/after-action-prompt");
    return res.type("text/xml").send(twiml.toString());
  } catch (error) {
    twiml.say({ voice: "alice" }, "Sorry. I could not retrieve the weather.");
    twiml.redirect("/voice");
    return res.type("text/xml").send(twiml.toString());
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`Weather phone server running on port ${port}`);
});