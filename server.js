const express = require("express");
const axios = require("axios");
const twilio = require("twilio");

const app = express();
app.use(express.urlencoded({ extended: false }));

const VoiceResponse = twilio.twiml.VoiceResponse;

app.post("/voice", async (req, res) => {
  try {
    const latitude = 45.5017;
    const longitude = -73.5673;

    const weather = await axios.get(
      `https://api.open-meteo.com/v1/forecast?latitude=${latitude}&longitude=${longitude}&current_weather=true`
    );

    const temp = weather.data.current_weather.temperature;
    const wind = weather.data.current_weather.windspeed;

    const response = new VoiceResponse();

    response.say(
      `Hello. The current temperature in Montreal is ${temp} degrees Celsius. Wind speed is ${wind} kilometers per hour.`
    );

    res.type("text/xml");
    res.send(response.toString());
  } catch (error) {
    const response = new VoiceResponse();

    response.say("Sorry. I could not retrieve the weather right now.");

    res.type("text/xml");
    res.send(response.toString());
  }
});

app.listen(3000, () => {
  console.log("Weather phone server running");
});