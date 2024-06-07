const express = require("express");
const axios = require("axios");
const bodyParser = require("body-parser");
const cors = require("cors");
const UpstoxClient = require("upstox-js-sdk");
const WebSocket = require("ws");
const protobuf = require("protobufjs");

const app = express();
const port = 3002;
const clientId = "44a61fa4-3712-40ce-aad7-f1f9b5bd8667";
const clientSecret = "xsl5zldfe5";
const redirectUri = "http://127.0.0.1:4000";
let protobufRoot = null;
let accessToken = null;
const apiVersion = "2.0";

app.use(cors());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// Set up the WebSocket server
const wss = new WebSocket.Server({ port: 8080 });

// Function to authorize the market data feed
const getMarketFeedUrl = async () => {
  return new Promise((resolve, reject) => {
    const apiInstance = new UpstoxClient.WebsocketApi(); // Create new Websocket API instance

    // Call the getMarketDataFeedAuthorize function from the API
    apiInstance.getMarketDataFeedAuthorize(
      apiVersion,
      (error, data, response) => {
        if (error) reject(error); // If there's an error, reject the promise
        else resolve(data.data.authorizedRedirectUri); // Else, resolve the promise with the authorized URL
      }
    );
  });
};

const connectWebSocket = async (wsUrl) => {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl, {
      headers: {
        "Api-Version": apiVersion,
        Authorization: `Bearer ${accessToken}`,
      },
      followRedirects: true,
    });

    // WebSocket event handlers
    ws.on("open", () => {
      console.log("connected");
      resolve(ws); // Resolve the promise once connected

      // Set a timeout to send a subscription message after 1 second
      setTimeout(() => {
        const data = {
          guid: "someguid",
          method: "sub",
          data: {
            mode: "full",
            instrumentKeys: ["NSE_INDEX|Nifty Bank", "NSE_INDEX|Nifty 50"],
          },
        };
        ws.send(Buffer.from(JSON.stringify(data)));
      }, 1000);
    });

    ws.on("close", () => {
      console.log("disconnected");
    });

    ws.on("message", (data) => {
      console.log(JSON.stringify(decodeProtobuf(data))); // Decode the protobuf message on receiving it
    });

    ws.on("error", (error) => {
      console.log("error:", error);
      reject(error); // Reject the promise on error
    });
  });
};

// Function to initialize the protobuf part
const initProtobuf = async () => {
  protobufRoot = await protobuf.load(__dirname + "/MarketDataFeed.proto");
  console.log("Protobuf part initialization complete");
};

// Function to decode protobuf message
const decodeProtobuf = (buffer) => {
  if (!protobufRoot) {
    console.warn("Protobuf part not initialized yet!");
    return null;
  }

  const FeedResponse = protobufRoot.lookupType(
    "com.upstox.marketdatafeeder.rpc.proto.FeedResponse"
  );
  return FeedResponse.decode(new Uint8Array(buffer));
};

// Initialize the protobuf part
initProtobuf();

app.post("/auth/code", async (req, res) => {
  const { code } = req.body;
  if (!code) {
    return res.status(400).send("Authorization code is missing");
  }

  try {
    const response = await axios.post(
      "https://api.upstox.com/v2/login/authorization/token",
      new URLSearchParams({
        client_id: clientId,
        client_secret: clientSecret,
        redirect_uri: redirectUri,
        grant_type: "210297",
        code,
      }),
      {
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Accept: "application/json",
        },
      }
    );
    accessToken = response.data.access_token;
    res.json({ access_token: accessToken });
    console.log("Access token is saved:", accessToken);

    // Initialize WebSocket connection after access token is generated
    try {
      const wsUrl = "wss://api.upstox.com/v2/feed/market-data-feed"; // WebSocket URL
      console.log("WebSocket URL:", wsUrl);
      console.log("Access Token:", accessToken);
      const ws = await connectWebSocket(wsUrl); // Connect to the WebSocket

      // Forward WebSocket messages to connected clients
      wss.on("connection", (client) => {
        console.log("WebSocket server connected");

        ws.on("message", (message) => {
          client.send(message); // Send the market data to the connected client
        });

        client.on("close", () => {
          console.log("WebSocket client disconnected");
        });

        client.on("error", (error) => {
          console.error("WebSocket client error:", error);
        });
      });
    } catch (error) {
      console.error("An error occurred while connecting to WebSocket:", error);
    }
  } catch (error) {
    console.error(
      "Error exchanging code for tokens:",
      error.response ? error.response.data : error.message
    );
    res.status(500).send("Error exchanging code for tokens");
  }
});

app.listen(port, async () => {
  console.log(`Server running at http://192.168.0.106:${port}`);
});
