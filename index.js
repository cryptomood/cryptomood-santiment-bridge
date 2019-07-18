const fs = require("fs");
const grpc = require("grpc");
const protoLoader = require("@grpc/proto-loader");
const sanExporterPkg = require('san-exporter/package.json');
const {Exporter} = require('san-exporter/index');
const exporter = new Exporter(sanExporterPkg.name);

const SERVER = process.env.CRYPTOMOOD_URL;
const CERT_FILE_PATH = process.env.CERT_FILE_PATH;
const PROTO_FILE_PATH = process.env.PROTO_FILE_PATH;
const ONLY_HISTORIC = process.env.ONLY_HISTORIC === "1";
const CANDLE_TYPE = process.env.CANDLE_TYPE;

const CANDLE_TYPES = {
  news: 'news',
  social: 'social',
};

const STATES = {
  INIT: -1,
  PROCESSING_HISTORIC_DATA: 0,
  PROCESSING_BUFFERED_DATA: 1,
  NORMAL: 2,
};

class CryptomoodSanExporter {
  constructor(type) {
    this.state = STATES.INIT;
    this.proto = null;
    this.sentClient = null;
    this.datasetClient = null;

    this.buffer = {};

    if (!CANDLE_TYPES[type]) {
      throw new Error("unknown candle type");
    }

    this.type = type;

    this.sentimentChannel = null;
  }

  normalizeCandle(candle) {
    candle.type = this.type;
    candle.key = `${this.type}_${this.getCandleTimestamp(candle)}_${candle.resolution}_${candle.asset}`;
  }

  getCandleTimestamp(candle) {
    return new Date(Date.parse(`${candle.id.year}-${candle.id.month}-${candle.id.day} ${candle.id.hour}:${candle.id.minute}:00Z`)).getTime() / 1000;
  }

  async onData(candle) {
    this.normalizeCandle(candle);
    await exporter.sendDataWithKey(candle, "key");
  }

  async run() {
    await exporter.connect();
    //await exporter.savePosition(null); // reset

    try {
      await this.connectCryptomood();
    } catch (e) {
      console.log("Cryptomood connection cannot be established", e);
      process.exit(1);
    }

    if (!ONLY_HISTORIC) {
      if (this.type === CANDLE_TYPES.social) {
        this.subscribeToSocialSentiment();
      } else {
        this.subscribeToNewsSentiment();
      }
    }

    try {
      const assets = await this.getAllAssets();
      await this.sleep(2000)
      await this.processOlderData(assets);
    } catch (e) {
      console.log("processOlderData unknown error", e);
      process.exit(1);
    }

    this.state = STATES.NORMAL;
    if (ONLY_HISTORIC) {
      process.exit(0);
    }
  }

  connectCryptomood() {
    this.proto = grpc.loadPackageDefinition(
      protoLoader.loadSync(PROTO_FILE_PATH, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
      })
    );

    this.sentClient = new this.proto.Sentiments(
      SERVER,
      grpc.credentials.createSsl(fs.readFileSync(CERT_FILE_PATH)),
    );

    this.datasetClient = new this.proto.Dataset(
      SERVER,
      grpc.credentials.createSsl(fs.readFileSync(CERT_FILE_PATH)),
    );

    return new Promise((resolve, reject) => {
      grpc.waitForClientReady(this.sentClient, new Date().getTime() + 60000, (err) => {
        if (err) {
          return reject(err);
        } else {
          return resolve();
        }
      });
    }).then(() => new Promise((resolve, reject) => {
      grpc.waitForClientReady(this.datasetClient, new Date().getTime() + 60000, (err) => {
        if (err) {
          return reject(err);
        } else {
          return resolve();
        }
      });
    }));
  }

  getAllAssets() {
    return new Promise((resolve, reject) => {
      return this.datasetClient.Assets({}, (err, req) => {
        if (err) {
          reject(err);
        }
        resolve(req);
      });
    });
  }

  getHistoricStream(config) {
    const method = this.type === CANDLE_TYPES.social ? this.sentClient.HistoricSocialSentiment : this.sentClient.HistoricNewsSentiment;
    return method.apply(this.sentClient, [config, function (err) {
      if (err) {
        console.log("Stream cannot be created");
        process.exit(1);
      }
    }]);
  }

  /**
   * Awaitable timeout
   * @param duration
   * @returns {Promise<any>}
   */
  sleep(duration) {
    return new Promise((resolve) => {
      setTimeout(resolve, duration);
    });
  }

  /**
   * Until streaming is done on the api side, we need to paginate
   * @returns {Promise<void>}
   */
  async processOlderData(assets) {
    this.state = STATES.PROCESSING_HISTORIC_DATA;

    let firstTimestamp = await exporter.getLastPosition();
    let upToTimestamp;

    if (!firstTimestamp) {
      firstTimestamp = 0
    }

    let currentTimestamp = new Date();

    currentTimestamp.setSeconds(0);
    currentTimestamp.setMilliseconds(0);
    currentTimestamp = currentTimestamp.getTime() / 1000;
    upToTimestamp = currentTimestamp + 60;

    console.log("Processing historic data: ", new Date(firstTimestamp * 1000), new Date(upToTimestamp * 1000));

    const promisifiedStream = (currentStream) => {
      return new Promise((resolve, reject) => {
        const allData = [];
        currentStream.on("data", async (candle) => {
          allData.push(candle)
        });
        currentStream.on("end", async () => {
          for (const singleData of allData.reverse()) {
            await this.onData(singleData);
          }
          resolve();
        });
        currentStream.on("error", (e) => {
          reject(e);
        });
      });
    };

    for (const wantedAsset of assets.assets) {
      const stream = this.getHistoricStream({
        from: {seconds: firstTimestamp}, // greater or equal condition
        to: {seconds: upToTimestamp}, // less than condition
        resolution: "M1",
        asset: wantedAsset.symbol,
      });

      console.log("Processing historic ", wantedAsset.symbol)
      try {
        await promisifiedStream(stream);
        await this.sleep(2000)
      } catch (e) {
        console.log("Historic stream error", e);
        process.exit(1);
      }
    }

    console.log("Historic stream ended");

    await this.processBuffered();
    console.log('Historical data processed');
  }

  async processBuffered() {
    this.state = STATES.PROCESSING_BUFFERED_DATA;

    const bufferedTimes = Object.keys(this.buffer).sort();
    for (const time of bufferedTimes) {
      for (const candle of this.buffer[time]) {
        await this.onData(candle);
      }
    }

    this.state = STATES.NORMAL;
  }

  /**
   * Will stop any incoming candle while processing buffered items
   * @returns {Promise<*>}
   */
  async waitUntilBufferProcessed() {
    return new Promise((resolve) => {
      var int = setInterval(() => {
        if (this.state === STATES.NORMAL) {
          clearInterval(int);
          resolve();
        }
      }, 1000);
    });
  }

  subscribeToSocialSentiment() {
    this.sentimentChannel = this.sentClient.SubscribeSocialSentiment({
      resolution: "M1",
      assets_filter: {all_assets: true}
    });
    console.log("Subscribed to social sentiment candles")
    this.sentimentChannel.on("data", this.channelCb.bind(this));
    this.sentimentChannel.on("end", this.channelEndedCb.bind(this));
    this.sentimentChannel.on("error", this.channelEndedCb.bind(this));
  }

  subscribeToNewsSentiment() {
    this.sentimentChannel = this.sentClient.SubscribeSocialSentiment({
      resolution: "M1",
      assets_filter: {all_assets: true}
    });
    console.log("Subscribed to news sentiment candles")
    this.sentimentChannel.on("data", this.channelCb.bind(this));
    this.sentimentChannel.on("end", this.channelEndedCb.bind(this));
    this.sentimentChannel.on("error", this.channelEndedCb.bind(this));
  }

  async channelCb(candle) {
    const currentTime = this.getCandleTimestamp(candle);
    switch (this.state) {
      case STATES.PROCESSING_HISTORIC_DATA:
        if (!this.buffer[currentTime]) {
          this.buffer[currentTime] = [];
        }
        this.buffer[currentTime].push(candle);
        break;

      case STATES.PROCESSING_BUFFERED_DATA:
        await this.waitUntilBufferProcessed();
        await this.onData(candle);
        break;

      case STATES.NORMAL:
        await this.onData(candle);
        await exporter.savePosition(currentTime);
        break;
    }
  }

  channelEndedCb(e) {
    console.log("Subscription stream error", e);
    process.exit(1);
  }
}

const cr = new CryptomoodSanExporter(CANDLE_TYPE);
cr.run();