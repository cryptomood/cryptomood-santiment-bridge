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
    this.subClient = null;
    this.histClient = null;

    this.buffer = {};

    if (!CANDLE_TYPES[type]) {
      throw new Error("unknown candle type");
    }

    this.type = type;

    this.sentimentChannel = null;
  }

  normalizeCandle(sentimentData) {
    sentimentData.type = this.type;
    sentimentData.id = this.type + "_" + sentimentData.id;
    sentimentData.start_time = sentimentData.start_time.seconds;
  }

  async onData(sentimentData, force) {
    this.normalizeCandle(sentimentData);

    if (force || sentimentData.updated) {
      await exporter.sendDataWithKey(sentimentData, "id");
    }
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
      await this.processOlderData();
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

    this.subClient = new this.proto.MessagesProxy(
      SERVER,
      grpc.credentials.createSsl(fs.readFileSync(CERT_FILE_PATH)),
    );

    this.histClient = new this.proto.HistoricData(
      SERVER,
      grpc.credentials.createSsl(fs.readFileSync(CERT_FILE_PATH)),
    );

    return new Promise((resolve, reject) => {
      grpc.waitForClientReady(this.subClient, new Date().getTime() + 60000, (err) => {
        if (err) {
          return reject(err);
        } else {
          return resolve();
        }
      });
    }).then(() => new Promise((resolve, reject) => {
      grpc.waitForClientReady(this.histClient, new Date().getTime() + 60000, (err) => {
        if (err) {
          return reject(err);
        } else {
          return resolve();
        }
      });
    }))
  }

  getHistoricStream(config) {
    const method = this.type === CANDLE_TYPES.social ? this.histClient.HistoricSocialSentiment : this.histClient.HistoricNewsSentiment;
    return method.apply(this.histClient, [config, function (err) {
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
    console.log(`Sleeping - waiting for ${duration}ms (for the second half of current minute)`);
    return new Promise((resolve) => {
      setTimeout(resolve, duration);
    });
  }

  /**
   * Until streaming is done on the api side, we need to paginate
   * @returns {Promise<void>}
   */
  async processOlderData() {
    this.state = STATES.PROCESSING_HISTORIC_DATA;

    let firstTimestamp = await exporter.getLastPosition();
    let upToTimestamp;

    if (!firstTimestamp) {
      firstTimestamp = 0
    }

    let currentTimestamp = new Date();

    // waiting for the second halfminute if required - to be sure we wont be missing a candle
    if (currentTimestamp.getSeconds() < 30) {
      await this.sleep((30 - currentTimestamp.getSeconds()) * 1000);
    }

    // rounded to minute - if currentTimestamp is 17:45:01, then upToTimestamp will be 17:46:00
    // - 45th minute should be (after sleep call) in the storage
    // the range will be <firstTimestamp; upToTimestamp)
    currentTimestamp.setSeconds(0);
    currentTimestamp.setMilliseconds(0);
    currentTimestamp = currentTimestamp.getTime() / 1000;
    upToTimestamp = currentTimestamp + 60;

    console.log("Processing historic data: ", new Date(firstTimestamp * 1000), new Date(upToTimestamp * 1000));
    const stream = this.getHistoricStream({
      from: {seconds: firstTimestamp}, // greater or equal condition
      to: {seconds: upToTimestamp}, // less than condition
      resolution: "M1",
      all_assets: true
    });

    const promisifiedStream = () => {
      return new Promise((resolve, reject) => {
        stream.on("data", async (historicCandle) => {
          const candleTimestamp = parseInt(historicCandle.start_time.seconds);
          console.log("Processing", new Date(candleTimestamp * 1000));
          await this.onData(historicCandle, true);
          await exporter.savePosition(candleTimestamp - 60); // to be sure
        });
        stream.on("end", () => {
          console.log("Historic stream ended");
          resolve();
        });
        stream.on("error", (e) => {
          reject(e);
        });
      });
    };

    try {
      await promisifiedStream();
    } catch (e) {
      console.log("Historic stream error", e);
      process.exit(1);
    }

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
    this.sentimentChannel = this.subClient.SubscribeSocialSentiment({
      resolution: "M1",
      asset_filter: {all_assets: true}
    });
    this.sentimentChannel.on("data", this.channelCb.bind(this));
    this.sentimentChannel.on("end", this.channelEndedCb.bind(this));
    this.sentimentChannel.on("error", this.channelEndedCb.bind(this));
  }

  subscribeToNewsSentiment() {
    this.sentimentChannel = this.subClient.SubscribeSocialSentiment({
      resolution: "M1",
      asset_filter: {all_assets: true}
    });
    this.sentimentChannel.on("data", this.channelCb.bind(this));
    this.sentimentChannel.on("end", this.channelEndedCb.bind(this));
    this.sentimentChannel.on("error", this.channelEndedCb.bind(this));
  }

  async channelCb(candle) {
    const currentTime = parseInt(candle.start_time.seconds);
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