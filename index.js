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
    //await exporter.savePosition(null) // reset

    try {
      await this.connectCryptomood();
    } catch (e) {
      console.log(e);
      return false;
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
      console.log(e);
      return false;
    }

    this.state = STATES.NORMAL;
    if (ONLY_HISTORIC) {
      process.exit();
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

  async getHistoricRange() {
    const method = this.type === CANDLE_TYPES.SOCIAL ? this.histClient.HistoricSocialSentimentRange : this.histClient.HistoricNewsSentimentRange;

    return new Promise((resolve, reject) => {
      method.apply(this.histClient, [null, function (err, req) {
        if (err) {
          reject(err);
        } else {
          resolve(req);
        }
      }]);
    })
  }

  async getHistoricData(config) {
    const method = this.type === CANDLE_TYPES.SOCIAL ? this.histClient.HistoricSocialSentiment : this.histClient.HistoricNewsSentiment;

    return new Promise((resolve, reject) => {
      method.apply(this.histClient, [config, function (err, req) {
        if (err) {
          reject(err);
        } else {
          resolve(req.items);
        }
      }]);
    })
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
      const possibleDataRange = await this.getHistoricRange();
      firstTimestamp = parseInt(possibleDataRange.first.seconds);
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

    console.log("Possible data range", new Date(firstTimestamp * 1000), new Date(upToTimestamp * 1000));

    const windowSize = 300; // by 5 minute steps

    let windowStart = firstTimestamp;
    let windowEnd = windowStart;

    while (windowStart < upToTimestamp) {
      windowEnd += windowSize;

      const data = await this.getHistoricData({
        from: {seconds: windowStart}, // greater or equal condition
        to: {seconds: Math.min(windowEnd, upToTimestamp)}, // less than condition
        resolution: "M1",
        allAssets: true
      });

      console.log('Processed', new Date(windowStart * 1000), new Date(Math.min(windowEnd, upToTimestamp) * 1000));
      windowStart = windowEnd;

      let lastPosition = windowEnd;
      for (const candle of data) {
        await this.onData(candle, true);
        lastPosition = parseInt(candle.start_time) + 60;
      }
      await exporter.savePosition(lastPosition);
    }

    this.processBuffered();
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
    this.sentimentChannel = this.subClient.SubscribeSocialSentiment();
    this.sentimentChannel.on("data", this.channelCb.bind(this));
  }

  subscribeToNewsSentiment() {
    this.sentimentChannel = this.subClient.SubscribeSocialSentiment();
    this.sentimentChannel.on("data", this.channelCb.bind(this));
  }

  async channelCb(candle) {
    const currentTime = parseInt(candle.start_time.seconds);
    if (candle.resolution !== "M1") {
      return;
    }
    switch (this.state) {
      case STATES.PROCESSING_HISTORIC_DATA:
        if (!this.buffer[currentTime]) {
          this.buffer[currentTime] = [];
        }
        this.buffer[currentTime].push(candle);
        break;

      case STATES.PROCESSING_BUFFERED_DATA:
        await this.waitUntilBufferProcessed();
        this.onData(candle);
        break;

      case STATES.NORMAL:
        this.onData(candle);
        try {
          await exporter.savePosition(currentTime);
        } catch (e) {
          console.warn("Error while saving position", currentTime)
        }
        break;
    }
  }
}

const cr = new CryptomoodSanExporter(CANDLE_TYPE);
cr.run();
