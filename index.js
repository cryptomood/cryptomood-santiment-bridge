const fs = require("fs");
const grpc = require("grpc");
const protoLoader = require("@grpc/proto-loader");
const sanExporterPkg = require('san-exporter/package.json');
const {Exporter} = require('san-exporter/index');
const exporter = new Exporter(sanExporterPkg.name);

const SERVER = process.env.CRYPTOMOOD_URL;
const CERT_FILE_PATH = process.env.CERT_FILE_PATH;
const PROTO_FILE_PATH = process.env.PROTO_FILE_PATH;

const SOCIAL_CANDLE_TYPE = {
  NEWS: 'news',
  SOCIAL: 'social',
};

class CryptomoodSanExporter {
  constructor() {
    this.proto = null;
    this.client = null;
    this.socialSentimentChannel = null;
    this.newsSentimentChannel = null;

    this.onSocialData = this.onData.bind(this, SOCIAL_CANDLE_TYPE.SOCIAL);
    this.onNewsData = this.onData.bind(this, SOCIAL_CANDLE_TYPE.NEWS);
  }

  async onData(type, sentimentData) {
    sentimentData.type = type;
    sentimentData.id = type + "_" + sentimentData.id;
    sentimentData.start_time = sentimentData.start_time.seconds;

    await exporter.sendDataWithKey(sentimentData, "id");
    console.log('ok', sentimentData.id)
  }

  async run() {
    await exporter.connect();

    try {
      await this.subscribe(this.onSocialData, this.onNewsData);
    } catch (e) {
      console.log(e);
    }
  }

  subscribe(socialCb, newsCb) {
    this.proto = grpc.loadPackageDefinition(
      protoLoader.loadSync(PROTO_FILE_PATH, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
      })
    );

    this.client = new this.proto.MessagesProxy(
      SERVER,
      grpc.credentials.createSsl(fs.readFileSync(CERT_FILE_PATH)),
    );

    return new Promise((resolve, reject) => {
      grpc.waitForClientReady(this.client, new Date().getTime() + 60000, (err) => {
        if (err) {
          return reject(err);
        }
        console.log("Cryptomood connected")
        this.socialSentimentChannel = this.client.SubscribeSocialSentiment();
        this.socialSentimentChannel.on("data", socialCb);

        this.newsSentimentChannel = this.client.SubscribeNewsSentiment();
        this.newsSentimentChannel.on("data", newsCb);
      });
    });
  }
}

const cr = new CryptomoodSanExporter()
cr.run();
