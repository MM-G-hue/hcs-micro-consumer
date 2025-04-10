const { InfluxDB } = require('@influxdata/influxdb-client');
require('dotenv').config();

const InfluxDBToken = process.env.INFLUXDB_TOKEN;
const InfluxDBOrg = process.env.INFLUXDB_ORG;
const InfluxDBBucket = process.env.INFLUXDB_BUCKET;
const InfluxDBUrl = process.env.INFLUXDB_URL;
const influxBatchSize = process.env.INFLUXDB_BATCH_SIZE || 500;
const influxFlushInterval = process.env.INFLUXDB_FLUSH_INTERVAL || 5000;
const influxMaxRetries = process.env.INFLUXDB_MAX_RETRIES || 3;

if (!InfluxDBToken || !InfluxDBOrg || !InfluxDBBucket || !InfluxDBUrl) {
    throw new Error('Missing InfluxDB environment variables');
}

const influxDB = new InfluxDB({ url: InfluxDBUrl, token: InfluxDBToken });
const writeApi = influxDB.getWriteApi(InfluxDBOrg, InfluxDBBucket, 's', {
    batchSize: influxBatchSize,
    flushInterval: influxFlushInterval,
    maxRetries: influxMaxRetries,
});

async function writeToInfluxDB(lines) {
    await writeApi.writeRecords(lines);
}

module.exports = {
    writeToInfluxDB,
};
