const amqp = require('amqplib/callback_api');
const { InfluxDB } = require('@influxdata/influxdb-client');
const { Writable } = require('stream');

require('dotenv').config();

// RabbitMQ Connection Details
const RabbitMQIP = process.env.RABBITMQ_IP;
const RabbitMQQueueName = process.env.RABBITMQ_QUEUE_NAME;
const RabbitMQDurable = process.env.RABBITMQ_DURABLE === 'true';
const RabbitMQUsername = process.env.RABBITMQ_USERNAME;
const RabbitMQPassword = process.env.RABBITMQ_PASSWORD;

// InfluxDB Connection Details
const InfluxDBToken = process.env.INFLUXDB_TOKEN;
const InfluxDBOrg = process.env.INFLUXDB_ORG;
const InfluxDBBucket = process.env.INFLUXDB_BUCKET;
const InfluxDBUrl = process.env.INFLUXDB_URL;

const influxDB = new InfluxDB({ url: InfluxDBUrl, token: InfluxDBToken });
const writeApi = influxDB.getWriteApi(InfluxDBOrg, InfluxDBBucket, 'ns'); // 'ms' ensures timestamp precision

// Create a writable stream with error handling
class InfluxLineProtocolStream extends Writable {
    constructor(options) {
        super({ objectMode: true, ...options });
        this.buffer = [];
        this.batchSize = options.batchSize || 500;
        this.flushInterval = options.flushInterval || 5000;
        this.timer = setInterval(() => this.flush(), this.flushInterval);
    }

    async _write(lineProtocolData, encoding, callback) {
        try {
            this.buffer.push(lineProtocolData);

            if (this.buffer.length >= this.batchSize) {
                await this.flush();
            }

            callback();
        } catch (error) {
            console.error('InfluxDB Write Failed:', error);
            callback(error); // Pass error to retry mechanism
        }
    }

    async flush() {
        if (this.buffer.length > 0) {
            const batch = this.buffer.join('\n'); // Join buffered data into Influx Line Protocol batch

            try {
                await writeApi.writeRecord(batch);
                this.buffer = [];
                console.log('InfluxDB WRITE SUCCESS');
            } catch (error) {
                console.error('InfluxDB Flush Error:', error);
                throw error; // Throw error to indicate failure
            }
        }
    }

    async _final(callback) {
        clearInterval(this.timer);
        try {
            await this.flush();
            await writeApi.close();
            console.log('InfluxDB WRITE STREAM CLOSED');
            callback();
        } catch (error) {
            callback(error);
        }
    }
}

const influxStream = new InfluxLineProtocolStream({ batchSize: 500, flushInterval: 3000 });

function connectRabbitMQ() {
    amqp.connect(`amqp://${RabbitMQUsername}:${RabbitMQPassword}@${RabbitMQIP}`, function (error0, connection) {
        if (error0) {
            throw error0;
        }

        connection.createChannel(function (error1, channel) {
            if (error1) {
                throw error1;
            }

            // Durable protects against message loss if RabbitMQ server crashes
            channel.assertQueue(RabbitMQQueueName, { durable: RabbitMQDurable });

            /**
             * This tells RabbitMQ not to give more than one message to a worker at a time.
             * Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous one.
             * Instead, it will dispatch it to the next worker that is not still busy.
             */
            channel.prefetch(1);

            console.log(`[*] Waiting for messages in ${RabbitMQQueueName}. To exit press CTRL+C`);

            let messageCount = 0;

            channel.consume(
                RabbitMQQueueName,
                async function (msg) {
                    messageCount++;
                    const ilpData = msg.content.toString().trim();

                    if (messageCount % 100 === 0) {
                        console.log(`[x] Processed ${messageCount} messages, latest: ${ilpData}`);
                    }

                    try {
                        // Send data to InfluxDB
                        await new Promise((resolve, reject) => {
                            influxStream.write(ilpData, 'utf-8', (err) => {
                                if (err) reject(err);
                                else resolve();
                            });
                        });

                        // Acknowledge the message ONLY if InfluxDB write succeeds
                        channel.ack(msg);
                    } catch (error) {
                        console.error('Failed to write to InfluxDB, retrying...', error);
                        channel.nack(msg, false, true); // Requeue the message for retry
                    }
                },
                { noAck: false }
            );
        });
    });
}

// Start consuming messages from RabbitMQ
connectRabbitMQ();
