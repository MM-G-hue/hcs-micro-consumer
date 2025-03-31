const amqp = require('amqplib/callback_api');
const { InfluxDB } = require('@influxdata/influxdb-client');

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
const writeApi = influxDB.getWriteApi(InfluxDBOrg, InfluxDBBucket, 's', {
    batchSize: 500,       // Flush when 500 points accumulate
    flushInterval: 5000,  // Flush every 5 seconds if not full
    maxRetries: 3,        // Retry up to 3 times on failure
});

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
                    const lines = msg.content.toString().trim().split(/\r?\n/).map(line => line.trim());

                    if (messageCount % 100 === 0) {
                        console.log(`[x] Processed ${messageCount} messages.`);
                    }

                    try {
                        // Write directly to InfluxDB's built-in buffer
                        writeApi.writeRecords(lines);

                        // Ensure flush after writing a batch (optional)
                        // if (messageCount % 500 === 0) {
                        //     console.log("Flushing InfluxDB buffer...");
                        //     await writeApi.flush();
                        // }

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
