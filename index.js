const amqp = require('amqplib/callback_api');
const { InfluxDB } = require('@influxdata/influxdb-client');
const fs = require('fs');
const path = require('path');
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

let rabbitmqConnection = null;
let rabbitmqChannel = null;

function connectRabbitMQ() {
    console.log("Connecting to RabbitMQ...");

    amqp.connect(`amqp://${RabbitMQUsername}:${RabbitMQPassword}@${RabbitMQIP}`, function (error0, connection) {
        if (error0) {
            console.error("RabbitMQ Connection Error:", error0.message);
            setTimeout(connectRabbitMQ, 5000);
            return;
        }

        rabbitmqConnection = connection;
        connection.on("error", err => {
            console.error("RabbitMQ Connection Error:", err.message);
            setTimeout(connectRabbitMQ, 5000);
        });

        connection.on("close", () => {
            console.warn("RabbitMQ Connection Closed. Reconnecting...");
            setTimeout(connectRabbitMQ, 5000);
        });

        console.log("Connected to RabbitMQ");
        createChannel(connection);
    });
}

function createChannel(connection) {
    connection.createChannel(function (error1, channel) {
        if (error1) {
            console.error("RabbitMQ Channel Error:", error1.message);
            return;
        }

        rabbitmqChannel = channel;
        channel.on("error", err => {
            console.error("RabbitMQ Channel Error:", err.message);
        });

        channel.on("close", () => {
            console.warn("RabbitMQ Channel Closed. Reconnecting...");
            setTimeout(connectRabbitMQ, 5000);
        });

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
        let lastLogTime = Date.now();
        const LOG_INTERVAL = 5000; // Log every 5 seconds

        channel.consume(
            RabbitMQQueueName,
            async function (msg) {
                if (!msg) return;
                messageCount++;
                const lines = msg.content.toString().trim().split(/\r?\n/).map(line => line.trim());

                const currentTime = Date.now();
                if (currentTime - lastLogTime >= LOG_INTERVAL) {
                    const timestamp = new Date().toISOString();
                    console.log(`[${timestamp}] Processed ${messageCount} messages so far.`);
                    console.log(`[${timestamp}] Last message:`);
                    console.log(lines);
                    lastLogTime = currentTime;
                }

                try {
                    writeApi.writeRecords(lines);

                    // Ensure flush after writing a batch (optional)
                    // if (messageCount % 500 === 0) {
                    //     console.log("Flushing InfluxDB buffer...");
                    //     await writeApi.flush();
                    // }

                    channel.ack(msg);
                } catch (error) {
                    console.error('Failed to write to InfluxDB, retrying...', error);
                    channel.nack(msg, false, true);
                }
            },
            { noAck: false }
        );
    });
}

function logMemoryUsage(interval = 5000, filename = 'memory_log_consumer.csv') {
    const filePath = path.join(__dirname, filename);
    const headers = 'Timestamp,RSS (bytes),Heap Total (bytes),Heap Used (bytes),External (bytes),Array Buffers (bytes)\n';
    
    // Write headers if file doesn't exist
    if (!fs.existsSync(filePath)) {
        fs.writeFileSync(filePath, headers);
    }

    setInterval(() => {
        const mem = process.memoryUsage();
        const timestamp = new Date().toISOString();
        const data = `${timestamp},${mem.rss},${mem.heapTotal},${mem.heapUsed},${mem.external},${mem.arrayBuffers}\n`;
        
        fs.appendFile(filePath, data, (err) => {
            if (err) console.error('Error writing to log file:', err);
        });
    }, interval);
}

logMemoryUsage(); // Logs memory every 5 seconds


// Start consuming messages from RabbitMQ
connectRabbitMQ();