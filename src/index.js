const { connectToRabbitMQ, RabbitMQQueueName } = require('./rabbitmq');
const { writeToInfluxDB } = require('./influxdb');
require('dotenv').config();
const logInterval = process.env.LOG_INTERVAL || 5000;

let rabbitmqChannel = null;

function startConsumer() {
    connectToRabbitMQ((channel) => {
        rabbitmqChannel = channel;
        console.log(`[*] Waiting for messages in ${RabbitMQQueueName}. To exit press CTRL+C`);

        let messageCount = 0;
        let lastLogTime = Date.now();

        channel.consume(
            RabbitMQQueueName,
            async (msg) => {
                if (!msg) return;
                messageCount++;
                const lines = msg.content.toString().trim().split(/\r?\n/).map(line => line.trim());

                const currentTime = Date.now();
                if (currentTime - lastLogTime >= logInterval) {
                    const timestamp = new Date().toISOString();
                    console.log(`[${timestamp}] Processed ${messageCount} messages so far.`);
                    console.log(`[${timestamp}] Last message:`);
                    console.log(lines);
                    lastLogTime = currentTime;
                }

                try {
                    await writeToInfluxDB(lines);
                    channel.ack(msg);
                } catch (error) {
                    console.error('Failed to write to InfluxDB, rejecting message.', error);
                    channel.nack(msg, false, true);
                }
            },
            { noAck: false }
        );
    });
}

startConsumer();