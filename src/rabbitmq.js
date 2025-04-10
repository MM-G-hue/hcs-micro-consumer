const amqp = require('amqplib/callback_api');
require('dotenv').config();

const RabbitMQIP = process.env.RABBITMQ_IP;
const RabbitMQQueueName = process.env.RABBITMQ_QUEUE_NAME;
const RabbitMQDurable = process.env.RABBITMQ_DURABLE === 'true';
const RabbitMQUsername = process.env.RABBITMQ_USERNAME;
const RabbitMQPassword = process.env.RABBITMQ_PASSWORD;

function connectToRabbitMQ(callback) {
    console.log("Connecting to RabbitMQ...");

    amqp.connect(`amqp://${RabbitMQUsername}:${RabbitMQPassword}@${RabbitMQIP}`, (error, connection) => {
        if (error) {
            console.error("RabbitMQ Connection Error:", error.message);
            setTimeout(() => connectToRabbitMQ(callback), 5000);
            return;
        }

        console.log("Connected to RabbitMQ");

        connection.on("error", (err) => {
            console.error("RabbitMQ Connection Error:", err.message);
            setTimeout(() => connectToRabbitMQ(callback), 5000);
        });

        connection.on("close", () => {
            console.warn("RabbitMQ Connection Closed. Reconnecting...");
            setTimeout(() => connectToRabbitMQ(callback), 5000);
        });

        connection.createChannel((error, channel) => {
            if (error) {
                console.error("RabbitMQ Channel Error:", error.message);
                return;
            }

            channel.assertQueue(RabbitMQQueueName, { durable: RabbitMQDurable });
            channel.prefetch(1);
            callback(channel);
        });
    });
}

module.exports = {
    connectToRabbitMQ,
    RabbitMQQueueName,
};
