const amqp = require('amqplib/callback_api');
require('dotenv').config();

const RabbitMQIP = process.env.RABBITMQ_IP;
const RabbitMQQueueName = process.env.RABBITMQ_QUEUE_NAME;
const RabbitMQDurable = process.env.RABBITMQ_DURABLE === 'true';
const RabbitMQUsername = process.env.RABBITMQ_USERNAME;
const RabbitMQPassword = process.env.RABBITMQ_PASSWORD;
const RabbitMQRetryInterval = process.env.RABBITMQ_RETRY_INTERVAL || 5000;

let rabbitmqConnection = null;
let rabbitmqChannel = null;

function connectToRabbitMQ(callback) {
    console.log("Connecting to RabbitMQ...");

    amqp.connect(`amqp://${RabbitMQUsername}:${RabbitMQPassword}@${RabbitMQIP}`, (error, connection) => {
        if (error) {
            console.error("RabbitMQ Connection Error:", error.message);
            setTimeout(() => connectToRabbitMQ(callback), RabbitMQRetryInterval);
            return;
        }

        rabbitmqConnection = connection;
        console.log("Connected to RabbitMQ");

        connection.on("error", (err) => {
            console.error("RabbitMQ Connection Error:", err.message);
            cleanupRabbitMQ();
            setTimeout(() => connectToRabbitMQ(callback), RabbitMQRetryInterval);
        });

        connection.on("close", () => {
            console.warn("RabbitMQ Connection Closed. Reconnecting...");
            cleanupRabbitMQ();
            setTimeout(() => connectToRabbitMQ(callback), RabbitMQRetryInterval);
        });

        createChannel(connection, callback);
    });
}

function createChannel(connection, callback) {
    connection.createChannel((error, channel) => {
        if (error) {
            console.error("RabbitMQ Channel Error:", error.message);
            setTimeout(() => createChannel(connection, callback), RabbitMQRetryInterval);
            return;
        }

        rabbitmqChannel = channel;
        console.log("RabbitMQ Channel Created");

        channel.on("error", (err) => {
            console.error("RabbitMQ Channel Error:", err.message);
            cleanupRabbitMQ();
            setTimeout(() => connectToRabbitMQ(callback), RabbitMQRetryInterval);
        });

        channel.on("close", () => {
            console.warn("RabbitMQ Channel Closed. Recreating...");
            cleanupRabbitMQ();
            setTimeout(() => connectToRabbitMQ(callback), RabbitMQRetryInterval);
        });

        channel.assertQueue(RabbitMQQueueName, { durable: RabbitMQDurable });
        channel.prefetch(1);
        callback(channel);
    });
}

function cleanupRabbitMQ() {
    if (rabbitmqChannel) {
        rabbitmqChannel.close();
        rabbitmqChannel = null;
    }
    if (rabbitmqConnection) {
        rabbitmqConnection.close();
        rabbitmqConnection = null;
    }
}

function onExitProcess() {
    console.log("Cleaning up RabbitMQ connection...");
    cleanupRabbitMQ();
}
process.on('SIGINT', onExitProcess);
process.on('SIGTERM', onExitProcess);
process.on('SIGQUIT', onExitProcess);

module.exports = {
    connectToRabbitMQ,
    RabbitMQQueueName,
};
