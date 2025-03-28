var amqp = require('amqplib/callback_api');

require('dotenv').config();
const RabbitMQIP = process.env.RABBITMQ_IP;
const RabbitMQQueueName = process.env.RABBITMQ_QUEUE_NAME;
const RabbitMQDurable = process.env.RABBITMQ_DURABLE;
const RabbitMQUsername = process.env.RABBITMQ_USERNAME;
const RabbitMQPassword = process.env.RABBITMQ_PASSWORD;

// Add delay function
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

amqp.connect(`amqp://${RabbitMQUsername}:${RabbitMQPassword}@${RabbitMQIP}`, function (error0, connection) {
    if (error0) {
        throw error0;
    }

    connection.createChannel(function (error1, channel) {
        if (error1) {
            throw error1;
        }
        channel.assertQueue(RabbitMQQueueName, {
            durable: RabbitMQDurable // Durable protects against message loss if RabbitMQ server crashes
        });
        
        /**
         * This tells RabbitMQ not to give more than one message to a worker at a time.
         * Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous one.
         * Instead, it will dispatch it to the next worker that is not still busy.
         */
        channel.prefetch(1);

        console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", RabbitMQQueueName);

        let messageCount = 0;

        channel.consume(RabbitMQQueueName, async function (msg) {
            messageCount++;
            if (messageCount % 100 === 0) {
                console.log(" [x] Received 100 messages, latest: %s", msg.content.toString());
            }
            channel.ack(msg);
        }, {
            noAck: false
        });

    });
});