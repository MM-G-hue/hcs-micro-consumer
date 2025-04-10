const amqplib = {
    connect: jest.fn((url, callback) => {
        const mockConnection = {
            createChannel: jest.fn((cb) => {
                const mockChannel = {
                    assertQueue: jest.fn(),
                    prefetch: jest.fn(),
                    consume: jest.fn(),
                    sendToQueue: jest.fn(),
                    ack: jest.fn(),
                    nack: jest.fn(),
                    close: jest.fn(),
                    on: jest.fn(),
                };
                cb(null, mockChannel);
            }),
            close: jest.fn(),
            on: jest.fn(),
        };
        callback(null, mockConnection);
    }),
};

module.exports = amqplib;
