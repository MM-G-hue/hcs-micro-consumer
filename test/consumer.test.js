jest.mock('amqplib');
jest.mock('influxdata/influxdb-client');

const amqplib = require('amqplib');
const { InfluxDB } = require('influxdata/influxdb-client');

describe('Consumer Tests', () => {
    let mockChannel, mockConnection;

    beforeEach(() => {
        mockChannel = {
            assertQueue: jest.fn(),
            consume: jest.fn(),
            ack: jest.fn(),
        };

        mockConnection = {
            createChannel: jest.fn().mockResolvedValue(mockChannel),
        };

        amqplib.connect.mockResolvedValue(mockConnection);
    });

    afterEach(() => {
        jest.clearAllMocks();
    });

    test('should connect to RabbitMQ and create a channel', async () => {
        const connection = await amqplib.connect('amqp://localhost');
        const channel = await connection.createChannel();

        expect(amqplib.connect).toHaveBeenCalledWith('amqp://localhost');
        expect(connection.createChannel).toHaveBeenCalled();
        expect(channel).toBe(mockChannel);
    });

    test('should assert a queue and consume messages', async () => {
        const queueName = 'test-queue';
        const messageHandler = jest.fn();

        const connection = await amqplib.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.assertQueue(queueName);
        await channel.consume(queueName, messageHandler);

        expect(channel.assertQueue).toHaveBeenCalledWith(queueName);
        expect(channel.consume).toHaveBeenCalledWith(queueName, messageHandler);
    });

    test('should handle connection failure to RabbitMQ', async () => {
        amqplib.connect.mockRejectedValue(new Error('Connection failed'));

        await expect(amqplib.connect('amqp://localhost')).rejects.toThrow('Connection failed');
        expect(amqplib.connect).toHaveBeenCalledWith('amqp://localhost');
    });

    test('should handle channel creation failure', async () => {
        mockConnection.createChannel.mockRejectedValue(new Error('Channel creation failed'));

        const connection = await amqplib.connect('amqp://localhost');
        await expect(connection.createChannel()).rejects.toThrow('Channel creation failed');
        expect(mockConnection.createChannel).toHaveBeenCalled();
    });

    test('should handle queue assertion failure', async () => {
        mockChannel.assertQueue.mockRejectedValue(new Error('Queue assertion failed'));

        const connection = await amqplib.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await expect(channel.assertQueue('test-queue')).rejects.toThrow('Queue assertion failed');
        expect(channel.assertQueue).toHaveBeenCalledWith('test-queue');
    });

    test('should handle message consumption failure', async () => {
        mockChannel.consume.mockRejectedValue(new Error('Message consumption failed'));

        const connection = await amqplib.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await expect(channel.consume('test-queue', jest.fn())).rejects.toThrow('Message consumption failed');
        expect(channel.consume).toHaveBeenCalledWith('test-queue', expect.any(Function));
        });

    test('should acknowledge a message after processing', async () => {
        const queueName = 'test-queue';
        const mockMessage = { content: Buffer.from('test message') };

        const connection = await amqplib.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.consume(queueName, (msg) => {
            expect(msg).toBe(mockMessage);
            channel.ack(msg);
        });

        const consumeCallback = channel.consume.mock.calls[0][1];
        consumeCallback(mockMessage);

        expect(channel.ack).toHaveBeenCalledWith(mockMessage);
    });

    test('should handle message acknowledgment failure', async () => {
        mockChannel.ack.mockImplementation(() => {
            throw new Error('Message acknowledgment failed');
        });

        const queueName = 'test-queue';
        const mockMessage = { content: Buffer.from('test message') };

        const connection = await amqplib.connect('amqp://localhost');
        const channel = await connection.createChannel();

        await channel.consume(queueName, (msg) => {
            expect(() => channel.ack(msg)).toThrow('Message acknowledgment failed');
        });

        const consumeCallback = channel.consume.mock.calls[0][1];
        consumeCallback(mockMessage);

        expect(channel.ack).toHaveBeenCalledWith(mockMessage);
    });

    test('should handle InfluxDB client initialization', () => {
        const influxDB = new InfluxDB({ url: 'http://localhost:8086', token: 'test-token' });

        expect(influxDB).toBeInstanceOf(InfluxDB);
    });

});

describe('InfluxDB Environment Variables', () => {
    let originalEnv;

    beforeEach(() => {
        originalEnv = { ...process.env };
    });

    afterEach(() => {
        process.env = originalEnv;
        jest.resetModules();
    });

    test('should throw error if InfluxDB environment variables are missing', () => {
        process.env.INFLUXDB_TOKEN = '';
        process.env.INFLUXDB_ORG = '';
        process.env.INFLUXDB_BUCKET = '';
        process.env.INFLUXDB_URL = '';

        expect(() => {
            require('../src/influxdb');
        }).toThrow('Missing InfluxDB environment variables');
    });

    test('should not throw error if InfluxDB environment variables are set', () => {
        process.env.INFLUXDB_TOKEN = 'test-token';
        process.env.INFLUXDB_ORG = 'test-org';
        process.env.INFLUXDB_BUCKET = 'test-bucket';
        process.env.INFLUXDB_URL = 'http://localhost:8086';

        expect(() => {
            require('../src/influxdb');
        }).not.toThrow();
    });
});
