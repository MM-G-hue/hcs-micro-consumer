class MockWriteApi {
    constructor() {
        this.writeRecords = jest.fn(async (lines) => Promise.resolve());
    }
}

class MockInfluxDB {
    constructor() {
        this.getWriteApi = jest.fn(() => new MockWriteApi());
    }
}

module.exports = {
    InfluxDB: MockInfluxDB,
};
