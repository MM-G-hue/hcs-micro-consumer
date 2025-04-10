module.exports = {
    moduleNameMapper: {
        "^influxdata/influxdb-client$": "<rootDir>/test/__mocks__/influxdb.js",
        "^amqplib/callback_api$": "<rootDir>/test/__mocks__/amqplib.js",
    },
};
