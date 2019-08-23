/* global Promise */

const MongoClient = require("mongodb").MongoClient;

function connect() {
    return Promise.resolve(MongoClient.connect(database.config.url));
}

var database = {};

database.setConfig = function (config) {
    database.config = config;
};

database.findOne = function (collection, callback, query, projection) {

    function fail(error) {
        callback(error);
    }

    function a(client) {

        function b(result) {
            callback(undefined, result);
        }

        function c() {
            client.close();
        }

        return client.db().collection(collection).findOne(query, {projection: projection}).then(b).catch(fail).then(c);
    }

    connect().then(a, fail);
};

database.findMany = function (collection, callback, query, projection) {

    function a1(client) {

        function b(error, documents) {
            if (error) {
                callback(error);
            } else {
                callback(undefined, documents);
            }
            client.close();
        }

        client.db().collection(collection).find(query).project(projection).toArray(b);
    }

    function a2(error) {
        callback(error);
    }

    connect().then(a1, a2);
};

database.insertOne = function (collection, callback, data) {

    function a1(client) {

        function b1(result) {
            callback(undefined, result);
        }

        function b2(error) {
            callback(error);
        }

        function c() {
            return client.close();
        }

        return client.db().collection(collection).insertOne(data).then(b1, b2).then(c);
    }

    function a2(error) {
        callback(error);
    }

    connect().then(a1, a2);
};

database.insertMany = function (collection, callback, data) {

    function a1(client) {

        function b1(result) {
            callback(undefined, result);
        }

        function b2(error) {
            callback(error);
        }

        function c() {
            return client.close();
        }

        return client.db().collection(collection).insertMany(data).then(b1, b2).then(c);
    }

    function a2(error) {
        callback(error);
    }

    connect().then(a1, a2);
};

module.exports = database;