/* global Promise */

const MongoClient = require("mongodb").MongoClient;

function connect() {
    return MongoClient.connect(database.config.url);
}

var database = {};

database.setConfig = function (config) {
    database.config = config;
};

database.findOne = function (collection, query, projection) {
    var findOneOptions = {projection: projection};

    function a(client) {

        function b(document) {

            function c(value) {
                return document;
            }

            return client.close().then(c);
        }

        return client.db().collection(collection).findOne(query, findOneOptions).then(b);
    }

    return connect().then(a);
};

database.findMany = function (collection, query, projection) {
    var findOptions = {projection: projection};

    function a(client) {
        var cursor = client.db().collection(collection).find(query, findOptions);

        function b(documents) {

            function d(value) {
                return documents;
            }

            function c() {
                client.close();
            }

            return cursor.close().then(c).then(d);
        }

        return cursor.toArray().then(b);
    }

    return connect().then(a);
};

database.insertOne = function (collection, data) {

    function a(client) {

        function b(result) {

            function c(value) {
                return result;
            }

            return client.close().then(c);
        }

        return client.db().collection(collection).insertOne(data).then(b);
    }

    return connect().then(a);
};

database.insertMany = function (collection, data) {

    function a(client) {

        function b(result) {

            function c(value) {
                return result;
            }

            return client.close().then(c);
        }

        return client.db().collection(collection).insertMany(data).then(b);
    }

    return connect().then(a);
};

module.exports = database;