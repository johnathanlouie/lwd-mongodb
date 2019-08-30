/* global Promise */

const mongodb = require("mongodb");
const MongoClient = mongodb.MongoClient;

function connect() {
    var options = {
        useNewUrlParser: true,
        useUnifiedTopology: true
    };
    return MongoClient.connect(database.config.url, options);
}

function cloneNoId(document) {
    var newDoc = Object.assign({}, document);
    delete newDoc._id;
    return newDoc;
}

var IdFilter = {};

IdFilter.fromHexString = function (hex) {
    return {_id: mongodb.ObjectId.createFromHexString(hex)};
};

IdFilter.fromDocument = function (document) {
    return IdFilter.fromHexString(document._id);
};

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

database.deleteOne = function (collection, filter) {

    function a(client) {

        function b(result) {

            function c(value) {
                return result;
            }

            return client.close().then(c);
        }

        return client.db().collection(collection).deleteOne(filter).then(b);
    }

    return connect().then(a);
};

database.replaceOne = function (collection, document) {

    function a(client) {

        function b(result) {

            function c(value) {
                return result;
            }

            return client.close().then(c);
        }

        var filter = IdFilter.fromDocument(document);
        var noId = cloneNoId(document);
        return client.db().collection(collection).replaceOne(filter, noId).then(b);
    }

    return connect().then(a);
};

database.getById = function (collection, id, projection) {
    return database.findOne(collection, IdFilter.fromHexString(id), projection);
};

database.deleteById = function (collection, id) {
    return database.deleteOne(collection, IdFilter.fromHexString(id));
};

module.exports = database;