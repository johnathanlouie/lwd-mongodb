/* global Promise */

const mongodb = require('mongodb');
const MongoClient = mongodb.MongoClient;


function connect() {
  return MongoClient.connect(Database.config.url, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
}


function cloneNoId(document) {
  var newDoc = Object.assign({}, document);
  delete newDoc._id;
  return newDoc;
}


class IdFilter {

  static fromHexString(hex) {
    return { _id: mongodb.ObjectId.createFromHexString(hex) };
  }

  static fromDocument(document) {
    return IdFilter.fromHexString(document._id);
  }

}


class Database {

  static config;

  static setConfig(config) {
    Database.config = config;
  }

  static findOne(collection, query, projection) {
    var findOneOptions = { projection: projection };

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
  }

  static findMany(collection, query, projection) {
    var findOptions = { projection: projection };

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
  }

  static insertOne(collection, data) {

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
  }

  static insertMany(collection, data) {

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
  }

  static deleteOne(collection, filter) {

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
  }

  static replaceOne(collection, document) {

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
  }

  static getById(collection, id, projection) {
    return Database.findOne(collection, IdFilter.fromHexString(id), projection);
  }

  static deleteById(collection, id) {
    return Database.deleteOne(collection, IdFilter.fromHexString(id));
  }

}


module.exports = Database;
