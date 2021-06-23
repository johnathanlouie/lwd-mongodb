/* global Promise */

const mongodb = require('mongodb');
const MongoClient = mongodb.MongoClient;


async function connect() {
  return await MongoClient.connect(Database.config.url, {
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

  static async findOne(collection, query, projection) {
    try {
      let findOneOptions = { projection: projection };
      var client = await connect();
      let result = await client.db().collection(collection).findOne(query, findOneOptions);
      return result;
    } finally {
      client.close();
    }
  }

  static async findMany(collection, query, projection) {
    try {
      let findOptions = { projection: projection };
      var client = await connect();
      var cursor = client.db().collection(collection).find(query, findOptions);
      let documents = await cursor.toArray();
      return documents;
    } finally {
      cursor.close();
      client.close();
    }
  }

  static async insertOne(collection, data) {
    try {
      var client = await connect();
      let result = await client.db().collection(collection).insertOne(data);
      return result;
    } finally {
      client.close();
    }
  }

  static async insertMany(collection, data) {
    try {
      var client = await connect();
      let result = await client.db().collection(collection).insertMany(data);
      return result;
    } finally {
      client.close();
    }
  }

  static async deleteOne(collection, filter) {
    try {
      var client = await connect();
      let result = await client.db().collection(collection).deleteOne(filter);
      return result;
    } finally {
      client.close();
    }
  }

  static async replaceOne(collection, document) {
    try {
      var client = await connect();
      let filter = IdFilter.fromDocument(document);
      let noId = cloneNoId(document);
      let result = await client.db().collection(collection).replaceOne(filter, noId);
      return result;
    } finally {
      client.close();
    }
  }

  static getById(collection, id, projection) {
    return Database.findOne(collection, IdFilter.fromHexString(id), projection);
  }

  static deleteById(collection, id) {
    return Database.deleteOne(collection, IdFilter.fromHexString(id));
  }

}


module.exports = Database;
