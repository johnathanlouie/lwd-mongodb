const mongodb = require('mongodb');


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

  static #config = null;
  static #client = null;
  static #db = null;

  static setConfig(config) {
    Database.#config = config;
  }

  static async connect() {
    Database.#client = await mongodb.MongoClient.connect(Database.#config.url, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    Database.#db = Database.#client.db();
  }

  static async close() {
    if (Database.#client !== null) {
      await Database.#client.close();
      Database.#client = null;
      Database.#db = null;
    }
  }

  static async findOne(collection, query, projection) {
    return await Database.#db.collection(collection).findOne(query, { projection: projection });
  }

  static async findMany(collection, query, projection) {
    return await Database.#db.collection(collection).find(query, { projection: projection }).toArray();
  }

  static async insertOne(collection, data) {
    return await Database.#db.collection(collection).insertOne(data);
  }

  static async insertMany(collection, data) {
    return await Database.#db.collection(collection).insertMany(data);
  }

  static async deleteOne(collection, filter) {
    return await Database.#db.collection(collection).deleteOne(filter);
  }

  static async replaceOne(collection, document) {
    let filter = IdFilter.fromDocument(document);
    let noId = cloneNoId(document);
    return await Database.#db.collection(collection).replaceOne(filter, noId);
  }

  static getById(collection, id, projection) {
    return Database.findOne(collection, IdFilter.fromHexString(id), projection);
  }

  static deleteById(collection, id) {
    return Database.deleteOne(collection, IdFilter.fromHexString(id));
  }

}


module.exports = Database;
