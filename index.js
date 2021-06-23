const mongodb = require('mongodb');


function cloneNoId(document) {
  var newDoc = Object.assign({}, document);
  delete newDoc._id;
  return newDoc;
}


class IdFilter {

  /**
   * 
   * @param {string} hex 
   * @returns 
   */
  static fromHexString(hex) {
    return { _id: mongodb.ObjectId.createFromHexString(hex) };
  }

  static fromDocument(document) {
    return IdFilter.fromHexString(document._id);
  }

}

/**
 * @typedef {Object} MongodbConfig
 * @property {string} url
 */


class Database {

  /** @type {MongodbConfig} */
  static #config = null;

  /** @type {mongodb.MongoClient} */
  static #client = null;

  /** @type {mongodb.Db} */
  static #db = null;

  /**
   * 
   * @param {MongodbConfig} config 
   */
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

  /**
   * 
   * @param {string} collection 
   * @param {*} query 
   * @param {*} projection 
   * @returns 
   */
  static async findOne(collection, query, projection) {
    return await Database.#db.collection(collection).findOne(query, { projection: projection });
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} query 
   * @param {*} projection 
   * @returns 
   */
  static async findMany(collection, query, projection) {
    return await Database.#db.collection(collection).find(query, { projection: projection }).toArray();
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} data 
   * @returns 
   */
  static async insertOne(collection, data) {
    return await Database.#db.collection(collection).insertOne(data);
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} data 
   * @returns 
   */
  static async insertMany(collection, data) {
    return await Database.#db.collection(collection).insertMany(data);
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} filter 
   * @returns 
   */
  static async deleteOne(collection, filter) {
    return await Database.#db.collection(collection).deleteOne(filter);
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} document 
   * @returns 
   */
  static async replaceOne(collection, document) {
    let filter = IdFilter.fromDocument(document);
    let noId = cloneNoId(document);
    return await Database.#db.collection(collection).replaceOne(filter, noId);
  }

  /**
   * 
   * @param {string} collection 
   * @param {string} id 
   * @param {*} projection 
   * @returns 
   */
  static getById(collection, id, projection) {
    return Database.findOne(collection, IdFilter.fromHexString(id), projection);
  }

  /**
   * 
   * @param {string} collection 
   * @param {string} id 
   * @returns 
   */
  static deleteById(collection, id) {
    return Database.deleteOne(collection, IdFilter.fromHexString(id));
  }

}


module.exports = Database;
