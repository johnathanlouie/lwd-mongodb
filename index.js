const fs = require('fs');
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
 * @property {?string} username
 * @property {?string} password
 * @property {string} hostname
 * @property {?number} port
 * @property {string} database
 */


class MongodbUrl {

  #username;
  #password;
  #hostname;
  #port;
  #database;

  /**
   * 
   * @param {MongodbConfig} config 
   */
  constructor(config) {
    this.#username = config.username;
    this.#password = config.password;
    this.#hostname = config.hostname;
    this.#port = config.port;
    this.#database = config.database;
  }

  #credentialsUrl() {
    if (this.#username && this.#password) {
      return `${this.#username}:${this.#password}@`;
    } else if (this.#username) {
      return `${this.#username}@`;
    } else {
      return '';
    }
  }

  #portUrl() {
    if (this.#port) {
      return `:${this.#port}`;
    }
    return '';
  }

  #url() {
    return `${this.#credentialsUrl()}${this.#hostname}${this.#portUrl()}/${this.#database}`;
  }

  standardUrl() {
    return `mongodb://${this.#url()}`;
  }

  dnsSrvUrl() {
    return `mongodb+srv://${this.#url()}`;
  }

}


class MongodbClient {

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
    MongodbClient.#config = config;
  }

  /**
   * 
   * @param {string} filepath 
   */
  static readConfig(filepath) {
    MongodbClient.#config = JSON.parse(fs.readFileSync(filepath));
  }

  static async connect() {
    MongodbClient.#client = await mongodb.MongoClient.connect(new MongodbUrl(MongodbClient.#config).standardUrl(), {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    MongodbClient.#db = MongodbClient.#client.db();
  }

  static async close() {
    if (MongodbClient.#client !== null) {
      await MongodbClient.#client.close();
      MongodbClient.#client = null;
      MongodbClient.#db = null;
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
    return await MongodbClient.#db.collection(collection).findOne(query, { projection: projection });
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} query 
   * @param {*} projection 
   * @returns 
   */
  static async findMany(collection, query, projection) {
    return await MongodbClient.#db.collection(collection).find(query, { projection: projection }).toArray();
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} data 
   * @returns 
   */
  static async insertOne(collection, data) {
    return await MongodbClient.#db.collection(collection).insertOne(data);
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} data 
   * @returns 
   */
  static async insertMany(collection, data) {
    return await MongodbClient.#db.collection(collection).insertMany(data);
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} filter 
   * @returns 
   */
  static async deleteOne(collection, filter) {
    return await MongodbClient.#db.collection(collection).deleteOne(filter);
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
    return await MongodbClient.#db.collection(collection).replaceOne(filter, noId);
  }

  /**
   * 
   * @param {string} collection 
   * @param {string} id 
   * @param {*} projection 
   * @returns 
   */
  static getById(collection, id, projection) {
    return MongodbClient.findOne(collection, IdFilter.fromHexString(id), projection);
  }

  /**
   * 
   * @param {string} collection 
   * @param {string} id 
   * @returns 
   */
  static deleteById(collection, id) {
    return MongodbClient.deleteOne(collection, IdFilter.fromHexString(id));
  }

}


module.exports = MongodbClient;
