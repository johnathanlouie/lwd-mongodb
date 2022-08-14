const fsPromises = require('fs/promises');
const mongodb = require('mongodb');


/**
 * Throws an error. Useful for nullish coalescing.
 * 
 * @param {?string} message Error message.
 * @returns {void} Never returns.
 * @throws {Error}
 */
function throwError(message = null) {
  throw new Error(message ?? undefined);
}

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
 * @typedef {Object} ConfigFile
 * @property {?string} username
 * @property {?string} password
 * @property {string} host
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
   * @param {ConfigFile} config 
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

  /** @type {ConfigFile} */
  #config;

  /** @type {mongodb.MongoClient} */
  #client;

  /** @type {mongodb.Db} */
  #db;

  /**
   * Constructs the client.
   * 
   * @param {ConfigFile} config Connection settings.
   * @throws {Error}
   */
  constructor(config) {
    this.#config = {
      username: config.username ?? null,
      password: config.password ?? null,
      host: config.host ?? throwError('MongodbClient: Host not found in config'),
      port: config.port ?? null,
      database: config.database ?? throwError('MongodbClient: Database not found in config'),
    };
  }

  /**
   * Loads a client from a configuration file.
   * 
   * @param {string} filepath 
   * @returns {MongodbClient}
   * @throws {Error}
   */
  static async loadConfig(filepath) {
    let config = await fsPromises.readFile(filepath, { encoding: 'utf8' });
    config = JSON.parse(config);
    return new this(config);
  }

  /**
   * Formats a connection string from a data object.
   * 
   * @param {ConfigFile} config 
   * @returns {string} MongoDB connection string.
   */
  static #createConnectionString(config) {
    let auth = config.password === null ? '' : `:${config.password}`;
    auth = config.username === null ? '' : `${config.username}${auth}@`;
    let port = config.port === null ? '' : `:${config.port}`;
    return `mongodb://${auth}${config.host}${port}`;
  }

  async connect() {
    this.#client = await mongodb.MongoClient.connect(new MongodbUrl(this.#config).standardUrl(), {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });
    this.#db = this.#client.db();
  }

  async close() {
    if (this.#client !== null) {
      await this.#client.close();
      this.#client = null;
      this.#db = null;
    }
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} query 
   * @param {*} projection 
   * @returns 
   */
  async findOne(collection, query, projection) {
    return await this.#db.collection(collection).findOne(query, { projection: projection });
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} query 
   * @param {*} projection 
   * @returns 
   */
  async findMany(collection, query, projection) {
    return await this.#db.collection(collection).find(query, { projection: projection }).toArray();
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} data 
   * @returns 
   */
  async insertOne(collection, data) {
    return await this.#db.collection(collection).insertOne(data);
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} data 
   * @returns 
   */
  async insertMany(collection, data) {
    return await this.#db.collection(collection).insertMany(data);
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} filter 
   * @returns 
   */
  async deleteOne(collection, filter) {
    return await this.#db.collection(collection).deleteOne(filter);
  }

  /**
   * 
   * @param {string} collection 
   * @param {*} document 
   * @returns 
   */
  async replaceOne(collection, document) {
    let filter = IdFilter.fromDocument(document);
    let noId = cloneNoId(document);
    return await this.#db.collection(collection).replaceOne(filter, noId);
  }

  /**
   * 
   * @param {string} collection 
   * @param {string} id 
   * @param {*} projection 
   * @returns 
   */
  getById(collection, id, projection) {
    return this.findOne(collection, IdFilter.fromHexString(id), projection);
  }

  /**
   * 
   * @param {string} collection 
   * @param {string} id 
   * @returns 
   */
  deleteById(collection, id) {
    return this.deleteOne(collection, IdFilter.fromHexString(id));
  }

}


module.exports = MongodbClient;
