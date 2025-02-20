'use strict';

var assert = require('assert');
var superagent = require('superagent');
var mongodb = require('mongodb');
const { log } = require('console');

/**
 *  This module exports a single function which takes an instance of connect
 *  (or Express) and returns a `MongoDBStore` class that can be used to
 *  store sessions in MongoDB.
 */
describe('MongoDBStore', function() {
  var underlyingDb;
  var server;

  beforeEach(async function() {
//    log('before each');
    const client = await mongodb.MongoClient.connect(
      'mongodb://127.0.0.1:27017/connect_mongodb_session_test',
      { serverSelectionTimeoutMS: 5000 },{ useUnifiedTopology: true}, { useNewUrlParser: true }, { connectTimeoutMS: 30000 }, { keepAlive: 1}
    );
    underlyingDb = await client.db('connect_mongodb_session_test');
//    log('delete Many');
    await client.db('connect_mongodb_session_test').collection('mySessions').deleteMany({});
//    log('delete many done');
    await new Promise(resolve => setTimeout(resolve, 200));
//    console.log("---BEFORE EACH ---------");
  });

  afterEach(async function() {
    server && server.close();
    await new Promise(resolve => setTimeout(resolve, 100));
//    await console.log("---Drop all indexes ---------");
    await underlyingDb.collection('mySessions').dropIndexes();
    await new Promise(resolve => setTimeout(resolve, 100));
//    await console.log("---AFTER EACH ---------");
  });

  /**
   *  If you pass in an instance of the
   *  [`express-session` module](http://npmjs.org/package/express-session)
   *  the MongoDBStore class will enable you to store your Express sessions
   *  in MongoDB.
   *
   *  The MongoDBStore class has 3 required options:
   *
   *  1. `uri`: a [MongoDB connection string](http://docs.mongodb.org/manual/reference/connection-string/)
   *  2. `databaseName`: the MongoDB database to store sessions in
   *  3. `collection`: the MongoDB collection to store sessions in
   *
   *  **Note:** You can pass a callback to the `MongoDBStore` constructor,
   *  but this is entirely optional. The Express 3.x example demonstrates
   *  that you can use the MongoDBStore class in a synchronous-like style: the
   *  module will manage the internal connection state for you.
   */
  it('can store sessions for Express 4', async function() {
    var express = require('express');
    var session = require('express-session');
    var MongoDBStore = require('connect-mongodb-session')(session);

    var app = express();
    var store = new MongoDBStore({
      uri: 'mongodb://127.0.0.1:27017/connect_mongodb_session_test',
      collection: 'mySessions'
    });
    await new Promise(resolve => setTimeout(resolve, 100));
//    console.log("---MongoDBStore ---------");
    // acquit:ignore:start

    store.on('connected', function() {
      store.client; // The underlying MongoClient object from the MongoDB driver
      assert.ok(store.client);
      assert.ok(store.db);
    });
    // acquit:ignore:end

    // Catch errors
    store.on('error', function(error) {
      console.log('error '+error);
      // acquit:ignore:start
      assert.ifError(error);
	    assert.ok(false);
      // acquit:ignore:end
    });

    app.use(session({
      secret: 'This is a secret',
      cookie: {
        maxAge: 1000 * 60 * 60 * 24 * 7 // 1 week
      },
      store: store,
      // Boilerplate options, see:
      // * https://www.npmjs.com/package/express-session#resave
      // * https://www.npmjs.com/package/express-session#saveuninitialized
      resave: true,
      saveUninitialized: true
    }));

    app.get('/', function(req, res) {
      res.send('Hello ' + JSON.stringify(req.session));
    });

    server = app.listen(3000, function() {
      console.log('ready to go!');});

    await new Promise(resolve => setTimeout(resolve, 50));
//    console.log("----------------------");

    let count = await underlyingDb.collection('mySessions').countDocuments({});
//    log('count '+count)
    assert.equal(0, count);
//    log('get express / endpoint')
    let response = await superagent.get('http://127.0.0.1:3000');
//    log('cookie count '+response.headers['set-cookie'].length)
    assert.equal(1, response.headers['set-cookie'].length);
    var cookie = require('cookie').parse(response.headers['set-cookie'][0]);
    assert.ok(cookie['connect.sid']);
    count = await underlyingDb.collection('mySessions').countDocuments({});
//    log('count session db '+count)    
    assert.equal(count, 1);
//    log('get express / endpoint with cookie set')
    response = await superagent.get('http://127.0.0.1:3000').set('Cookie', 'connect.sid=' + cookie['connect.sid']);
    assert.ok(!response.headers['set-cookie']);
//    log('clearing store');
    await store.clear();
    await new Promise(resolve => setTimeout(resolve, 500));
//    console.log("----------------------");
//    log('store clear');
    count = await underlyingDb.collection('mySessions').countDocuments({});
//    log('count '+count)
    assert.equal(count, 0);
  });

  /**
   *  You should pass a callback to the `MongoDBStore` constructor to catch
   *  errors. If you don't pass a callback to the `MongoDBStore` constructor,
   *  `MongoDBStore` will `throw` if it can't connect.
   */
  it('throws an error when it can\'t connect to MongoDB', function(done) {
    var express = require('express');
    var session = require('express-session');
    var MongoDBStore = require('connect-mongodb-session')(session);

    var app = express();
    var numExpectedSources = 2;
    var store = new MongoDBStore(
      {
        uri: 'mongodb://bad.host:27000/connect_mongodb_session_test?serverSelectionTimeoutMS=100',
        databaseName: 'connect_mongodb_session_test',
        collection: 'mySessions'
      },
      function(error) {
        // Should have gotten an error
        // acquit:ignore:start
        assert.ok(error);
        --numExpectedSources || done();
        // acquit:ignore:end
      });

    store.on('error', function(error) {
      // Also get an error here
      // acquit:ignore:start
      assert.ok(error);
      --numExpectedSources || done();
      // acquit:ignore:end
    });

    app.use(session({
      secret: 'This is a secret',
      cookie: {
        maxAge: 1000 * 60 * 60 * 24 * 7 // 1 week
      },
      store: store,
      // Boilerplate options, see:
      // * https://www.npmjs.com/package/express-session#resave
      // * https://www.npmjs.com/package/express-session#saveuninitialized
      resave: true,
      saveUninitialized: true
    }));

    app.get('/', function(req, res) {
      res.send('Hello ' + JSON.stringify(req.session));
    });

    server = app.listen(3000, function() {
      console.log('ready to go!');});
  });

  /**
   * There are several other options you can pass to `new MongoDBStore()`:
   */
  it('supports several other options', function() {
    var express = require('express');
    var session = require('express-session');
    var MongoDBStore = require('connect-mongodb-session')(session);

    var store = new MongoDBStore({
      uri: 'mongodb://127.0.0.1:27017/connect_mongodb_session_test',
      collection: 'mySessions',

      // By default, sessions expire after 2 weeks. The `expires` option lets
      // you overwrite that by setting the expiration in milliseconds
      expires: 1000 * 60 * 60 * 24 * 30, // 30 days in milliseconds

      // Lets you set options passed to `MongoClient.connect()`. Useful for
      // configuring connectivity or working around deprecation warnings.
      connectionOptions: {
        serverSelectionTimeoutMS: 10000
      }
    });
  });

  /**
   * There are several other options you can pass to `new MongoDBStore()`:
   */
  it('test forceExpire false options', function() {
    var express = require('express');
    var session = require('express-session');
    var MongoDBStore = require('connect-mongodb-session')(session);

    var store = new MongoDBStore({
      uri: 'mongodb://127.0.0.1:27017/connect_mongodb_session_test',
      collection: 'mySessions',
      expiresAfterSeconds: 60 * 60 * 24 * 30,
      forceIndex: false,

      // By default, sessions expire after 2 weeks. The `expires` option lets
      // you overwrite that by setting the expiration in milliseconds
      expires: 1000 * 60 * 60 * 24 * 30, // 30 days in milliseconds

      // Lets you set options passed to `MongoClient.connect()`. Useful for
      // configuring connectivity or working around deprecation warnings.
      connectionOptions: {
        serverSelectionTimeoutMS: 10000
      }
    });
  });

  /**
   * There are several other options you can pass to `new MongoDBStore()`:
   */
  it('test forceExpire true options while creating 2 stores', async function() {
    var express = require('express');
    var session = require('express-session');
    var MongoDBStore = require('connect-mongodb-session')(session);

    var store = await new MongoDBStore({
      uri: 'mongodb://127.0.0.1:27017/connect_mongodb_session_test',
      collection: 'mySessions',
      expiresAfterSeconds: 60 * 60 * 24 * 30,
      forceIndex: true,

      // By default, sessions expire after 2 weeks. The `expires` option lets
      // you overwrite that by setting the expiration in milliseconds
      expires: 1000 * 60 * 60 * 24 * 30, // 30 days in milliseconds

      // Lets you set options passed to `MongoClient.connect()`. Useful for
      // configuring connectivity or working around deprecation warnings.
      connectionOptions: {
        serverSelectionTimeoutMS: 10000
      }
    });

    log('wait before creating a new store');
    await new Promise(resolve => setTimeout(resolve, 1500));
    log('creating a new store');

    var store = await new MongoDBStore({
      uri: 'mongodb://127.0.0.1:27017/connect_mongodb_session_test',
      collection: 'mySessions',
      expiresAfterSeconds: 60 * 60 * 24 * 60,
      forceIndex: true,

      // By default, sessions expire after 2 weeks. The `expires` option lets
      // you overwrite that by setting the expiration in milliseconds
      expires: 1000 * 60 * 60 * 24 * 30, // 30 days in milliseconds

      // Lets you set options passed to `MongoClient.connect()`. Useful for
      // configuring connectivity or working around deprecation warnings.
      connectionOptions: {
        serverSelectionTimeoutMS: 10000
      }
    });
  });
});
