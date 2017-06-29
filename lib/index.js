/*!
 * Copyright (c) 2016-2017 Digital Bazaar, Inc. All rights reserved.
 */

'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const crypto = require('crypto');
const niUri = require('ni-uri');

const api = {};
module.exports = api;

// register this plugin
bedrock.events.on('bedrock.start', callback => {
  brLedger.use({
    capabilityName: 'UnilateralConsensus2017',
    capabilityValue: {
      type: 'consensus',
      api: api
    }
  }, callback);
});

const blocks = api.blocks = {};

blocks.setConfig = (ledgerNode, configBlock, callback) => {
  async.auto({
    hashBlock: callback => hasher(configBlock, callback),
    writeConfig: ['hashBlock', (results, callback) => {
      const meta = {
        blockHash: results.hashBlock,
        consensus: Date.now()
      };
      const options = {};
      ledgerNode.storage.blocks.add(configBlock, meta, options, callback);
    }]
  }, callback);
};

const events = api.events = {};

events.add = (event, storage, options, callback) => {
  if(typeof options === 'function') {
    callback = options;
    options = {};
  }
  async.auto({
    hashEvent: callback => hasher(event, callback),
    writeEvent: ['hashEvent', (results, callback) => {
      console.log('PPPPPP', results.hashEvent);
      const meta = {
        eventHash: results.hashEvent,
        consensus: 'pending'
      };
      const options = {};
      storage.events.add(event, meta, options, callback);
    }],
    signBlock: ['hashEvent', (results, callback) => {
      callback();
    }],
    writeBlock: ['signBlock', (results, callback) => {
      callback();
    }],
    updateEvent: ['writeBlock', (results, callback) => {
      callback();
    }]
  }, (err, results) => callback(err, results.writeEvent));
};

// FIXME: normalize data
function hasher(data, callback) {
  callback(null, niUri.format({
    algorithm: 'sha-256',
    value: crypto.createHash('sha256').update(JSON.stringify(data))
      .digest('hex')
  }));
}
