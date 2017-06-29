/*!
 * Copyright (c) 2016-2017 Digital Bazaar, Inc. All rights reserved.
 */

'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
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
      console.log('MMMMMMMMMMMMMMSET', meta);
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
      const meta = {
        eventHash: results.hashEvent,
        // NOTE: WILL BE SETTING `consensus = date.now()` later
        pending: true
      };
      const options = {};
      storage.events.add(event, meta, options, callback);
    }],
    addBlock: ['hashEvent', (resultsAlpha, callback) => {
      let done = false;
      async.until(() => done, loopCallback => {
        async.auto({
          getLatest: callback => {
            const options = {};
            storage.blocks.getLatest(options, callback);
          },
          signBlock: ['getLatest', (results, callback) => {
            console.log('55555555555', resultsAlpha.hashEvent);
            console.log('6666666666', results.getLatest.eventBlock);
            const latestEventBlock = results.getLatest.eventBlock;
            const block = {
              '@context': 'https://w3id.org/webledger/v1',
              type: 'WebLedgerEventBlock',
              event: [resultsAlpha.hashEvent],
              previousBlock: latestEventBlock.block.id,
              previousBlockHash: latestEventBlock.meta.blockHash
            };
            console.log('BBBBBBBB', block);
            // TODO: sign block
            callback(null, event);
          }],
          writeBlock: ['signBlock', (results, callback) => {
            // storage.blocks.add()
            callback();
          }]
        }, err => {
          if(!err) {
            done = true;
            return loopCallback();
          }
          // TODO: look for database is duplicate and just call loopback
          callback(err);
        });
      }, callback);
    }],
    updateEvent: ['addBlock', (results, callback) => {
      callback();
    }]
  }, (err, results) => callback(err, results.writeEvent));
};

// FIXME: normalize data
function hasher(data, callback) {
  callback(null, niUri.digest('sha-256', JSON.stringify(data), true));
}
