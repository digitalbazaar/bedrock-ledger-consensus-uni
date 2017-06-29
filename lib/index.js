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
    hashBlock: callback => {
      // FIXME: add previousBlock and previousBlock hash here, what values?
      hasher(configBlock, callback);
    },
    writeConfig: ['hashBlock', (results, callback) => {
      const meta = {
        blockHash: results.hashBlock,
        consensus: true
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
          buildBlock: ['getLatest', (results, callback) => {
            const latestEventBlock = results.getLatest.eventBlock;
            const block = {
              '@context': 'https://w3id.org/webledger/v1',
              id: incrementBlock(latestEventBlock.block.id),
              type: 'WebLedgerEventBlock',
              event: [resultsAlpha.hashEvent],
              previousBlock: latestEventBlock.block.id,
              previousBlockHash: latestEventBlock.meta.blockHash
            };
            callback(null, block);
          }],
          hashBlock: ['buildBlock', (results, callback) =>
            hasher(results.buildBlock, callback)
          ],
          signBlock: ['buildBlock', (results, callback) => {
            // TODO: sign block
            callback(null, results.buildBlock);
          }],
          writeBlock: ['hashBlock', 'signBlock', (results, callback) => {
            const meta = {
              blockHash: results.hashBlock,
              consensus: true
            };
            const options = {};
            storage.blocks.add(results.signBlock, meta, options, callback);
          }]
        }, err => {
          if(!err) {
            done = true;
            return loopCallback();
          }
          // if duplicate block, try again
          if(isDuplicateError(err)) {
            return loopCallback();
          }
          loopCallback(err);
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

// FIXME: better algo?
function incrementBlock(blockId) {
  const i = blockId.lastIndexOf('/') + 1;
  const b = Number(blockId.substring(i)) + 1;
  return blockId.substring(0, i) + b;
}

/**
 * Returns true if the given error is a duplicate block error.
 *
 * @param err the error to check.
 *
 * @return true if the error is a duplicate block error, false if not.
 */
function isDuplicateError(err) {
  return err.name === 'DuplicateBlock';
}
