/*!
 * Copyright (c) 2016-2017 Digital Bazaar, Inc. All rights reserved.
 */

'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const jsonld = bedrock.jsonld;
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

blocks.setConfig = (ledgerNode, configEvent, callback) => {
  const configBlock = {
    '@context': 'https://w3id.org/webledger/v1',
    // FIXME: This should be generated based on the latest block number
    id: configEvent.input[0].ledger + '/blocks/1',
    type: 'WebLedgerEventBlock',
    event: [configEvent]
  };

  async.auto({
    hashEvent: callback => {
      hasher(configEvent, callback);
    },
    hashBlock: callback => {
      // FIXME: add previousBlock and previousBlock hash here, what values?
      hasher(configBlock, callback);
    },
    writeConfigEvent: ['hashEvent', (results, callback) => {
      const meta = {
        eventHash: results.hashEvent,
        consensus: true
      };
      const options = {};
      ledgerNode.storage.events.add(configEvent, meta, options, callback);
    }],
    writeConfigBlock: ['hashBlock', (results, callback) => {
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
          // if duplicate block, reconstruct the block and try again
          if(isDuplicateError(err)) {
            return loopCallback();
          }
          loopCallback(err);
        });
      }, callback);
    }],
    updateEvent: ['addBlock', (results, callback) => {
      storage.events.update(results.hashEvent, [{
        op: 'delete',
        changes: {
          meta: {
            pending: true
          }
        }
      }, {
        op: 'add',
        changes: {
          meta: {
            consensus: true
          }
        }
      }], callback);
    }]
  }, (err, results) => callback(err, results.writeEvent));
};

function hasher(data, callback) {
  async.auto({
    // normalize ledger event to nquads
    normalize: callback => jsonld.normalize(data, {
      algorithm: 'URDNA2015',
      format: 'application/nquads'
    }, callback),
    hash: ['normalize', (results, callback) =>
      callback(null, niUri.digest('sha-256', results.normalize, true))
    ]
  }, (err, results) => callback(err, results.hash));
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
