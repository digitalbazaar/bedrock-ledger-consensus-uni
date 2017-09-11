/*!
 * Copyright (c) 2016-2017 Digital Bazaar, Inc. All rights reserved.
 */

'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');

const api = {};
module.exports = api;

// register this plugin
bedrock.events.on('bedrock.start', () => {
  brLedgerNode.use('UnilateralConsensus2017', {
    type: 'consensus',
    api: api
  });
});

const events = api.events = {};

events.add = (event, ledgerNode, options, callback) => {
  if(typeof options === 'function') {
    callback = options;
    options = {};
  }
  const storage = ledgerNode.storage;
  async.auto({
    hashEvent: callback => brLedgerNode.consensus._hasher(event, callback),
    writeEvent: ['hashEvent', (results, callback) => {
      const meta = {
        eventHash: results.hashEvent,
        pending: true
      };
      const options = {};
      storage.events.add(event, meta, options, callback);
    }],
    addBlock: ['writeEvent', (resultsAlpha, callback) => {
      if(options.genesis) {
        return _writeGenesisBlock(event, storage, callback);
      }
      let done = false;
      // FIXME: implement max retries
      async.until(() => done, loopCallback => async.auto({
        getConfig: callback => storage.events.getLatestConfig(callback),
        validate: ['getConfig', (results, callback) =>
          brLedgerNode.consensus._validateEvent(
            event, results.getConfig.event.ledgerConfiguration.eventValidator, {
              requireEventValidation: results.getConfig.event
                .ledgerConfiguration.requireEventValidation
            },
            callback)],
        getLatestBlock: ['validate', (results, callback) =>
          storage.blocks.getLatest(options, callback)],
        buildBlock: ['getLatestBlock', (results, callback) => {
          const latestEventBlock = results.getLatestBlock.eventBlock;
          const block = {
            '@context': 'https://w3id.org/webledger/v1',
            id: incrementBlock(latestEventBlock.block.id),
            blockHeight: getBlockHeight(latestEventBlock.block.id),
            type: 'WebLedgerEventBlock',
            event: [resultsAlpha.hashEvent],
            previousBlock: latestEventBlock.block.id,
            previousBlockHash: latestEventBlock.meta.blockHash
          };
          callback(null, block);
        }],
        hashBlock: ['buildBlock', (results, callback) =>
          brLedgerNode.consensus._hasher(results.buildBlock, callback)
        ],
        writeBlock: ['hashBlock', (results, callback) => storage.blocks.add(
          results.buildBlock, {
            blockHash: results.hashBlock,
            consensus: true,
            consensusDate: Date.now()
          }, callback)]
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
      }), callback);
    }],
    updateEvent: ['addBlock', (results, callback) => {
      storage.events.update(results.hashEvent, [{
        op: 'unset',
        changes: {
          meta: {
            pending: true
          }
        }
      }, {
        op: 'set',
        changes: {
          meta: {
            consensus: true,
            consensusDate: Date.now(),
            updated: Date.now()
          }
        }
      }], callback);
    }]
  }, (err, results) => callback(err, results.writeEvent));
};

// FIXME: better algo?
function incrementBlock(blockId) {
  const i = blockId.lastIndexOf('/') + 1;
  const b = Number(blockId.substring(i)) + 1;
  return blockId.substring(0, i) + b;
}

function getBlockHeight(blockId) {
  const i = blockId.lastIndexOf('/') + 1;
  const b = Number(blockId.substring(i));
  return b;
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

function _writeGenesisBlock(event, storage, callback) {
  const configBlock = {
    '@context': 'https://w3id.org/webledger/v1',
    // FIXME: This should be generated based on the latest block number
    id: event.ledgerConfiguration.ledger + '/blocks/1',
    blockHeight: 0,
    type: 'WebLedgerEventBlock',
    event: [event]
  };
  const requireEventValidation =
    event.ledgerConfiguration.requireEventValidation || false;
  async.auto({
    validate: callback => brLedgerNode.consensus._validateEvent(
      event, event.ledgerConfiguration.eventValidator, {requireEventValidation},
      callback),
    hashBlock: callback => brLedgerNode.consensus._hasher(
      configBlock, callback),
    writeBlock: ['hashBlock', (results, callback) => {
      const meta = {
        blockHash: results.hashBlock,
        consensus: true,
        consensusDate: Date.now()
      };
      const options = {};
      storage.blocks.add(configBlock, meta, options, callback);
    }]
  }, callback);
}
