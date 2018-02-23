/*!
 * Copyright (c) 2016-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const uuid = require('uuid/v4');

const constants = {
  WEB_LEDGER_CONTEXT_V1_URL: 'https://w3id.org/webledger/v1'
};

const api = {};
module.exports = api;

// register this plugin
bedrock.events.on('bedrock.start', () => {
  brLedgerNode.use('UnilateralConsensus2017', {
    type: 'consensus',
    api: api
  });
});

api.config = {};

// NOTE: no application for `genesisBlock` here
api.config.change = ({genesis, ledgerConfiguration, ledgerNode}, callback) => {
  const event = {
    '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerConfigurationEvent',
    ledgerConfiguration
  };
  _addEvent({event, genesis, ledgerNode}, err => callback(err));
};

api.operations = {};
api.operations.add = ({operation, ledgerNode}, callback) => {
  const event = {
    '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerOperationEvent',
    operation: [operation]
  };
  _addEvent({event, ledgerNode}, err => callback(err));
};

function _addEvent({event, genesis, ledgerNode}, callback) {
  event.nonce = uuid();
  const storage = ledgerNode.storage;
  async.auto({
    eventHash: callback => brLedgerNode.consensus._hasher(event, callback),
    writeEvent: ['eventHash', (results, callback) => {
      const meta = {
        eventHash: results.eventHash,
        pending: true
      };
      storage.events.add({event, meta}, callback);
    }],
    addBlock: ['writeEvent', (resultsAlpha, callback) => {
      const {eventHash} = resultsAlpha;
      if(genesis) {
        return _writeGenesisBlock({event, eventHash, storage}, callback);
      }
      let done = false;
      // FIXME: implement max retries
      async.until(() => done, loopCallback => async.auto({
        getLatest: callback => storage.blocks.getLatest(callback),
        buildBlock: ['getLatest', (results, callback) => {
          const latestEventBlock = results.getLatest.eventBlock;
          const block = {
            '@context': 'https://w3id.org/webledger/v1',
            id: incrementBlock(latestEventBlock.block.id),
            blockHeight: getBlockHeight(latestEventBlock.block.id),
            type: 'WebLedgerEventBlock',
            event: [eventHash],
            previousBlock: latestEventBlock.block.id,
            previousBlockHash: latestEventBlock.meta.blockHash
          };
          callback(null, block);
        }],
        blockHash: ['buildBlock', (results, callback) =>
          brLedgerNode.consensus._hasher(results.buildBlock, callback)
        ],
        writeBlock: ['blockHash', (results, callback) => storage.blocks.add({
          block: results.buildBlock,
          meta: {
            blockHash: results.blockHash,
            consensus: true,
            consensusDate: Date.now()
          }
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
      const {eventHash} = results;
      const patch = [{
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
      }];
      storage.events.update({eventHash, patch}, callback);
    }]
  }, (err, results) => callback(err, results.writeEvent));
}

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
  return err.name === 'DuplicateError';
}

function _writeGenesisBlock({event, eventHash, storage}, callback) {
  const block = {
    '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
    // FIXME: This should be generated based on the latest block number
    id: event.ledgerConfiguration.ledger + '/blocks/1',
    blockHeight: 0,
    type: 'WebLedgerEventBlock',
    event: [event]
  };
  async.auto({
    blockHash: callback => brLedgerNode.consensus._hasher(block, callback),
    writeBlock: ['blockHash', (results, callback) => {
      const meta = {
        blockHash: results.blockHash,
        consensus: true,
        consensusDate: Date.now()
      };
      block.event = [eventHash];
      storage.blocks.add({block, meta}, callback);
    }]
  }, callback);
}
