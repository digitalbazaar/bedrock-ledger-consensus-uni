/*!
 * Copyright (c) 2016-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const {constants} = bedrock.config;
const database = require('bedrock-mongodb');
const uuid = require('uuid/v4');
require('bedrock-ledger-context');

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
api.operations.add = ({ledgerNode, operation}, callback) => {
  const event = {
    '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerOperationEvent',
  };
  async.auto({
    operationHash: callback => brLedgerNode.consensus._hasher(
      operation, callback),
    eventHash: ['operationHash', (results, callback) => {
      const {operationHash} = results;
      event.operationHash = [operationHash];
      brLedgerNode.consensus._hasher(event, callback);
    }],
    storeOperation: ['eventHash', (results, callback) => {
      const {eventHash, operationHash} = results;
      const meta = {
        eventHash: database.hash(eventHash), eventOrder: 0, operationHash
      };
      const operations = [{meta, operation}];
      ledgerNode.storage.operations.addMany({operations}, callback);
    }],
    addEvent: ['storeOperation', (results, callback) => {
      const {eventHash} = results;
      _addEvent({event, eventHash, ledgerNode}, callback);
    }]
  }, err => callback(err));
};

function _addEvent({event, eventHash, genesis, ledgerNode}, callback) {
  const {storage} = ledgerNode;
  if(genesis) {
    return _writeGenesisBlock({event, storage}, callback);
  }
  async.auto({
    eventHash: callback => {
      if(eventHash) {
        return callback(null, eventHash);
      }
      brLedgerNode.consensus._hasher(event, callback);
    },
    writeEvent: ['eventHash', (results, callback) => {
      const {eventHash} = results;
      const meta = {eventHash, pending: true};
      storage.events.add({event, meta}, callback);
    }],
    addBlock: ['writeEvent', (resultsAlpha, callback) => {
      const {eventHash} = resultsAlpha;
      async.retry({
        errorFilter: err => err.name === 'DuplicateError',
        times: Infinity
      }, callback => async.auto({
        getLatest: callback => storage.blocks.getLatestSummary(callback),
        assignEvent: ['getLatest', (results, callback) => {
          let {blockHeight} = results.getLatest.eventBlock.block;
          blockHeight++;
          const patch = [{
            op: 'set',
            changes: {meta: {blockHeight, blockOrder: 0}}
          }];
          storage.events.update({eventHash, patch}, callback);
        }],
        buildBlock: ['assignEvent', (results, callback) => {
          const {eventBlock} = results.getLatest;
          let {blockHeight} = eventBlock.block;
          blockHeight++;
          const block = {
            '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
            id: _blockId({blockHeight, id: eventBlock.block.id}),
            blockHeight,
            type: 'WebLedgerEventBlock',
            eventHash: [eventHash],
            previousBlock: eventBlock.block.id,
            previousBlockHash: eventBlock.meta.blockHash,
          };
          callback(null, block);
        }],
        blockHash: ['buildBlock', (results, callback) =>
          brLedgerNode.consensus._hasher(results.buildBlock, callback)
        ],
        writeBlock: ['blockHash', (results, callback) => {
          const block = results.buildBlock;
          block.event = block.eventHash;
          delete block.eventHash;
          storage.blocks.add({
            block,
            meta: {
              blockHash: results.blockHash,
              consensus: true,
              consensusDate: Date.now()
            }
          }, callback);
        }]
      }, callback), callback);
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

function _blockId({blockHeight, id}) {
  const i = id.lastIndexOf('/') + 1;
  return id.substring(0, i) + blockHeight;
}

function _writeGenesisBlock({event, storage}, callback) {
  const block = {
    '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
    id: event.ledgerConfiguration.ledger + '/blocks/0',
    blockHeight: 0,
    type: 'WebLedgerEventBlock',
    // event: [event]
  };
  async.auto({
    eventHash: callback => brLedgerNode.consensus._hasher(event, callback),
    writeEvent: ['eventHash', (results, callback) => {
      const {eventHash} = results;

      // assign the event to the block
      block.event = [eventHash];

      const meta = {
        blockHeight: 0, blockOrder: 0, consensus: true,
        consensusDate: Date.now(), eventHash
      };
      storage.events.add({event, meta}, callback);
    }],
    blockHash: ['writeEvent', (results, callback) =>
      brLedgerNode.consensus._hasher(block, callback)],
    writeBlock: ['blockHash', (results, callback) => {
      const meta = {
        blockHash: results.blockHash,
        consensus: true,
        consensusDate: Date.now()
      };
      // block.event = [eventHash];
      storage.blocks.add({block, meta}, callback);
    }]
  }, callback);
}
