/*!
 * Copyright (c) 2016-2017 Digital Bazaar, Inc. All rights reserved.
 */

'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const jsonld = bedrock.jsonld;
const niUri = require('ni-uri');
const BedrockError = bedrock.util.BedrockError;

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
    addBlock: ['writeEvent', (resultsAlpha, callback) => {
      if(options.genesis) {
        const configBlock = {
          '@context': 'https://w3id.org/webledger/v1',
          // FIXME: This should be generated based on the latest block number
          id: event.input[0].ledger + '/blocks/1',
          type: 'WebLedgerEventBlock',
          event: [event]
        };
        async.auto({
          validate: callback => eventGuard(
            event, event.input[0].validationEventGuard, callback),
          hashBlock: callback => hasher(configBlock, callback),
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
      } else {
        let done = false;
        // FIXME: implement max retries
        async.until(() => done, loopCallback => async.auto({
          getConfig: callback => storage.events.getLatestConfig(callback),
          validate: ['getConfig', (results, callback) => eventGuard(
            event, results.getConfig.event.input[0].validationEventGuard,
            callback)],
          getLatestBlock: ['validate', (results, callback) =>
            storage.blocks.getLatest(options, callback)],
          buildBlock: ['getLatestBlock', (results, callback) => {
            const latestEventBlock = results.getLatestBlock.eventBlock;
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
          writeBlock: ['hashBlock', (results, callback) => {
            const meta = {
              blockHash: results.hashBlock,
              consensus: true,
              consensusDate: Date.now()
            };
            const options = {};
            storage.blocks.add(results.buildBlock, meta, options, callback);
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
        }), callback);
      }
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
            consensusDate: Date.now()
          }
        }
      }], callback);
    }]
  }, (err, results) => callback(err, results.writeEvent));
};

// FIXME: need to ensure that events are properly structured
// what happens when the event type has no guards?
function eventGuard(event, guardConfig, callback) {
  // get guards applicable to this event type
  const guards = guardConfig.filter(g => {
    // if no filter is specified, this guard should run on all event types
    if(!g.eventFilter) {
      return true;
    }
    return g.eventFilter.some(f =>
      f.type === 'EventTypeFilter' && f.eventType.includes(event.type));
  });
  // run all the guards in parallel
  const guardReport = {};
  async.every(guards, (guard, callback) => async.auto({
    getGuard: callback => brLedger.use(guard.type, callback),
    runGuard: ['getGuard', (results, callback) =>
      results.getGuard.isValid(event, guard, (err, result) => {
        guardReport[guard.type] = {
          err: err,
          result: result,
          timeStamp: Date.now()
        };
        callback(err, result);
      })]
  }, (err, results) => callback(err, results.runGuard))
  , (err, result) => {
    if(err || !result) {
      return callback(new BedrockError(
        'The event failed validation.', 'GuardRejection', {
          guardReport: guardReport
        }, err));
    }
    callback();
  });
}

function hasher(data, callback) {
  async.auto({
    // normalize ledger event to nquads
    normalize: callback => jsonld.normalize(data, {
      algorithm: 'URDNA2015',
      format: 'application/nquads'
    }, callback),
    hash: ['normalize', (results, callback) => {
      callback(null, niUri.digest('sha-256', results.normalize, true));
    }]
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
