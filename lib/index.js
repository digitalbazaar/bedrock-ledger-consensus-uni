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
        const requireEventValidation =
          event.input[0].requireEventValidation || false;
        async.auto({
          validate: callback => eventValidation(
            event, event.input[0].eventValidator, {requireEventValidation},
            callback),
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
          validate: ['getConfig', (results, callback) => eventValidation(
            event, results.getConfig.event.input[0].eventValidator, {
              requireEventValidation:
                results.getConfig.event.input[0].requireEventValidation
            },
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
            consensusDate: Date.now(),
            updated: Date.now()
          }
        }
      }], callback);
    }]
  }, (err, results) => callback(err, results.writeEvent));
};

function eventValidation(
  event, validators, {requireEventValidation = false}, callback) {
  // run all the validators in parallel
  const validatorReports = [];
  async.each(validators, (validatorConfig, callback) => async.auto({
    getValidator: callback => brLedger.use(validatorConfig.type, callback),
    mustValidate: ['getValidator', (results, callback) =>
      results.getValidator.mustValidateEvent(
        event, validatorConfig, (err, mustValidateEvent) => {
          if(err) {
            return callback(err);
          }
          const report = {
            validatorConfig,
            mustValidateEvent
          };
          callback(null, report);
        })],
    validateEvent: ['mustValidate', (results, callback) => {
      const report = results.mustValidate;
      if(!report.mustValidateEvent) {
        validatorReports.push(report);
        return callback();
      }
      results.getValidator.validateEvent(event, validatorConfig, err => {
        report.error = err;
        report.validated = !err;
        report.timeStamp = Date.now();
        validatorReports.push(report);
        callback();
      });
    }]
  }, err => callback(err)), err => {
    if(err) {
      return callback(err);
    }
    // when requireEventValidation === true, at least one validator MUST
    // validate the event
    if(requireEventValidation &&
      !validatorReports.some(r => r.mustValidateEvent)) {
      return callback(new BedrockError(
        'No validator was found for this event.', 'ValidationError', {
          httpStatusCode: 400,
          public: true,
          event,
          validatorReports
        }, err));
    }
    // any validator that MUST validate should validate successfully
    if(!validatorReports.every(r => r.mustValidateEvent ? r.validated : true)) {
      return callback(new BedrockError(
        'The event is invalid.', 'ValidationError', {
          httpStatusCode: 400,
          public: true,
          event,
          validatorReports
        }, err));
    }
    // success
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
      const hash = niUri.digest('sha-256', results.normalize, true);
      callback(null, hash);
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
