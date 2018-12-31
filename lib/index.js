/*!
 * Copyright (c) 2016-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const {callbackify, BedrockError} = bedrock.util;
const {constants} = bedrock.config;
const database = require('bedrock-mongodb');
require('bedrock-ledger-context');

const api = {};
module.exports = api;

// register this plugin
bedrock.events.on('bedrock.start', () =>
  brLedgerNode.use('UnilateralConsensus2017', {api, type: 'consensus'}));

api.config = {};

// NOTE: no application for `genesisBlock` here
api.config.change = callbackify(async (
  {genesis, ledgerConfiguration, ledgerNode}) => {
  if(!genesis) {
    const {event: {ledgerConfiguration: {
      ledger: expectedLedger,
      sequence: lastSequence
    }}} = await ledgerNode.storage.events.getLatestConfig();
    const expectedSequence = lastSequence + 1;
    const {ledger, sequence} = ledgerConfiguration;
    if(ledger !== expectedLedger) {
      throw new BedrockError(
        `Invalid configuration 'ledger' value.`, 'SyntaxError', {
          expectedLedger,
          ledger,
          ledgerConfiguration,
          httpStatusCode: 400,
          public: true,
        });
    }
    if(sequence !== expectedSequence) {
      throw new BedrockError(
        `Invalid configuration 'sequence' value.`, 'SyntaxError', {
          expectedSequence,
          ledgerConfiguration,
          sequence,
          httpStatusCode: 400,
          public: true,
        });
    }
  }
  const event = {
    '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerConfigurationEvent',
    ledgerConfiguration
  };
  return _addEvent({event, genesis, ledgerNode});
});

api.operations = {};
api.operations.add = callbackify(async ({ledgerNode, operation}) => {
  const event = {
    '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerOperationEvent',
  };
  const operationHash = await brLedgerNode.consensus._hasher(operation);
  event.operationHash = [operationHash];
  const eventHash = await brLedgerNode.consensus._hasher(event);
  const meta = {
    eventHash,
    eventOrder: 0,
    operationHash
  };
  const recordId = database.hash(operation.record.id);
  const operations = [{meta, operation, recordId}];
  await ledgerNode.storage.operations.addMany({operations});
  await _addEvent({event, eventHash, ledgerNode});
});

async function _addEvent({event, eventHash, genesis, ledgerNode}) {
  const {storage} = ledgerNode;
  if(genesis) {
    return _writeGenesisBlock({event, storage});
  }
  if(!eventHash) {
    eventHash = await brLedgerNode.consensus._hasher(event);
  }
  const meta = {eventHash, pending: true};
  await storage.events.add({event, meta});

  let blockHeight;
  while(true) {
    try {
      const {eventBlock} = await storage.blocks.getLatestSummary();

      // compute new block height
      blockHeight = eventBlock.block.blockHeight + 1;
      const patch = [{
        op: 'set',
        changes: {meta: {blockHeight, blockOrder: 0}}
      }];
      await storage.events.update({eventHash, patch});

      // create the new block
      const block = {
        '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
        id: _blockId({blockHeight, id: eventBlock.block.id}),
        blockHeight,
        type: 'WebLedgerEventBlock',
        eventHash: [eventHash],
        previousBlock: eventBlock.block.id,
        previousBlockHash: eventBlock.meta.blockHash,
      };
      // compute hash using `eventHash`
      const blockHash = await brLedgerNode.consensus._hasher(block);
      // replace `eventHash` with `event` (but use `eventHash` as the value
      // for storage optimization)
      block.event = block.eventHash;
      delete block.eventHash;

      // add the new block
      await storage.blocks.add({
        block,
        // instruct storage API not to emit a `block.add` event
        emit: false,
        meta: {
          blockHash,
          consensus: true,
          consensusDate: Date.now()
        }
      });

      // success, break out
      break;
    } catch(e) {
      // ignore duplicate errors and try again (another process wrote
      // a different block at the same height and time we tried to so we
      // must loop to try again)
      if(e.name !== 'DuplicateError') {
        throw e;
      }
    }
  }

  // update the event from pending to having achieved consensus
  const now = Date.now();
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
        consensusDate: now,
        updated: now
      }
    }
  }];
  if(event.type === 'WebLedgerConfigurationEvent') {
    patch[1].changes.meta.effectiveConfiguration = true;
  }
  await storage.events.update({eventHash, patch});

  // NOTE: listeners for the `block.add` event expect to be able to perform
  // record queries immediately upon receiving the event. This means that
  // all operations and events must be finalized before the event is emitted.
  // In this case, the algorithm adds the block before finalizing the event.
  // Therefore, the `block.add` event must be emitted manually.
  await bedrock.events.emit(
    'bedrock-ledger-storage.block.add', {
      blockHeight, ledgerNodeId: ledgerNode.id
    });
}

function _blockId({blockHeight, id}) {
  const i = id.lastIndexOf('/') + 1;
  return id.substring(0, i) + blockHeight;
}

async function _writeGenesisBlock({event, storage}) {
  const eventHash = await brLedgerNode.consensus._hasher(event);
  await storage.events.add({event, meta: {
    blockHeight: 0, blockOrder: 0, consensus: true,
    consensusDate: Date.now(), effectiveConfiguration: true, eventHash
  }});

  const block = {
    '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
    id: event.ledgerConfiguration.ledger + '/blocks/0',
    blockHeight: 0,
    type: 'WebLedgerEventBlock',
    eventHash: [eventHash]
  };
  const blockHash = await brLedgerNode.consensus._hasher(block);
  // assign the event to the block using its hash for optimization purposes
  block.event = [eventHash];
  delete block.eventHash;

  const meta = {
    blockHash,
    consensus: true,
    consensusDate: Date.now()
  };
  return storage.blocks.add({block, meta});
}
