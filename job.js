// job.js
const { startBatchImport } = require('./index.js');

// parse runId & batchNum from environment or args
const argv = require('minimist')(process.argv.slice(2));
const runId    = argv.runId    || new Date().toISOString().slice(0,10);
const batchNum = parseInt(argv.batchNum || '1', 10);

(async () => {
  try {
    await startBatchImport({ query: { runId, batchNum } }, {
      status: () => ({ json: () => {} }),
      statusCode: 200,
      json: () => {}
    });
    process.exit(0);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
