// index.js
const { Firestore, FieldValue } = require('@google-cloud/firestore');
const { Storage }               = require('@google-cloud/storage');
const axios                     = require('axios');
const FormData                  = require('form-data');

const firestore = new Firestore();
const storage   = new Storage();

const BUCKET_NAME     = 'evergreen-import-storage';
const RUNS_COLLECTION = 'imports';
const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY;
const HUBSPOT_UPLOAD  = 'https://api.hubapi.com/crm/v3/imports/files';

// Define your batches by _base_ filename (no date or part suffix)
const BATCH_FILES = {
  1: ['test0', 'test1'],
  2: ['test2'],
  3: ['test0', 'test2']
};

// Column mappings keyed by base filename
const FILE_SCHEMA = {
  "test0": [
      { columnObjectTypeId: '0-8', columnName: 'Key_Number', propertyName: 'key_number' },
      { columnObjectTypeId: '0-3', columnName: 'Order_Number', propertyName: 'order_number' },
      { columnObjectTypeId: '0-3', columnName: 'PO Number', propertyName: 'po_number' },
      { columnObjectTypeId: '0-8', columnName: 'Item_ID', propertyName: 'hs_sku' },
      { columnObjectTypeId: '0-8', columnName: 'Item_Description', propertyName: 'hs_sku' },
      { columnObjectTypeId: '0-8', columnName: 'Item_Price', propertyName: 'price' },
      { columnObjectTypeId: '0-8', columnName: 'Total_Price', propertyName: 'total_price' }
  ],
  "test1": [
      { columnObjectTypeId: '0-8', columnName: 'Key_Number', propertyName: 'key_number' },
      { columnObjectTypeId: '0-3', columnName: 'Order_Number', propertyName: 'order_number' },
      { columnObjectTypeId: '0-3', columnName: 'PO Number', propertyName: 'po_number' },
      { columnObjectTypeId: '0-8', columnName: 'Item_ID', propertyName: 'hs_sku' },
      { columnObjectTypeId: '0-8', columnName: 'Item_Description', propertyName: 'hs_sku' },
      { columnObjectTypeId: '0-8', columnName: 'Item_Price', propertyName: 'price' },
      { columnObjectTypeId: '0-8', columnName: 'Total_Price', propertyName: 'total_price' },
      //{ columnObjectTypeId: '0-1', columnName: 'Rep_Email', propertyName: 'email' }
  ],
  "test2": [
      { columnObjectTypeId: '0-8', columnName: 'Key_Number', propertyName: 'key_number' },
      { columnObjectTypeId: '0-3', columnName: 'Order_Number', propertyName: 'order_number' },
      { columnObjectTypeId: '0-3', columnName: 'PO Number', propertyName: 'po_number' },
      { columnObjectTypeId: '0-8', columnName: 'Item_ID', propertyName: 'hs_sku' },
      { columnObjectTypeId: '0-8', columnName: 'Item_Description', propertyName: 'hs_sku' },
      { columnObjectTypeId: '0-8', columnName: 'Item_Price', propertyName: 'price' },
      { columnObjectTypeId: '0-8', columnName: 'Total_Price', propertyName: 'total_price' },
      { columnObjectTypeId: '0-1', columnName: 'Rep_Email', propertyName: 'email' }
  ]
}

/**
 * List all files in GCS for this batch:
 *   - names look like `${base}____YYYY-MM-DD.txt` or with `___partN` before .txt
 * Groups them by base and ensures each base has >=1 file.
 * Returns array of filenames including suffixes.
 */
async function discoverBatchFiles(batchNum, runId) {
  const bases = BATCH_FILES[batchNum];
  const dateSuffix = runId; // e.g. "2025-05-22"
  const prefix = 'uploads/';

  console.log('Discover Batch Files')

  // List all objects under "uploads/"
  const [files] = await storage.bucket(BUCKET_NAME).getFiles({ prefix });

  // Build a map base → [full filenames]
  const groups = bases.reduce((acc, b) => ({ ...acc, [b]: [] }), {});
  for (const file of files) {
    const name = file.name.replace(prefix, ''); // e.g. "test0____2025-05-22___part1.txt"
    for (const base of bases) {
      // match files starting with base____date
      if (name.startsWith(`${base}____${dateSuffix}`) && name.endsWith('.txt')) {
        groups[base].push(name);
      }
    }
  }

  // Ensure each base has at least one file
  const missing = bases.filter(b => groups[b].length === 0);
  if (missing.length) {
    throw new Error(`Still waiting for files: ${missing.join(', ')}`);
  }

  // Flatten into single array
  return Object.values(groups).flat();
}

/**
 * Builds and sends the multipart/form-data import request
 */
async function createHubSpotImport(runId, batchNum, filenames) {
  const form = new FormData();

  console.log('Creating HubSpot Import')

  // Build the importRequest JSON
  const importRequest = {
    name: `Import ${runId} - batch${batchNum}`,
    files: filenames.map(fn => {
      const base = fn.split('____')[0];
      return {
        fileName: fn,
        fileFormat: 'CSV',
        fileImportPage: {
          hasHeader: true,
          columnMappings: FILE_SCHEMA[base]
        }
      };
    })
  };
  form.append('importRequest', JSON.stringify(importRequest), {
    contentType: 'application/json'
  });

  // Attach each file stream
  for (const fn of filenames) {
    const stream = storage
      .bucket(BUCKET_NAME)
      .file(`uploads/${fn}`)
      .createReadStream();
    form.append('files', stream, { filename: fn, contentType: 'text/csv' });
  }

  const headers = {
    ...form.getHeaders(),
    Authorization: `Bearer ${HUBSPOT_API_KEY}`
  };

  const resp = await axios.post(HUBSPOT_UPLOAD, form, { headers });
  return resp.data.id;
}

exports.startBatchImport = async (req, res) => {

  console.log('Starting')
  
  try {
    // 1) Read runId & batchNum
    const runId    = req.query.runId    || new Date().toISOString().slice(0,10);
    const batchNum = parseInt(req.query.batchNum || '1', 10);
    if (!BATCH_FILES[batchNum]) {
      return res.status(400).json({ error: `Unknown batchNum ${batchNum}` });
    }
    const batchKey = `batch${batchNum}`;

    // 2) Discover all split files for this batch
    let filenames;
    try {
      filenames = await discoverBatchFiles(batchNum, runId);
    } catch (e) {
      // Still waiting for some parts
      return res.status(202).json({ message: e.message });
    }

    // 3) Initialize Firestore
    const runRef = firestore.collection(RUNS_COLLECTION).doc(runId);
    await runRef.set({
      createdAt: FieldValue.serverTimestamp(),
      currentBatch: batchNum,
      [`batches.${batchKey}.status`]: 'pending',
      [`batches.${batchKey}.files`]: filenames
    }, { merge: true });

    // 4) Kick off HubSpot import
    const importId = await createHubSpotImport(runId, batchNum, filenames);

    // 5) Update Firestore to in_progress
    await runRef.update({
      [`batches.${batchKey}.importId`]: importId,
      [`batches.${batchKey}.status`]: 'in_progress'
    });

    res.json({ runId, batch: batchKey, importId, files: filenames });
  } catch (err) {
    console.error('startBatchImport error:', err);
    res.status(500).json({ error: err.message });
  }
};
