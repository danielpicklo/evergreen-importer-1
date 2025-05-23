// index.js
const { Firestore, FieldValue } = require('@google-cloud/firestore');
const { Storage }               = require('@google-cloud/storage');
const axios                     = require('axios');
const FormData                  = require('form-data');
const minimist                  = require('minimist');

const firestore = new Firestore({
  projectId: 'evergreen-45696013',
  databaseId: 'imports'
});
const storage   = new Storage();

const BUCKET_NAME     = 'evergreen-import-storage';
const RUNS_COLLECTION = 'imports';
const HUBSPOT_API_KEY = process.env.HUBSPOT_API_KEY;
const HUBSPOT_UPLOAD  = 'https://api.hubapi.com/crm/v3/imports/';

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
// Helper: list & group all split files for a batch
async function discoverBatchFiles(batchNum, runId) {

  console.log('Discovering Batch Files:', batchNum)
  console.log('--------------------------')
  
  const bases = BATCH_FILES[batchNum];
  const dateSuffix = runId; // e.g. "2025-05-23"
  const prefix = 'uploads/';

  const [files] = await storage.bucket(BUCKET_NAME).getFiles({ prefix });
  const groups = bases.reduce((acc, b) => ({ ...acc, [b]: [] }), {});
  
  for (const f of files) {
    const name = f.name.replace(prefix, '');
    bases.forEach(base => {
      if (name.startsWith(`${base}____${dateSuffix}`) && name.endsWith('.txt')) {
        groups[base].push(name);
        console.log('File Name:', name)
      }
    });
  }

  /*const missing = bases.filter(b => groups[b].length === 0);
  if (missing.length) {
    throw new Error(`Missing files for base: ${missing.join(', ')}`);
  }*/

  return Object.values(groups).flat();
}

// Helper: perform multipart/form-data import to HubSpot
async function createHubSpotImport(runId, batchNum, filenames) {

  console.log('--------------------------')
  console.log('Creating Import:', batchNum)
  
  const form = new FormData();
  form.append('importRequest', JSON.stringify({
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
  }), { contentType: 'application/json' });

  for (const fn of filenames) {
    form.append('files',
      storage.bucket(BUCKET_NAME)
        .file(`uploads/${fn}`)
        .createReadStream(),
      { filename: fn, contentType: 'text/csv' }
    );
  }

  const headers = { 
    ...form.getHeaders(),
    Authorization: `Bearer ${HUBSPOT_API_KEY}`
  };

  //console.log(form)
  //console.log(HUBSPOT_API_KEY)

  try{
    const resp = await axios.post(HUBSPOT_UPLOAD, form, { headers });
    console.log('Success!')
    
    return resp.data.id;
  }catch(error){
    console.error('HubSpot Error:', error.response)
  }
}

// Main entrypoint
(async () => {

  console.log('Initializing')
  
  try {
    // 1) parse args
    const argv = minimist(process.argv.slice(2));
    const runId    = argv.runId    || new Date().toISOString().slice(0,10);
    const batchNum = parseInt(argv.batchNum || '1', 10);
    if (!BATCH_FILES[batchNum]) {
      throw new Error(`Unknown batchNum ${batchNum}`);
    }
    const batchKey = `batch${batchNum}`;

    // 2) discover files
    const filenames = await discoverBatchFiles(batchNum, runId);

    // 3) init Firestore doc
    const runRef = firestore.collection(RUNS_COLLECTION).doc(runId);
    await runRef.set({
      createdAt: FieldValue.serverTimestamp(),
      currentBatch: batchNum,
      [`batches.${batchKey}.status`]: 'pending',
      [`batches.${batchKey}.files`]: filenames
    }, { merge: true });

    // 4) call HubSpot import
    const importId = await createHubSpotImport(runId, batchNum, filenames);

    // 5) mark in_progress
    /*await runRef.update({
      [`batches.${batchKey}.importId`]: importId,
      [`batches.${batchKey}.status`]: 'in_progress'
    });

    console.log(`✔ Launched batch${batchNum} (importId: ${importId})`);*/
    process.exit(0);

  } catch (err) {
    console.error('❌ Importer job failed:', err);
    process.exit(1);
  }
})();
