// index.js
const { Firestore, FieldValue } = require('@google-cloud/firestore');
const { Storage } = require('@google-cloud/storage');
const axios = require('axios');
const FormData = require('form-data');

const firestore = new Firestore();
const storage   = new Storage();

const BUCKET           = 'evergreen-import-storage/uploads';
const RUNS_COLLECTION  = 'imports';
const HUBSPOT_API_KEY  = process.env.HUBSPOT_API_KEY;
const HUBSPOT_UPLOAD   = 'https://api.hubapi.com/crm/v3/imports/files';

// Batch 1 definition
const BATCH_NUM = 1;
const BATCH_KEY = `batch${BATCH_NUM}`;
const FILES     = [
  `test0___${new Date().toISOString().split('T')[0]}.txt`,
  `test1___${new Date().toISOString().split('T')[0]}.txt`
];

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
    ]
}

/**
 * Creates a multipart/form-data request to HubSpot:
 * - attaches each file under the "files" field
 * - sends importRequest JSON under the "importRequest" field
 * Returns the single importId from the response.
 */
async function createHubSpotImportFromFiles(runId, batchKey, fileNames) {
  const form = new FormData();

  // 1) Build the importRequest JSON body
  const importRequest = {
    name: `Import ${runId} - ${batchKey}`,
    files: fileNames.map(fileName => ({
      fileName: fileName,
      fileFormat: 'CSV',
      fileImportPage: {
        hasHeader: true,
        columnMappings: FILE_SCHEMA[fileName.split('___')[0]]
      }
    }))
  };
  form.append('importRequest', JSON.stringify(importRequest), {
    contentType: 'application/json'
  });

  // 2) Attach each file stream from GCS under the "files" key
  for (const fileName of fileNames) {
    const file = storage.bucket(BUCKET).file(`uploads/${fileName}`);
    const stream = file.createReadStream();
    form.append('files', stream, { filename: fileName, contentType: 'text/csv' });
  }

  // 3) POST form-data to HubSpot
  const headers = {
    ...form.getHeaders(),
    Authorization: `Bearer ${HUBSPOT_API_KEY}`
  };

  const resp = await axios.post(HUBSPOT_UPLOAD, form, { headers });
  return resp.data.id; // single import ID for the batch
}


exports.startBatchImport = async (req, res) => {
  try {
    const runId = req.query.runId || new Date().toISOString().slice(0,10);
    const runRef = firestore.collection(RUNS_COLLECTION).doc(runId);

    // Initialize Firestore record
    await runRef.set({
      createdAt: FieldValue.serverTimestamp(),
      currentBatch: BATCH_NUM,
      [`batches.${BATCH_KEY}.status`]: 'pending',
      [`batches.${BATCH_KEY}.files`]: FILES
    }, { merge: true });

    // Kick off the multipart upload to HubSpot
    const importId = await createHubSpotImportFromFiles(runId, BATCH_KEY, FILES);

    // Update Firestore with the importId & new status
    await runRef.update({
      [`batches.${BATCH_KEY}.importId`]: importId,
      [`batches.${BATCH_KEY}.status`]: 'in_progress'
    });

    res.status(200).json({ runId, batch: BATCH_KEY, importId });
  } catch (err) {
    console.error('Error starting batch import:', err);
    res.status(500).json({ error: err.message });
  }
};
