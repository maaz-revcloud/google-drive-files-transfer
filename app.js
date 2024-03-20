const { google } = require("googleapis");
const AWS = require("aws-sdk");
const { Pool } = require("pg");
const { Consumer } = require("sqs-consumer");
const { SQSClient } = require("@aws-sdk/client-sqs");
const moment = require("moment");
const stream = require("stream");

AWS.config.update({
  region: "us-west-2",
  signatureVersion: "v4",
});

const SERVICE_ACCOUNT_FILE = "google-security.json";
const SCOPES = ["https://www.googleapis.com/auth/drive"];

const AWS_REGION = "us-west-2";
const S3_BUCKET_NAME = "palisade-api-connectors";

const auth = new google.auth.GoogleAuth({
  keyFile: SERVICE_ACCOUNT_FILE,
  scopes: SCOPES,
});

const drive = google.drive({ version: "v3", auth });

const s3 = new AWS.S3({
  region: AWS_REGION,
});

let init = false;
let pool = null;

const parseSecrets = async (secretString) => {
  const parsedSecrets = JSON.parse(secretString);
  for (const key in parsedSecrets) {
    if (parsedSecrets.hasOwnProperty(key)) {
      const innerString = parsedSecrets[key];
      parsedSecrets[key] = JSON.parse(innerString);
    }
  }
  return parsedSecrets;
};

const fetchSecrets = async () => {
  try {
    const secretsManager = new AWS.SecretsManager();
    const DEV_SECRET_NAME = "dev/envs";

    const data = await secretsManager
      .getSecretValue({ SecretId: DEV_SECRET_NAME })
      .promise();
    const secretString = data.SecretString;
    if (secretString) {
      const parsedSecrets = await parseSecrets(secretString);
      return parsedSecrets;
    } else {
      throw new Error("SecretString is empty");
    }
  } catch (error) {
    console.error("Error retrieving secret:", error);
    throw error;
  }
};

const getDatabasePool = async (event) => {
  try {
    if (!init) {
      const { DB_CREDENTIALS } = await fetchSecrets();

      pool = new Pool({
        user: DB_CREDENTIALS.user,
        host: DB_CREDENTIALS.host,
        database: DB_CREDENTIALS.database,
        password: DB_CREDENTIALS.password,
        port: DB_CREDENTIALS.port,
        idleTimeoutMillis: 10000000,
        connectionTimeoutMillis: 1000000,
      });
      init = true;
    }

    if (!pool) {
      throw new Error("Env not initialized.");
    }
    return pool;
  } catch (error) {
    console.error("Error", error);
  }
};

getDatabasePool();

const queue = Consumer.create({
  queueUrl: "https://sqs.us-west-2.amazonaws.com/221490242148/gdrive-files",
  handleMessage: async ({ Body }) => {
    const jsonString = Body.replace(/^"?'?|"?'?$/g, "");
    const jsonObject = JSON.parse(jsonString);

    await processQueueMessage(jsonObject);
  },
  sqs: new SQSClient({
    region: "us-west-2",
  }),
});

queue.on("error", (err) => {
  console.error(err.message);
});

queue.on("processing_error", (err) => {
  console.error(err.message);
});

queue.start();

async function processQueueMessage(message) {
  try {
    if (message.type === "CREATE") {
      const folderId = message.body.folderId;
      await fetchFilesFromFolder(folderId, "", message);
    }
    if (message.type === "SYNC") {
      await syncFolder(message);
    }
  } catch (error) {
    console.error("Error:", error);
  }
}

processQueueMessage({
  type: "CREATE",
  body: {
    userId: 180,
    connectionId: 359,
    folderId: "1MzT-pGjvIdDFHj91lqGHU1YcvPbWqD5t",
  },
});

async function fetchFilesFromFolder(
  folderId,
  parentFolderPath = "",
  message,
  flows
) {
  try {
    const res = await drive.files.list({
      q: `'${folderId}' in parents`,
      fields: "files(id, name, mimeType, size, modifiedTime)",
    });
    const files = res.data.files;
    if (files.length) {
      for (const file of files) {
        if (file.mimeType === "application/vnd.google-apps.folder") {
          await fetchFilesFromFolder(
            file.id,
            `${parentFolderPath}/${file.name}`
          );
        } else {
          if (flows) {
            const flowToUpdate = flows.find(
              (flow) => flow.event_type === file.name
            );
            if (flowToUpdate) {
              const lastModifiedTime = moment(flowToUpdate.last_execution_time);
              const newModifiedTime = moment(file.modifiedTime).subtract(
                5,
                "hours"
              );

              if (lastModifiedTime.isBefore(newModifiedTime)) {
                await downloadAndUploadFile(
                  file,
                  parentFolderPath,
                  message,
                  true,
                  flowToUpdate,
                  folderId
                );
              }
            } else {
              await downloadAndUploadFile(
                file,
                parentFolderPath,
                message,
                false,
                null,
                folderId
              );
            }
          } else {
            await downloadAndUploadFile(
              file,
              parentFolderPath,
              message,
              false,
              null,
              folderId
            );
          }
        }
      }
    }
  } catch (err) {
    console.error("Error fetching files from folder:", err);
  }
}

async function downloadAndUploadFile(
  file,
  parentFolderPath,
  message,
  update,
  flowToUpdate,
  folderId
) {
  try {
    const uploadParams = {
      Bucket: S3_BUCKET_NAME,
      Key: `${message.body.userId}/${folderId}/${file.name}.csv`,
    };

    let response;

    if (file.mimeType === "application/vnd.google-apps.spreadsheet") {
      response = await drive.files.export(
        {
          fileId: file.id,
          mimeType: "text/csv",
        },
        { responseType: "stream" }
      );
    } else if (file.mimeType === "text/csv") {
      response = await drive.files.get(
        {
          fileId: file.id,
          alt: "media",
        },
        { responseType: "stream" }
      );
    }

    if (response) {
      const flow = await createFlow(file, message, update, flowToUpdate);
      const passThroughStream = new stream.PassThrough();
      response.data.pipe(passThroughStream);
      uploadParams.Body = passThroughStream;

      const uploadResult = await new Promise((resolve, reject) => {
        s3.upload(uploadParams, async (err, data) => {
          if (err) {
            reject(err);
          } else {
            resolve(data);
          }
        });
      });

      console.log("File uploaded successfully to S3:", uploadResult.Location);
      if (flow) {
        await updateFlow(flow.id, "Successful")
      }
      response.data
        .on("end", async () => {
          console.log("Done downloading file.");
        })
        .on("error", async (err) => {
          console.error("Error downloading file:", err);
        });
    }
  } catch (err) {
    if (flow) {
      await updateFlow(flow.id, "Error")
    }
    console.error("Error downloading/uploading file:", err);
  }
}

async function syncFolder(message) {
  try {
    const { connectionId, folderId } = message.body;
    const fetchFlowsQuery = `select * from api_connectors.flows where connection_id = $1`;
    const pool = await getDatabasePool();
    const flows = await pool.query(fetchFlowsQuery, [connectionId]);
    await fetchFilesFromFolder(folderId, "", message, flows.rows);
    return;
  } catch (error) {
    console.error("Error syncing:", error);
  }
}

async function createFlow(file, message, update, flowToUpdate) {
  try {
    const pool = await getDatabasePool();
    const { userId, connectionId, folderId } = message.body;
    if (update) {
      const flowUpdateQuery = `
        UPDATE api_connectors.flows SET last_execution_time = $1 WHERE id = $2`;

      const flowUpdate = await pool.query(flowUpdateQuery, [
        file.modifiedTime,
        flowToUpdate.id,
      ]);
    } else {
      const flowNameUnique = `${folderId}-${file.name}`;
      const flowCreationQuery = `
        INSERT INTO api_connectors.flows(
            name, connection_id, status, event_type, flow_type, last_execution_time, last_execution_status
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7
        )
        RETURNING *
        `;

      const flowCreation = await pool.query(flowCreationQuery, [
        flowNameUnique,
        connectionId,
        "Active",
        file.name,
        "HISTORICAL",
        file.modifiedTime,
        "In Progress"
      ]);

      return flowCreation.rows[0];
    }
    return;
  } catch (error) {
    console.error("Error downloading/uploading file:", error);
  }
}

async function updateFlow(flowId, status) {
  try {
    const pool = await getDatabasePool();

    const currentDate = moment();
    const formattedDate = currentDate.format('YYYY-MM-DD HH:mm:ss.SSS');

    const flowUpdateQuery = `UPDATE api_connectors.flows SET last_execution_status = $1, last_modified = $2 WHERE id = $3`;
    const flowUpdate = await pool.query(flowUpdateQuery, [status, formattedDate, flowId]);
  } catch (error) {
    console.error("Error updating flow status:", error);
  }
}

