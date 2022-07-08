const { BlobServiceClient, ContainerClient, ContainerCreateResponse } = require('@azure/storage-blob');

const  readFileInChunks  = require('./src/files/filesUtility').readFileInChunks;




require('dotenv').config();


const CONTAINER_NAME = 'equipment-n';


function createBlobServiceClient()  {
    console.log(process.env.CONN_STRING)
    const blobServiceClient = BlobServiceClient.fromConnectionString(process.env.CONN_STRING);
    return blobServiceClient;
    // return BlobServiceClient.fromConnectionString(CONN_STRING);

}

async function createContainerClient(blobServiceClient) {
    const containerClient =  blobServiceClient.getContainerClient(CONTAINER_NAME)
    const createContainerResponse = await containerClient.createIfNotExists();
    console.log("client got created", createContainerResponse.requestId);
    return containerClient;

}


function readFile(fileName ){
    const nameToFilePathMapper = {
        equipment : './src/data/johancastberg-equipments-data.json',
        system : './src/data/jc-system.json'
    }
    return readFileInChunks(nameToFilePathMapper[fileName]);
}

async function addEquipmentDataJsonAsBlobs(data) {
    let promisesList = [];
    const blobServiceClient = createBlobServiceClient();
    // create table
    const containerClient = await createContainerClient(blobServiceClient);

    


    
    // loop through data
    for (let i = 0; i < data.length; i++) {
        const entity = {
            partitionKey: "johancastberg",
            rowKey: data[i].id,
            type: data[i].type,
            tagNumber: data[i].tagNumber,
            description: data[i].description,
            facility: data[i].facility,
            area: data[i].area,
            system: data[i].system,
            discipline: data[i].discipline,
            procurementPackage: data[i].procurementPackage,
            processLine: data[i].processLine,
            id: data[i].id,
            topTag: data[i].topTag,
            attributes : JSON.stringify(data[i]?.attributes)
        };
        const body = JSON.stringify(data[i]);
        promisesList.push(containerClient.getBlockBlobClient(`${data[i].id}.json`).upload(body, body.length))
       
        // threshold of 500 entities per batch
        if(i % 500 === 0){
            const uploadBlobResponse =  await Promise.all(promisesList)
            console.log("data inserted", i);
            console.log("Blob was uploaded successfully. requestId: ",uploadBlobResponse.requestId);
            promisesList = [];
        }
    }
}

async function execute(){
    const data = await readFile("equipment")
    addEquipmentDataJsonAsBlobs(data);
}


execute();