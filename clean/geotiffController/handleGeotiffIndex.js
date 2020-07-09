


const {join} = require("path");
const processor = join("geotiffProcessor.js");
const os = require("os");
const ingestor = require("../meta_ingestor");
const syncCodes = require("syncCodes.json");
const Semaphore = require("../Semaphore");


//node geotiffcontroller.js -f ./input/index.json -r ./input/RF_Monthly_3_Yr_Sample -d test -o ../../output -l 1 -i 1

if(process.argv.length < 2) {
    throw new Error("No options argument provided.");
}

options = JSON.parse(process.argv[2]);


//-------------main--------------------

const index = require(options.indexFile);


ingestor.setMaxSpawn(options.apiSpawnAlloc);

//NEED TO ADD apiSpawnAlloc AND indexRange TO OPTIONS

let geotiffMeta = index.index;
let chunkSize = 100;

//set up counters
let submittedDocs = 0;
let finishedDocs = 0;
let failedDocs = 0;

let done = false;
let id = 0;



let geotiffChunk = 3;
let geotiffHandleSemaphore = new Semaphore(geotiffChunk);




function createDoc(meta, createHeader, createValue, headerID, valueID) {
    if(!createHeader && !createValue) {
        return Promise.resolve([]);
    }

    let submitToAPI = (outFile, metadata, id) => {
        //send request for resources to coordinator
        process.send({
            id: id,
            code: syncCodes.request.request
        });

        return new Promise((resolve, reject) => {
            process.on("message", (data) => {
                //if does not equal id this is fullfilling another request
                if(data.id == id) {
                    switch(data.code) {
                        //good to go
                        case syncCodes.response.accept: {
                            dataHandlerPromise = ingestor.dataHandler(outFile, metadata, options.retryLimit, options.cleanup, options.containerLoc);
                            resolve(dataHandlerPromise);
                        }
                        case syncCodes.response.reject: {
                            reject("Request for API access rejected by coordinator.");
                        }
                    }
                }
            });
        });
    }

    let submitHeader = (header, id) => {
        //header should contain header object and dataset from index, anything else?
        let metaDoc = {
            dataset: options.dataset,
        }
        for(let field in header) {
            metaDoc[field] = header[field];
        }
        let wrapped = {
            name: options.headerName,
            value: metaDoc
        };

        let fname = `meta_${id}.json`;
        let outFile = join(options.outDir, fname);

        return submitToAPI(outFile, wrapped, id);
    }

    let submitValues = (values, id) => {
        let metaDoc = {
            values: values,
            timeGranularity: index.descriptor.timeGranularity,
            date: {
                $date: index.descriptor.date
            },
            unit: index.descriptor.unit,
            dataset: options,dataset
        };

        let wrapped = {
            name: options.valueName,
            value: metaDoc
        };

        let fname = `meta_${id}.json`;
        let outFile = join(options.outDir, fname);
        
        return submitToAPI(outFile, wrapped, id);
        
    }


    fpath = join(options.dataRoot, meta.fpath);
    return processor.getDataFromGeoTIFFFile(fpath).then((data) => {
        promises = []
        if(createHeader) {
            promises.push(submitHeader(data.header, headerID));
        }
        if(createValue) {
            promises.push(submitValues(data.values, valueID));
        }


        return Promise.all(promises);

    }, (e) => {
        return Promise.reject(e);
    });
    
}





// options.indexRange holds range handled by this process
chunkedLoop(options.indexRange[0], options.indexRange[1], chunkSize, (i) => {
    let meta = geotiffMeta[i];

    let createHeader = i == 0 && options.createHeader;
    if(createHeader) {
        submittedDocs++;
    }
    let createValue = true;
    if(submittedDocs++ >= options.docLimit) {
        createValue = false;
    }
    
    geotiffHandleSemaphore.acquire().then(() => {
        createDoc(meta, createHeader, createValue, id++, id++).then((results) => {
            for(let e of results) {
                if(e) {
                    warning(`Failed to cleanup file ${docName}\n${e.toString()}`);
                }
                geotiffHandleSemaphore.release();
                docIngested();
            }
        }, (e) => {
            error(e);
            if(failedDocs++ >= options.faultLimit) {
                errorExit(`Fault limit reached.`);
            }
    
            
        });
    });

    

    return submittedDocs < options.docLimit;
}).then(() => {
    allSubmitted();
}, (e) => {
    errorExit(e);
});


//need callback for after, maybe promise!!!
function chunkedLoop(start, end, chunkSize, routine) {
    return new Promise((resolve, reject) => {
        let pos = start;
        continueLoop = true;
        for(let i = 0; i < chunkSize && pos < end; i++, pos++) {
            let continueLoop = routine(pos);
            if(!continueLoop) {
                break;
            }
        }
        if(pos < end && continueLoop) {
            setImmediate(() => {
                chunkedLoop(pos, end, chunkSize, routine).then(() => {
                    resolve();
                });
            });
        }
        else {
            resolve();
        }
    });
    
    
}


//--------------------------error/warning handling---------------------------------------

function warning(warning) {
    console.log(`Warning from handler:\n${warning.toString()}`);
}

function error(e) {
    console.error(`Error from handler:\n${e.toString()}`);
}

function errorExit(e) {
    console.error(`Critical error in handler. The process will exit.\n${e.toString()}`);
    cleanup();
    exit(1);
}

//-----------------------cleanup and output aux------------------------------

function exit(code = 0) {
    process.exit(code);
}

function cleanup() {
}



function allSubmitted() {
    done = true;
}

function docIngested() {
    finishedDocs++;
    if(finishedDocs % options.notificationInterval == 0) {
        console.log(`Completed ingesting ${finishedDocs} docs.`);
    }
    if(done && finishedDocs >= submittedDocs) {
        ingestionComplete();
    }
}

function ingestionComplete() {
    console.log("Complete!");
    exit();
}


process.on("uncaughtException", (e) => {
    errorExit(e);
});








