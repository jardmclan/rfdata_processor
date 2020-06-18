


const {join} = require("path");




const processor = join("geotiffProcessor.js");


const os = require("os");
const ingestor = require("../../meta_ingestor");
const ProcessPool = require("process-pool").default;

console.log(ProcessPool.Pool);
//node geotiffcontroller.js -f ./input/index.json -r ./input/RF_Monthly_3_Yr_Sample -d test -o ../../output -l 1 -i 1

options = JSON.parse(process.argv[2]);


//-------------main--------------------

const index = require(options.indexFile);


ingestor.maxSpawn = options.apiSpawnAlloc;

//NEED TO ADD apiSpawnAlloc AND indexRange TO OPTIONS

let geotiffMeta = index.index;
let chunkSize = 100;

//set up counters
let submittedDocs = 0;
let finishedDocs = 0;
let failedDocs = 0;

let done = false;
let id = 0;

let procExec = pool.prepare((context) => {

    options = context.options;
    return (meta, createHeader, createValue, headerID, valueID) => {
        if(!createHeader && !createValue) {
            return null;
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

            return ingestor.dataHandler(outFile, wrapped, options.retryLimit, options.cleanup, options.containerLoc);
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

            return ingestor.dataHandler(outFile, wrapped, options.retryLimit, options.cleanup, options.containerLoc);
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
}, {
    options: options
});


// options.indexRange
chunkedLoop(options.indexRange[0], options.indexRange[1], chunkSize, (i) => {
    let meta = geotiffMeta[i];

    //only handle statewide mappings, just continue if spatial extent is not statewide
    if(meta.descriptor.spatialExtent != "St") {
        return true;
    }

    let createHeader = i == 0 && options.createHeader && submittedDocs < docLimit;
    if(createHeader) {
        submittedDocs++;
    }
    let createValue = true;
    if(submittedDocs++ >= options.docLimit) {
        createValue = false;
    }
    

    procExec(meta, createHeader, createValue, id++, id++).then((results) => {
        for(let e of results) {
            if(e) {
                warning(`Failed to cleanup file ${docName}\n${e.toString()}`);
            }
            docIngested();
        }
    }, (e) => {
        error(e);
        if(failedDocs++ >= options.faultLimit) {
            errorExit(`Fault limit reached.`);
        }

        
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
    console.log(`Warning from controller:\n${warning.toString()}`);
}

function error(e) {
    console.error(`Error from controller:\n${e.toString()}`);
}

function errorExit(e) {
    console.error(`Critical error in controller. The process will exit.\n${e.toString()}`);
    cleanup();
    exit(1);
}

//-----------------------cleanup and output aux------------------------------

function exit(code = 0) {
    process.exit(code);
}

function cleanup() {
    pool.destroy();
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
    pool.destroy();
    exit();
}



process.on("SIGINT", function() {
    console.log("Caught interrupt, exitting...");
    cleanup();
    exit();
});

process.on("uncaughtException", (e) => {
    errorExit(e);
});








