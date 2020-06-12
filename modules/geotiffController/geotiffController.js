


const {join} = require("path");




const processor = join("geotiffProcessor.js");


const os = require("os");
const ingestor = require("../../meta_ingestor");
const ProcessPool = require("process-pool").default;

console.log(ProcessPool.Pool);
//node geotiffcontroller.js -f ./input/index.json -r ./input/RF_Monthly_3_Yr_Sample -d test -o ../../output -l 1 -i 1


//-------------parse args--------------------

let options = {
    indexFile: undefined,
    dataRoot: undefined,
    dataset: undefined,
    outDir: undefined,

    maxSpawn: -1,
    createHeader: true,
    valueName: "value_map",
    headerName: "raster_header",
    cleanup: true,
    docLimit: -1,
    retryLimit: 3,
    faultLimit: 0,
    containerLoc: null,
    notificationInterval: -1
}



let helpString = "Available arguments:\n"
+ "-f, --index_file: Required. An index file containing the paths to the geotiff files and their respective metadata.\n"
+ "-r, --data_root: Required. The root folder for the geotiff files. Index file paths should be relative to this folder.\n"
+ "-d, --dataset: Required. Identifier for dataset being ingested.\n"
+ "-o, --output_directory: Required. Directory to write JSON documents and other output.\n"
+ "-s, --max_spawn: Optional. The maximum number of geotiff file processor processes to spawn at once. Negative values indicate equal to the number of logical cores on the system. Default value -1.\n"
+ "-nh, --no_header: Optional. Indicates that no header object should be generated. By default a document containing information on the geotiff file header will be produced.\n"
+ "-vn, --value_name: Optional. Name to assign to value documents. Default value 'value_map'.\n"
+ "-hn, --header_name: Optional. Name to assign to header document. Default value 'raster_header'.\n"
+ "-nc, --no_cleanup: Optional. Turns off document cleanup after ingestion. JSON output will not be deleted (deleted by default).\n"
+ "-l, --document_limit: Optional. Limit the number of metadata documents to be ingested. Negative value indicates no limit. Default value -1.\n"
+ "-r, --retry_limit: Optional. Limit the number of times to retry a document ingestion on failure before counting it as a fault. Negative value indicates no limit. Default value 3.\n"
+ "-fl, --fault_limit: Optional. Limit the allowable number of metadata ingestion faults before failing. Negative value indicates no limit. Default value 0.\n"
+ "-c, --containerized: Optional. Indicates that the agave instance to be used is containerized and commands will be run using exec with the specified singularity image. Note that faults may not be properly detected when using agave containerization.\n"
+ "-i, --notification_interval: Optional. Print a notification to stdout after this many documents. Negative value indicates never print notification. Default value -1\n"
+ "-h, --help: Show this message.\n"

function invalidArgs() {
    console.error(helpString);
    process.exit(2);
}

function helpAndTerminate() {
    console.log(helpString);
    process.exit(0);
}

let args = process.argv.slice(2);

for(let i = 0; i < args.length; i++) {
    switch(args[i]) {
        case "-s":
        case "--max_spawn": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            let valuei = parseInt(args[i]);
            if(isNaN(valuei)) {
                invalidArgs();
            }
            else {
                options.maxSpawn = valuei;
            }
            break;
        }

        case "-nh":
        case "--no_header": {
            options.createHeader = false;
            break;
        }
        case "-vn":
        case "--value_name": {
            if(++i >= args.length) {
                invalidArgs();
            }
            options.valueName = args[i];
            break;
        }
        case "-hn":
        case "--header_name": {
            if(++i >= args.length) {
                invalidArgs();
            }
            options.headerName = args[i];
            break;
        }
        case "-r":
        case "--data_root": {
            if(++i >= args.length) {
                invalidArgs();
            }
            options.dataRoot = args[i];
            break;
        }
        case "-f":
        case "--index_file": {
            if(++i >= args.length) {
                invalidArgs();
            }
            options.indexFile = args[i];
            break;
        }


        case "-d":
        case "--dataset": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            options.dataset = value;
            break;
        }
        
        case "-o":
        case "-output_directory": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            options.outDir = value;
            break;
        }
        case "-nc":
        case "--no_cleanup": {
            options.cleanup = false;
            break;
        }
        case "-l":
        case "--document_limit": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            valuei = parseInt(value);
            if(isNaN(valuei)) {
                invalidArgs();
            }
            else {
                options.docLimit = valuei;
            }
            break;
        }
        case "-fl":
        case "--fault_limit": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            valuei = parseInt(value);
            if(isNaN(valuei)) {
                invalidArgs();
            }
            else {
                options.faultLimit = valuei;
            }
            break;
        }
        case "-r":
        case "--retry_limit": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            valuei = parseInt(value);
            if(isNaN(valuei)) {
                invalidArgs();
            }
            else {
                options.retryLimit = valuei;
            }
            break;
        }
        case "-c":
        case "--containerized": {
            //get next arg, ensure not out of range
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            options.containerLoc = value;
            break;
        }
        case "-i":
        case "--notification_interval": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            valuei = parseInt(value);
            if(isNaN(valuei)) {
                invalidArgs();
            }
            else {
                options.notificationInterval = valuei;
            }
            break;
        }
        case "-h":
        case "--help": {
            helpAndTerminate();
            break;
        }
        default: {
            invalidArgs();
        }
    }
}



if(options.indexFile === undefined || options.dataRoot === undefined || options.dataset === undefined || options.outDir === undefined) {
    invalidArgs()
}


//convert negative limits to infinity for easier comparisons
if(options.faultLimit < 0) {
    options.faultLimit = Number.POSITIVE_INFINITY;
}
if(options.docLimit < 0) {
    options.docLimit = Number.POSITIVE_INFINITY;
}
if(options.notificationInterval < 0) {
    options.notificationInterval = Number.POSITIVE_INFINITY;
}
if(options.retryLimit < 0) {
    options.retryLimit = Number.POSITIVE_INFINITY;
}
if(options.maxSpawn < 0) {
    options.maxSpawn = os.cpus().length;
}








//-------------main--------------------

const index = require(options.indexFile);


let geotiffMeta = index.index;
let chunkSize = 100;
let pool = new ProcessPool({processLimit: options.maxSpawn});

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



chunkedLoop(0, geotiffMeta.length, chunkSize, (i) => {
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








