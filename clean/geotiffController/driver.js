


const {join} = require("path");
const {fork} = require("child_process");
const processor = require("geotiffProcessor");
const os = require("os");
const resourceSync = require("./resourceSync.js");


//node geotiffcontroller.js -f ./input/index.json -r ./input/RF_Monthly_3_Yr_Sample -d test -o ../../output -l 1 -i 1


//-------------parse args--------------------

//default options
let options = {
    indexFile: undefined,
    dataRoot: undefined,
    dataset: undefined,
    outDir: undefined,

    maxSpawn: -1,
    apiSpawnLimit: -1,
    createHeader: true,
    valueName: "value_map",
    headerName: "raster_header",
    cleanup: true,
    docLimit: -1,
    retryLimit: 3,
    faultLimit: 0,
    containerLoc: null,
    notificationInterval: -1,
    loopResetInterval: 100,
    geotiffHandleChunk: 1
};


let optionsFile = "./config.json";

if(process.argv.length > 2) {
    switch(process.argv[2]) {
        case "-h":
        case "--help": {
            help();
            break;
        }
        default: {
            optionsFile = process.argv[2];
        }
    }
    optionsFile = process.argv[2];
}

let userOptions = require(optionsFile);
for(let option in userOptions) {
    options[option] = userOptions[option];
}

if(options.indexFile === undefined || options.dataRoot === undefined || options.dataset === undefined || options.outDir === undefined) {
    invalidOpts();
}

//need to change this
let helpString = "Available options:\n"
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
+ "-fl, --fault_limit: Optional. Limit the allowable number of metadata ingestion faults (per process) before failing. Note that single process failure will result in program termination. Negative value indicates no limit. Default value 0.\n"
+ "-c, --containerized: Optional. Indicates that the agave instance to be used is containerized and commands will be run using exec with the specified singularity image. Note that faults may not be properly detected when using agave containerization.\n"
+ "-i, --notification_interval: Optional. Print a notification to stdout after this many documents. Negative value indicates never print notification. Default value -1\n"
+ "-h, --help: Show this message.\n";

function invalidOpts() {
    console.error("Invalid config file provided.\n");
    console.error(helpString);
    process.exit(1);
}

function help() {
    console.log(helpString);
    process.exit();
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
let metaLen = geotiffMeta.length;


//make sure not spawning more processes than there are values
let procLimit = Math.min(metaLen, options.processLimit);

let chunkSizeLow = Math.floor(metaLen / processLimit);
let chunkSizeHigh = chunkSizeLow + 1;

let leftover = metaLen % processLimit;

let procLimitHigh = leftover;
let procLimitLow = procLimit - procLimitHigh;

//generate ranges
let ranges = [];

let s = 0;
//ranges are [low, high)
for(let i = 0; i < procLimitHigh; i++) {
    ranges.push([s, s + chunkSizeHigh]);
    s += chunkSizeHigh;
}

for(let i = 0; i < procLimitLow; i++) {
    ranges.push([s, s + chunkSizeLow]);
    s += chunkSizeLow;
}

//sanity check
if(s != metaLen || ranges.length != procLimit) {
    errorExit("Failed sanity check, index file not chunked correctly.");
}


resourceSync.setSpawnLevel(option);




for(let range of ranges) {
    options.indexRange = range;
    //pass options as stringified json object
    let child = fork("handleGeotiffIndex", [JSON.stringify(options)]);

    child.on("exit", (code) => {
        if(code != 0) {
            //should write log of errors etc for children
            errorExit(`Child process failed with non-zero exit code. See log for details. Exit code ${code}. Terminating program.`);
        }
    });

    children.push(child);
}





//---------------------end---------------------





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
    if(children) {
        for(let child of children) {
            child.kill();
        }
    }
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








