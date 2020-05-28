const path = require("path");
const moduleLoader = require("./module_loader.js");
const {fork} = require("child_process");


let Controller = null;
let source = null;
let ingestionCoordinator = null;

let options = null;
let outDir = null;

let cleanup = true;
let containerLoc = null;
let documentLimit = -1;
let faultLimit = -1;
let retryLimit = 3;
let maxSpawn = 5;
let notificationInterval = -1;
let highWaterMark = 100 * Math.pow(2, 20);


//-------------parse args--------------------

let helpString = "Available arguments:\n"
+ "-m, --module: Required. The controller module for generating metadata documents, either by name if registered in the module index file, or by path. May be followed by a JSON string options argument, otherwise all further arguments will be parsed as module options.\n"
+ "-o, --output_directory: Required. Directory to write JSON documents and other output.\n"
+ "-nc, --no_cleanup: Optional. Turns off document cleanup after ingestion. JSON output will not be deleted (deleted by default).\n"
+ "-l, --document_limit: Optional. Limit the number of metadata documents to be ingested. Negative value indicates no limit. Default value -1.\n"
+ "-rl, --retry-limit: Optional. Limit the number of times to retry a document ingestion on failure before counting it as a fault. Negative value indicates no limit. Default value 3.\n"
+ "-fl, --fault_limit: Optional. Limit the number of metadata ingestion faults before failing. Negative value indicates no limit. Default value -1.\n"
+ "-c, --containerized: Optional. Indicates that the agave instance to be used is containerized and commands will be run using exec with the specified singularity image. Note that faults may not be properly detected when using agave containerization.\n"
+ "-s, --max_spawn: Optional. The maximum number of ingestor processes to spawn at once. Note that the total number of processes will be n+2 (main process and coordinator process) and does not include processes spawned by controller implementaitons (which may have their own max spawn option). Default value 5.\n"
+ "-hw, --high_water_mark: Optional. Size of message queue before attempting to throttling message source. Can be provided in bytes, KB (K suffix), MB (M suffix), or GB (G suffix). Negative value indicates no limit. Default value 100M\n"
+ "-i, --notification_interval: Optional. Print a notification to stdout after this many documents. Negative value indicates never print notification. Default value -1\n"
+ "-h, --help: Show this message.\n";

function invalidArgs(message) {
    console.error(message);
    process.exit(2);
}

function helpAndTerminate(message) {
    console.log(message);
    process.exit(0);
}

let args = process.argv.slice(2);
argLoop:
for(let i = 0; i < args.length; i++) {
    switch(args[i]) {
        case "-m":
        case "--module": {
            //get next arg, ensure not out of range
            if(++i >= args.length) {
                invalidArgs(helpString);
            }
            let controller = args[i];
            //set up controller so have available for options parsing
            try {
                Controller = moduleLoader.load(controller);
            }
            catch(e) {
                errorExit(`Error getting controller.\n${e.toString()}`);
            }
            //check if options provided (not required, use empty object if not)
            let nextArgC = i + 1;
            if(nextArgC < args.length) {
                nextArg = args[nextArgC];
                //check if next arg is a valid JSON string and skip if not
                try {
                    //json parse requires double quotes, allow single quote use in arg so don't have to escape when typing
                    nextArg.replace(/'/g, '"');
                    options = JSON.parse(nextArg);
                    //if setting options didn't throw an error then consume the next argument
                    i++;
                }
                catch(e) {}
            }
            //if options parameter wasn't set then process remaining arguments as controller options
            if(options === null) {
                let result = Controller.parseArgs(args.slice(i + 1));
                if(result.success) {
                    if(typeof result.result == "string") {
                        helpAndTerminate(`Help message from controller module:\n${result.result}`);
                    }
                    else {
                        options = result.result;
                    }
                }
                else {
                    //print help message from result object
                    invalidArgs(`Error processing controller module args:\n${result.result}`);
                }
                //break out of arg loop (already processed rest of args)
                break argLoop;
            }
            break;
        }
        case "-o":
        case "-output_directory": {
            if(++i >= args.length) {
                invalidArgs(helpString);
            }
            outDir = args[i];
            break;
        }
        case "-nc":
        case "--no_cleanup": {
            cleanup = false;
            break;
        }
        case "-l":
        case "--document_limit": {
            if(++i >= args.length) {
                invalidArgs(helpString);
            }
            documentLimit = parseInt(args[i]);
            if(isNaN(documentLimit)) {
                invalidArgs(helpString);
            }
            break;
        }
        case "-fl":
        case "--fault_limit": {
            if(++i >= args.length) {
                invalidArgs(helpString);
            }
            faultLimit = parseInt(args[i]);
            if(isNaN(faultLimit)) {
                invalidArgs(helpString);
            }
            break;
        }
        case "-rl":
        case "--retry_limit": {
            if(++i >= args.length) {
                invalidArgs(helpString);
            }
            retryLimit = parseInt(args[i]);
            if(isNaN(retryLimit)) {
                invalidArgs(helpString);
            }
            break;
        }
        case "-c":
        case "--containerized": {
            //get next arg, ensure not out of range
            if(++i >= args.length) {
                invalidArgs(helpString);
            }
            containerLoc = args[i];
            break;
        }
        case "-s":
        case "--max_spawn": {
            if(++i >= args.length) {
                invalidArgs(helpString);
            }
            maxSpawn = parseInt(args[i]);
            if(isNaN(maxSpawn)) {
                invalidArgs(helpString);
            }
            break;
        }
        case "-hw":
        case "--high_water_mark": {
            if(++i >= args.length) {
                invalidArgs(helpString);
            }
            highWaterMark = parseToBytes(args[i]);
            //make sure argument was valid, returns null if not
            if(highWaterMark === null) {
                invalidArgs(helpString);
            }
            break;
        }
        case "-i":
        case "--notification_interval": {
            if(++i >= args.length) {
                invalidArgs(helpString);
            }
            notificationInterval = parseInt(args[i]);
            if(isNaN(notificationInterval)) {
                invalidArgs(helpString);
            }
            break;
        }
        case "-h":
        case "--help": {
            helpAndTerminate(helpString);
            break;
        }
        default: {
            invalidArgs(helpString);
        }
    }
}

//need to specify controller and output directory
if(Controller == null || outDir == null) {
    invalidArgs(helpString);
}

//convert negative limits to infinity for easier comparisons (keep coordinator values negative for arg translation)
if(faultLimit < 0) {
    faultLimit = Number.POSITIVE_INFINITY;
}
if(documentLimit < 0) {
    documentLimit = Number.POSITIVE_INFINITY;
}
if(notificationInterval < 0) {
    notificationInterval = Number.POSITIVE_INFINITY;
}

//parse highwatermark arg to bytes
function parseToBytes(size) {
    let sizeInBytes = null;
    //check if suffixed
    let suffix = size.slice(-1).toUpperCase();
    switch(suffix) {
        case "K": {
            //remove suffix and parse as number
            let sizef = parseFloat(size.slice(0, -1));
            //return null if can't parse size as number
            if(!isNaN(sizef)) {
                //size is in KB, scale accordingly
                sizef *= Math.pow(2, 10);
                sizeInBytes = sizef;
            }
            break;
        }
        case "M": {
            //remove suffix and parse as number
            let sizef = parseFloat(size.slice(0, -1));
            //return null if can't parse size as number
            if(!isNaN(sizef)) {
                //size is in MB, scale accordingly
                sizef *= Math.pow(2, 20);
                sizeInBytes = sizef;
            }
            break;
        }
        case "G": {
            //remove suffix and parse as number
            let sizef = parseFloat(size.slice(0, -1));
            //return null if can't parse size as number
            if(!isNaN(sizef)) {
                //size is in GB, scale accordingly
                sizef *= Math.pow(2, 30);
                sizeInBytes = sizef;
            }
            break;
        }
        default: {
            //parse size as number
            let sizef = parseFloat(size);
            //return null if can't parse size as number
            if(!isNaN(sizef)) {
                sizeInBytes = sizef;
            }
        }
    }
    return sizeInBytes;
}

//-------------end parse args--------------------



//just use sequential ids, also serves as a counter for the number of docs for exiting
let docID = 0;
let returned = 0;
let allSent = false;

//verify that document has required name and value fields
function validateDocument(document) {
    return document.name !== undefined && document.value !== undefined;
}

function sendData(metadata) {
    if(!validateDocument(metadata)) {
        console.error(`Error: Received invalid document from controller. Document must have name and value fields.`);
        return;
    }
    let id = docID++;
    let fname = path.join(outDir, `metadoc_${id}.json`);
    let message = {
        id: id,
        data: JSON.stringify(metadata),
        fname: fname,
        cleanup: cleanup,
        container: containerLoc
    };
    ingestionCoordinator.send(message, (e) => {
        if(e) {
            console.error(`Error: Failed to send message.\nID: ${message.id}\nReason: ${e.toString()}\n`);
        }
    });
}

function complete() {
    //make sure this hasn't already been called
    if(!allSent) {
        //signal to coordinator that all documents have been sent
        ingestionCoordinator.send(null);
        //set flag to indicate complete
        allSent = true;
    }
}


let ingestorOptions = {
    highWaterMark: highWaterMark,
    maxSpawn: maxSpawn,
    retry: retryLimit
};

ingestionCoordinator = fork("ingestion_coord.js", [JSON.stringify(ingestorOptions)], {stdio: "pipe"});

function errorExit(e) {
    console.error(`An error has occurred, the process will exit.\n${e.toString()}`);
    cleanupFunct();
    process.exit(1);
}

function cleanupFunct() {
    //kill the coordinator process if process handle exists
    if(ingestionCoordinator) {
        ingestionCoordinator.kill("SIGKILL");
    }
    //destroy source
    if(source) {
        source.destroy();
    }
}

//message should have a value type and value
function processMessage(message) {
    switch(message.type) {
        //control messages, use to throttle input document stream
        case "control": {
            controlMessageHandler(message.value);
            break;
        }
        //result messages
        case "result": {
            resultMessageHandler(message.value);
            break;
        }
        //?
        default: {
            console.error("Unrecognized message type received from coordinator.");
        }
    }
}

let controlPause = false;

function controlMessageHandler(message) {
    switch(message) {
        //message queue is at high water mark, please stop sending messages
        case "pause": {
            controlPause = true;
            //pause document source
            source.pause();
            break;
        }
        //go ahead and send more
        case "resume": {
            controlPause = false;
            //resume document source, don't worry about checking for data throttle, breaking that isn't too important
            source.resume();
            break;
        }
    }
}

function resultMessageHandler(message) {
    if(!message.result.success) {
        console.error(`Error: Metadata ingestion failed.\nID: ${message.id}\nPOF: ${message.result.pof}\nReason: ${message.result.error}\n`);
        if(++faults > faultLimit) {
            errorExit(new Error("Fault limit reached. Too many metadata ingestor processes exited with an error."));
        }
    }
    else if(message.result.pof != null) {
        console.log(`Warning: An error occured after metadata insertion.\nID: ${message.id}\nPOF: ${message.result.pof}\nReason: ${message.result.error}\n`);
    }

    returned++;
    if(returned % notificationInterval == 0) {
        console.log(`Finished ${returned} documents.`);
    }

    if(returned >= docID && allSent) {
        console.log("Complete!");
        process.exit(0);
    }
}



//push errors from coordination process to stderr
ingestionCoordinator.stderr.on("data", (chunk) => {
    console.error(`Error in coordinator process: ${chunk.toString()}`);
});

//if coordination process exits with an error code exit process imediately
ingestionCoordinator.on("exit", (code) => {
    if(code != 0) {
        console.error(`Error: Coordinator process has exited with a non-zero exit code. Error code ${code}.`);
        process.exit(1);
    }
});

let faults = 0;
ingestionCoordinator.on("message", (message) => {
    processMessage(message);
});


let documentsReceived = 0;
source = new Controller(options);












//--------------------------------------------


function cleanupFile(fname) {
    return new Promise((resolve, reject) => {
        fs.unlink(fname, (e) => {
            if(e) {
                reject(e)
            }
            else {
                resolve();
            }
        });
    });
}

function addMeta(metaFile, container) {
    return new Promise((resolve, reject) => {
        child = container == null ? spawn("bash", ["./bin/agave_local/add_meta.sh", metaFile]) : spawn("bash", ["./bin/agave_containerized/add_meta.sh", container, metaFile]);
        //could not spawn bash process
        child.on("error", (e) => {
            reject(e);
        });
        child.stderr.on('data', (e) => {
            reject(e);
        });
        child.on('close', (code) => {
            if(code == 0) {
                resolve();
            }
            else {
                reject(`Child process exited with code ${code}.`);
            }
        });
    });
    
}


function ingestData(fname) {
    return new Promise((resolve, reject) => {
        addMeta(fname, containerLoc).then(() => {
            if(cleanup) {
                cleanupFile(fname).then(() => {
                    resolve(null)
                }, (e) => {
                    resolve(e)
                })
            }
        }, (e) => {
            reject(e)
            //still try to cleanup, but ignore output
            cleanupFile(fname)
        })
    });
    
}


function dataHandler(fname, attempt = 0) {

    return ingestData(fname).then((error) => {
        return error;
    }, (error) => {
        if(attempt++ >= retryLimit) {
            return Promise.reject(error);
        }
        else {
            return ingestData(fname, attempt++);
        }
    });

    
}

source.on("data", (fname) => {

    dataHandler(fname).then((error) => {
        console.log(`Warning: Failed to cleanup file ${fname}\n${error}`);
    }, (error) => {
        console.error(`Error: Failed to ingest file ${fname}\n${error}`);
        if(faults++ >= faultLimit) {
            errorExit(new Error("Fault limit reached. Too many metadata ingestor processes exited with an error."));
        }
    })
    

    tries = 0;

    documentsReceived
    if(++documentsReceived >= documentLimit) {

    }
    
    dataHandler(fname);
});

source.on("end", () => {
    complete();
});

source.on("warning", (message) => {
    console.log(`Warning from controller:\n${message}`);
});

source.on("error", (e) => {
    errorExit(`Error in controller.\n${e.toString()}`);
});



//catch interrupts and uncaught exceptions and cleanup
//still sometimes leaves zombies? Probably not cleaning up coordinator processes properly
//should add cleanup process in coordinator
process.on("SIGINT", function() {
    console.log("Caught interrupt, exitting...");
    cleanupFunct();
    process.exit(0);
});

process.on("uncaughtException", (e) => {
    errorExit(e);
});