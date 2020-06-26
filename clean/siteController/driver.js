const csv = require("csv-parser");
const fs = require("fs");
const schemaTrans = require("./schema_translation");
const schema = require("./doc_schema");
const ingestor = require("../../meta_ingestor");
const path = require("path");
const os = require("os");

//node siteController.js -f ./input/daily_rf_data_2019_11_27.csv -d test -v rainfall -u mm -o ../../output -l 1

//-------------parse args--------------------

let options = {
    dataFile: undefined,
    dataset: undefined,
    valueType: undefined,
    units: undefined,
    outDir: undefined,

    cleanup: true,
    containerLoc: null,
    valueName: "sitevalue",
    metaName: "sitemeta",
    nodata: "NA",
    
    retryLimit: 3,
    faultLimit: 0,

    docLimit: -1,
    metaLimit: -1,
    valueLimit: -1,
    valueLimitI: -1,
    rowLimit: -1,
    
    maxSpawn: -1,

    notificationInterval: -1
}



let helpString = "Available arguments:\n"
+ "-f, --datafile: Required. CSV file containing the site metadata and values.\n"
+ "-d, --dataset: Required. Identifier for dataset being ingested.\n"
+ "-v, --valuetype: Required. Type of the values in this dataset (e.g. rainfall, average temperature).\n"
+ "-u, --units: Required. Units values are represented in.\n"
+ "-o, --output_directory: Required. Directory to write JSON documents and other output.\n"
+ "-s, --max-spawn: Optional. The maximum number of Agave request handler processes to spawn at once. Negative values indicate equal to the number of logical cores on the system minus one (main process). Default value -1.\n"
+ "-nc, --no_cleanup: Optional. Turns off document cleanup after ingestion. JSON output will not be deleted (deleted by default).\n"
+ "-l, --document_limit: Optional. Limit the number of metadata documents to be ingested. Negative value indicates no limit. Default value -1.\n"
+ "-r, --retry_limit: Optional. Limit the number of times to retry a document ingestion on failure before counting it as a fault. Negative value indicates no limit. Default value 3.\n"
+ "-fl, --fault_limit: Optional. Limit the allowable number of metadata ingestion faults before failing. Negative value indicates no limit. Default value 0.\n"
+ "-c, --containerized: Optional. Indicates that the agave instance to be used is containerized and commands will be run using exec with the specified singularity image. Note that faults may not be properly detected when using agave containerization.\n"
+ "-vn, --valuename: Optional. Name to assign to value documents. Default value 'sitevalue'\n"
+ "-mn, --metadataname: Optional. Name to assign to metadata documents. Default value 'sitemeta'\n"
+ "-nd, --nodata: Optional. No data value. Cells with this value will be ignored and no document will be produced. Default value 'NA'.\n"
+ "-ml, --metadatalimit: Optional. Maximum number of metadata documents to stream. Negative value indicates no limit. Default value -1.\n"
+ "-vl, --valuelimit: Optional. Maximum number of value documents to stream. Negative value indicates no limit. Default value -1.\n"
+ "-vli, --valuelimitindividual: Optional. Maximum number of value documents to stream for each row. Negative value indicates no limit. Default value -1.\n"
+ "-rl, --rowlimit: Optional. Maximum number of rows to process. Negative value indicates no limit. Default value -1.\n"
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
        case "-f":
        case "--datafile": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            options.dataFile = value;
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
        case "-v":
        case "--valuetype": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            options.valueType = value;
            break;
        }
        case "-u":
        case "--units": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            options.units = value;
            break;
        }
        case "-vn":
        case "--valuename": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            options.valueName = value;
            break;
        }
        case "-mn":
        case "--metadataname": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            options.metaName = value;
            break;
        }
        case "-nd":
        case "--nodata": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            options.nodata = value;
            break;
        }
        case "-ml":
        case "--metadatalimit": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            let valuei = parseInt(value);
            if(isNaN(valuei)) {
                invalidArgs();
            }
            else {
                options.metaLimit = valuei;
            }
            break;
        }
        case "-vl":
        case "--valuelimit": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            let valuei = parseInt(value);
            if(isNaN(valuei)) {
                invalidArgs();
            }
            else {
                options.valueLimit = valuei;
            }
            break;
        }
        case "-vli":
        case "--valuelimitindividual": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            let valuei = parseInt(value);
            if(isNaN(valuei)) {
                invalidArgs();
            }
            else {
                options.valueLimitI = valuei;
            }
            break;
        }
        case "-rl":
        case "--rowlimit": {
            if(++i >= args.length) {
                invalidArgs();
            }
            value = args[i];
            let valuei = parseInt(value);
            if(isNaN(valuei)) {
                invalidArgs();
            }
            else {
                options.rowLimit = valuei;
            }
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



if(options.dataFile === undefined || options.dataset === undefined || options.valueType === undefined || options.units === undefined || options.outDir === undefined) {
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
if(options.metaLimit < 0) {
    options.metaLimit = Number.POSITIVE_INFINITY;
}
if(options.valueLimit < 0) {
    options.valueLimit = Number.POSITIVE_INFINITY;
}
if(options.valueLimitI < 0) {
    options.valueLimitI = Number.POSITIVE_INFINITY;
}
if(options.rowLimit < 0) {
    options.rowLimit = Number.POSITIVE_INFINITY;
}
if(options.maxSpawn < 0) {
    options.maxSpawn = os.cpus().length;
}


ingestor.setMaxSpawn(options.maxSpawn);


//-------------main--------------------

//set up counters
let metaDocsProcessed = 0;
let valueDocsProcessed = 0;
let rowsProcessed = 0;

let done = false;
let docsComplete = 0;

ingestionErrors = 0;

let docID = 0;


//use headers: false to create custom separation based on index
source = fs.createReadStream(options.dataFile)
.pipe(csv({
    headers: false
}));

let isHeader = true;
let translation = null;
source.on("data", (row) => {
    //first row is header
    if(isHeader) {
        translation = createTranslation(row);
        isHeader = false;
    }
    else {
        if(rowsProcessed++ < options.rowLimit && (metaDocsProcessed < options.metaLimit || valueDocsProcessed < options.valueLimit) && metaDocsProcessed + valueDocsProcessed < options.docLimit) {
            processRow(row, translation);
        }
        else {
            source.destroy();
            allSubmitted();
        }
    }
    
});
source.on("finished", () => {
    allSubmitted();
});



//---------------------------data handling/processing-----------------------------------

function dateParser(date) {
    //remove x at beginning
    let sd = date.slice(1);
    //let's manually convert to iso string so we don't have to worry about js date potentially adding a timezone offset
    let isoDate = sd.replace(/\./g, "-") + "T00:00:00.000Z";
    return isoDate;
}

function createTranslation(header) {
    translationMap = {
        meta: {},
        values: {}
    };
    
    let dateRegex = new RegExp(schemaTrans.date);
    let sknExists = false;
    for(let index in header) {
        field = header[index];
        let translation = schemaTrans.meta[field];
        if(translation) {
            translationMap.meta[index] = translation
            if(translation = "skn") {
                sknExists = true;
            }
        }
        else if(dateRegex.test(field)) {
            //parse date to ISO
            translation = dateParser(field);
            translationMap.values[index] = translation;
        }
        else {
            warning(`Warning: No translation found for header ${field}`);
        }
    }

    if(!sknExists) {
        parseError(`No header translation for "skn" found. Terminating ingestor.`);
    }
    return translationMap;
}


function processRow(row, translationMap) {


    let metaDoc = schema.getMetaTemplate();

    for(let index in translationMap.meta) {
        let translation = translationMap.meta[index];
        let value = row[index];
        metaDoc.setProperty(translation, value);
    }

    metaDoc.setProperty("dataset", options.dataset);

    let skn = metaDoc.getProperty("skn");
    //at least verify skn not no data or empty string
    if(skn == options.nodata || skn == "") {
        warning("Invalid skn, skipping row...");
        return;
    }

    if(metaDocsProcessed < options.metaLimit) {
        let wrappedMetaDoc = {
            name: options.metaName,
            value: metaDoc.toJSON()
        };
    
        let metaDocName = getDocName();
        ingestDoc(metaDocName, wrappedMetaDoc);

        metaDocsProcessed++;
    }

    

    valueDocsProcessedI = 0;
    let indices = Object.keys(translationMap.values);
    let handleRange = [0, indices.length];
    let chunkSize = 100;
    chunkedLoop(handleRange[0], handleRange[1], chunkSize, (i) => {
        let index = indices[i];
        if(valueDocsProcessed >= options.valueLimit || valueDocsProcessedI >= options.valueLimitI || metaDocsProcessed + valueDocsProcessed >= options.docLimit) {
            return false
        }

        let value = row[index];

        if(value == options.nodata) {
            return true;
        }

        valuef = parseFloat(value);

        if(Number.isNaN(valuef)) {
            warning(`Value at row ${row}, column ${index} not 'no data' or parseable as float. Skipping...`);
            return true;
        }

        date = translationMap.values[index];

        let valueDoc = schema.getValueTemplate();
        
        valueDoc.setProperty("skn", skn)
        valueDoc.setProperty("date", date)
        valueDoc.setProperty("value", valuef)
        valueDoc.setProperty("dataset", options.dataset)
        valueDoc.setProperty("type", options.valueType)
        valueDoc.setProperty("units", options.units)

        let wrappedValueDoc = {
            name: options.metaName,
            value: valueDoc.toJSON()
        };

        let valueDocName = getDocName();
        ingestDoc(valueDocName, wrappedValueDoc);

        valueDocsProcessedI++;
        valueDocsProcessed++;

        return true;
    });
    
}

function chunkedLoop(start, end, chunkSize, routine) {
    let pos = start;
    continueLoop = true;
    for(let i = 0; i < chunkSize && pos < end; i++, pos++) {
        continueLoop = routine(pos);
        if(!continueLoop) {
            break;
        }
    }
    if(pos < end && continueLoop) {
        setImmediate(() => {
            chunkedLoop(pos, end, chunkSize, routine);
        });
    }
    
}


function ingestDoc(docName, doc) {
    ingestor.dataHandler(docName, doc, options.retryLimit, options.cleanup, options.containerLoc).then((e) => {
        if(e) {
            warning(`Failed to cleanup file ${docName}\n${e.toString()}`);
        }
        docIngested();
    }, (e) => {
        error(e);
        if(ingestionErrors++ >= options.faultLimit) {
            errorExit(`Fault limit reached.`);
        }
    });
}


function getDocName() {
    dir = options.outDir;
    fname = `doc_${docID}.json`;
    fpath = path.join(dir, fname);
    docID++;
    return fpath;
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
    console.trace();
    cleanup();
    exit(1);
}

//-----------------------cleanup and output aux------------------------------

function exit(code = 0) {
    process.exit(code);
}

function cleanup() {
    //ends stream and releases resources
    source.destroy();
}

function allSubmitted() {
    done = true;
    source.destroy();
}

function docIngested() {
    docsComplete++;
    if(docsComplete % options.notificationInterval == 0) {
        console.log(`Completed ingesting ${docsComplete} docs.`);
    }
    if(done && docsComplete >= metaDocsProcessed + valueDocsProcessed) {
        ingestionComplete();
    }
}

function ingestionComplete() {
    console.log("Complete!");
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

