const path = require("path");
const schemaTrans = require("./schema_translation");
const schema = require("./doc_schema");
const csvParser = require("./csv_parser");
const {fork} = require("child_process");

let dataFile = null;
let outDir = null;

let noData = "NA";

let cleanup = true;

let metaLimit = -1;
let valueLimit = -1;
let valueLimitI = -1;
let maxSpawn = 50;

let docNames = {
    meta: "meta_test",
    value: "value_test"
};

//-------------parse args--------------------

let helpString = "Available arguments:\n"
+ "-f || --input-file: Required. CSV to convert to documents.\n"
+ "-o || --output_directory: Required. Directory to write JSON documents and other output.\n"
+ "-mn || --meta_document_name: Optional. Name for site metadata documents. Default value 'meta_test'.\n"
+ "-vn || --value_document_name: Optional. Name for site value documents. Default value 'value_test'.\n"
+ "-nd || --nodata_value: Optional. No data value in input document. Default value 'NA'.\n"
+ "-nc || --no_cleanup: Optional. Turns off document cleanup after ingestion. JSON output will not be deleted (deleted by default).\n"
+ "-ml || --metadata_document_limit: Optional. Limit the number of metadata documents to be ingested. Negative value indicates no limit. Default value -1.\n"
+ "-vl || --value_document_limit: Optional. Limit the number of value documents to be ingested. Negative value indicates no limit. Default value -1.\n"
+ "-vli || --value_document_limit_individual: Optional. Limit the number of value documents to be ingested per rainfall station. Negative value indicates no limit. Default value -1.\n"
+ "-s || --max_spawn: Optional. The maximum number of ingestor processes to spawn at once. Note that the total number of processes will be n+2 (main process and coordinator process). Default value 50.\n"
+ "-h || --help: Show this message.\n";

function invalidArgs() {
    console.error(helpString);
    process.exit(1);
}

function helpAndTerminate() {
    console.log(helpString);
    process.exit(0);
}

let args = process.argv.slice(2);
for(let i = 0; i < args.length; i++) {
    switch(args[i]) {
        case "-f":
        case "--input_file": {
            //get next arg, ensure not out of range
            if(++i >= args.length) {
                invalidArgs();
            }
            dataFile = args[i];
            break;
        }
        case "-o":
        case "-output_directory": {
            if(++i >= args.length) {
                invalidArgs();
            }
            outDir = args[i];
            break;
        }
        case "-mn":
        case "--meta_document_name": {
            //get next arg, ensure not out of range
            if(++i >= args.length) {
                invalidArgs();
            }
            docNames.meta = args[i];
            break;
        }
        case "-vn":
        case "--value_document_name": {
            //get next arg, ensure not out of range
            if(++i >= args.length) {
                invalidArgs();
            }
            docNames.value = args[i];
            break;
        }
        case "-nd":
        case "--nodata_value": {
            //get next arg, ensure not out of range
            if(++i >= args.length) {
                invalidArgs();
            }
            noData = args[i];
            break;
        }
        case "-nc":
        case "--no_cleanup": {
            cleanup = false;
            break;
        }
        case "-ml":
        case "--matadata_document_limit": {
            if(++i >= args.length) {
                invalidArgs();
            }
            metaLimit = parseInt(args[i]);
            if(isNaN(metaLimit)) {
                invalidArgs();
            }
            break;
        }
        case "-vl":
        case "--value_document_limit": {
            if(++i >= args.length) {
                invalidArgs();
            }
            valueLimit = parseInt(args[i]);
            if(isNaN(valueLimit)) {
                invalidArgs();
            }
            break;
        }
        case "-vli":
        case "--value_document_limit_individual": {
            if(++i >= args.length) {
                invalidArgs();
            }
            valueLimitI = parseInt(args[i]);
            if(isNaN(valueLimitI)) {
                invalidArgs();
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

//need to specify data file and output directory
if(dataFile == null || outDir == null) {
    invalidArgs();
}

//convert negative limits to infinity for easier comparisons
if(metaLimit < 0) {
    metaLimit = Number.POSITIVE_INFINITY;
}
if(valueLimit < 0) {
    valueLimit = Number.POSITIVE_INFINITY;
}
if(valueLimitI < 0) {
    valueLimitI = Number.POSITIVE_INFINITY;
}


//-------------end parse args--------------------


//just use sequential ids, also serves as a counter for the number of docs for exiting
let docID = 0;
let returned = 0;
let allSent = false;

let metaSent = 0;
let valueSent = 0;

function sendData(metadata, type) {
    let name = docNames[type];
    if(name == undefined) {
        console.error(`Error: Document type ${type} is not defined. Could not add metadata document.`);
    }
    wrappedMeta = {
        name: name,
        value: metadata
    };
    let id = docID++;
    let fname = path.join(outDir, `metadoc_${id}.json`);
    let message = {
        id: id,
        data: JSON.stringify(wrappedMeta),
        fname: fname,
        cleanup: cleanup
    };
    ingestionCoordinator.send(message, (e) => {
        if(e) {
            console.error(`Error: Failed to send message.\nID: ${message.id}\nReason: ${e.toString()}\n`);
        }
    });
}

function complete() {
    ingestionCoordinator.send(null);
    allSent = true;
}



function dateParser(date) {
    //remove x at beginning
    let sd = date.slice(1);
    //let's manually convert to iso string so we don't have to worry about js date potentially adding a timezone offset
    let isoDate = sd.replace(/\./g, "-") + ":00:00:00.000Z";
    return isoDate;
}


let ingestionCoordinator = fork("ingestion_coord.js", [maxSpawn.toString()], {stdio: "pipe"});

function errorExit(e) {
    console.error(`An error has occurred, the process will exit.\n${e.toString()}`);
    if(ingestionCoordinator) {
        ingestionCoordinator.kill("SIGABRT");
    }
    process.exit(1);
}

//push errors from coordination thread to stderr
ingestionCoordinator.stderr.on("data", (chunk) => {
    console.error(`Error in coordinator process: ${chunk.toString()}`);
});

//if coordination thread exits with an error code exit process imediately
ingestionCoordinator.on("exit", (code) => {
    if(code > 0) {
        console.error(`Error: Coordinator process has exited with a non-zero exit code. Error code ${code}.`);
    }
    process.exit(1);
});

ingestionCoordinator.on("message", (message) => {
    if(!message.result.success) {
        console.error(`Error: Metadata ingestion failed.\nID: ${message.id}\nPOF: ${message.result.pof}\nReason: ${message.result.error}\n`);
    }
    else if(message.result.pof != null) {
        console.log(`Warning: An error occured after metadata insertion.\nID: ${message.id}\nPOF: ${message.result.pof}\nReason: ${message.result.error}\n`);
    }

    if(++returned >= docID && allSent) {
        console.log("Complete!");
        process.exit(0);
    }
});

csvParser.parseCSV(dataFile, true).then((data) => {
    //run through trim map to remove any extraneous whitespace that may have been left in the file
    let headers = data.headers.map((header) => {
        return header.trim();
    });
    let dataRows = data.values.map((row) => {
        return row.map((value) => {
            return value.trim();
        });
    });

    dateRegex = new RegExp(schemaTrans.date);

    for(let i = 0; i < dataRows.length; i++) {
        //if both limits reached just break, no need to process the rest of the rows
        if(metaSent >= metaLimit && valueSent >= valueLimit) {
            break;
        }

        let row = dataRows[i];

        let metadata = {};
        let values = {};

        headers.forEach((label, j) => {
            let value = row[j];
            let docLabel = schemaTrans.meta[label];
            if(docLabel != undefined) {
                metadata[docLabel] = value;
            }
            else if(dateRegex.test(label)) {
                //if no data don't generate a document, just skip
                if(value != noData) {
                    let date = dateParser(label);
                    //probably want the value to be stored numerically
                    let valuef = parseFloat(value);
                    if(Number.isNaN(valuef)) {
                        console.log(`Warning: Value not 'no data' or parseable as float. Skipping...`);
                    }
                    else {
                        values[date] = valuef;
                    }
                }
            }
            else {
                console.log(`Warning: No translation for label ${label}, check schema. Skipping column...`);
            }
        });

        //generate and add metadata doc and value docs
        let metaDoc = schema.getMetaTemplate();
        Object.keys(metadata).forEach((label) => {
            if(!metaDoc.setProperty(label, metadata[label])) {
                console.log(`Warning: Could not set property ${label}, not found in template.`);
            }
        });
        
        //at least verify skn exists
        let skn = metaDoc.getProperty("skn");
        if(skn == undefined || skn == null) {
            console.log(`Warning: SKN not set. Skipping row...`);
        }
        else {
            //send site metadata to ingestor if limit not reached
            if(metaSent < metaLimit) {
                sendData(metaDoc.toJSON(), "meta");
                metaSent++;
            }

            
            //value docs
            valueFields = {
                skn: skn,
                date: null,
                value: null
            }
            let dates = Object.keys(values);
            //iterate over values and check if value limit reached for both individual station and total
            for(let i = 0; i < dates.length, i < valueLimitI, valueSent < valueLimit; i++, valueSent++) {
                date = dates[i];
                valueFields.date = date;
                valueFields.value = values[date];
                let valueDoc = schema.getValueTemplate();
                Object.keys(valueFields).forEach((label) => {
                    if(!valueDoc.setProperty(label, valueFields[label])) {
                        console.log(`Warning: Could not set property ${label}, not found in template.`);
                    }
                });

                //send value to ingestor
                sendData(valueDoc.toJSON(), "value");
            }
        }
        
    }

    complete();
}, (e) => {
    errorExit(e);
});