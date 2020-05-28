const csv = require('csv-parser')
const fs = require("fs");
const {EventEmitter} = require("events");
const schemaTrans = require("./schematranslation");
const schema = require("./docschema");
const ingestor = require("../../metaingestor");


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
    faultLimit: -1,

    docLimit: -1,
    metaLimit: -1,
    valueLimit: -1,
    valueLimitI: -1,
    rowLimit: -1,

    notificationInterval: -1
}



let helpString = "Available arguments:\n"
+ "-f, --datafile: Required. CSV file containing the site metadata and values.\n"
+ "-d, --dataset: Required. Identifier for dataset being ingested.\n"
+ "-v, --valuetype: Required. Type of the values in this dataset (e.g. rainfall, average temperature).\n"
+ "-u, --units: Required. Units values are represented in.\n"
+ "-o, --output_directory: Required. Directory to write JSON documents and other output.\n"
+ "-nc, --no_cleanup: Optional. Turns off document cleanup after ingestion. JSON output will not be deleted (deleted by default).\n"
+ "-l, --document_limit: Optional. Limit the number of metadata documents to be ingested. Negative value indicates no limit. Default value -1.\n"
+ "-rl, --retry_limit: Optional. Limit the number of times to retry a document ingestion on failure before counting it as a fault. Negative value indicates no limit. Default value 3.\n"
+ "-fl, --fault_limit: Optional. Limit the number of metadata ingestion faults before failing. Negative value indicates no limit. Default value -1.\n"
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
        case "-rl":
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




//-------------main--------------------

//set up counters
let metaDocsProcessed = 0;
let valueDocsProcessed = 0;
let rowsProcessed = 0;


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
        processRow(row, translation);
    }
    
});




//---------------------------data handling/processing-----------------------------------

function createTranslation(header) {
    translationmap = {
        meta: {},
        values: {}
    };
    
    let dateRegex = new RegExp(schemaTrans.date);
    let sknExists = false;
    for(let index in header) {
        field = header[index];
        let translation = schemaTrans.meta[field];
        if(translation) {
            translationmap.meta[index] = translation
            if(translation = "skn") {
                sknExists = true;
            }
        }
        else if(dateRegex.test(key)) {
            //parse date to ISO
            translation = dateParser(key);
            translations.values[index] = translation;
        }
        else {
            warning(`Warning: No translation found for header ${field}`);
        }
    }

    if(!sknExists) {
        parseError(`No header translation for "skn" found. Terminating ingestor.`);
    }
    return translationmap;
}


function processRow(row, translation) {
    row++;

    let metaDoc = schema.getMetaTemplate();

    for(let index in translation.meta) {
        let translation = translation.meta[index];
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

    let wrappedMetaDoc = {
        name: options.metaName,
        value: metaDoc.toJSON()
    };

    let metaDocName = getDocName();
    ingestor.dataHandler(metaDocName, wrappedMetaDoc).then((error) => {
        if(error) {
            warning(`Failed to cleanup file ${metaDocName}\n${error.toString()}`);
        }
        ingestionFinished(row, "meta");
    }, (error) => {
        ingestionError(error);
    });

    for(let index in translation.values) {
        let value = row[index];

        if(value == options.nodata) {
            continue;
        }

        valuef = parseFloat(value);

        if(Number.isNaN(valuef)) {
            warning(`Value at row ${row}, column ${index} not 'no data' or parseable as float. Skipping...`);
            continue;
        }

        date = translation.values[index];

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
        ingestor.dataHandler(valueDocName, wrappedValueDoc).then((error) => {
            if(error) {
                warning(`Failed to cleanup file ${valueDocName}\n${error.toString()}`);
            }
            ingestionFinished(row, "meta");
        }, (error) => {
            ingestionError(error);
        });
    }
    
}


//--------------------------error/warning handling---------------------------------------

function errorExit(e) {
    console.error(e);
    cleanup();
    process.exit(1);
}

//-----------------------cleanup------------------------------

function cleanup() {
    //ends stream and releases resources
    source.destroy();
}




module.exports = class SiteControllerModule extends GenericModule {
    
    constructor(options) {

        paused = false;
        metaKeys = [];
        valueKeys = [];
        metaDocsProcessed = 0;
        valueDocsProcessed = 0;
        rowsProcessed = 0;
        destroyed = false;

        
    }

    row = 0;
    processRow(row, translation) {
        row++;

        let metaDoc = schema.getMetaTemplate();

        for(let index in translation.meta) {
            let translation = translation.meta[index];
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

        let wrappedMetaDoc = {
            name: options.metaName,
            value: metaDoc.toJSON()
        };

        let metaDocName = getDocName();
        ingestor.dataHandler(metaDocName, wrappedMetaDoc).then((error) => {
            if(error) {
                warning(`Failed to cleanup file ${metaDocName}\n${error.toString()}`);
            }
            ingestionFinished(row, "meta");
        }, (error) => {
            ingestionError(error);
        });

        for(let index in translation.values) {
            let value = row[index];

            if(value == options.nodata) {
                continue;
            }

            valuef = parseFloat(value);

            if(Number.isNaN(valuef)) {
                warning(`Value at row ${row}, column ${index} not 'no data' or parseable as float. Skipping...`);
                continue;
            }

            date = translation.values[index];

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
            ingestor.dataHandler(valueDocName, wrappedValueDoc).then((error) => {
                if(error) {
                    warning(`Failed to cleanup file ${valueDocName}\n${error.toString()}`);
                }
                ingestionFinished(row, "meta");
            }, (error) => {
                ingestionError(error);
            });
        }
        
    }

    ingestionFinished(row, doc) {
        source.emit("dataingested", );
    }

    //need to add in base path
    docID = 0;
    getDocName() {
        return `doc${docID++}.json`;
    }


    dateParser(date) {
        //remove x at beginning
        let sd = date.slice(1);
        //let's manually convert to iso string so we don't have to worry about js date potentially adding a timezone offset
        let isoDate = sd.replace(/\./g, "-") + "T00:00:00.000Z";
        return isoDate;
    }


    warning(warning) {
        source.emit("warning", warning);
    }

    error(error) {
        source.emit("error", error);
    }

    parseError(error) {
        source.emit("parseerror", error);
        destroy();
    }

    ingestionError(error) {
        source.emit("ingestionerror", error);
    }

    createTranslation(header) {
        translationmap = {
            meta: {},
            values: {}
        };
        
        let dateRegex = new RegExp(schemaTrans.date);
        let sknExists = false;
        for(let index in header) {
            field = header[index];
            let translation = schemaTrans.meta[field];
            if(translation) {
                translationmap.meta[index] = translation
                if(translation = "skn") {
                    sknExists = true;
                }
            }
            else if(dateRegex.test(key)) {
                //parse date to ISO
                translation = dateParser(key);
                translations.values[index] = translation;
            }
            else {
                warning(`Warning: No translation found for header ${field}`);
            }
        }

        if(!sknExists) {
            parseError(`No header translation for "skn" found. Terminating ingestor.`);
        }
        return translationmap;
    }


    
    finish() {
        source.emit("close");
        //emit finished signal
        source.emit("finish");
    }

    //generator to convert data to docs, returns meta doc first, then value docs
    * convertToDocs(data) {
        //exceeded limit, complete stream
        if(rowsProcessed >= options.rowLimit || (metaDocsProcessed >= options.metaLimit && valueDocsProcessed >= options.valueLimit)) {
            endStream();
            return null;
        }

        let individualValueDocsProcessed = 0;

        let translations = translateKeys(Object.keys(data));

        //construct and yield metadata doc
        let metaDoc = schema.getMetaTemplate();
        let metaTranslations = translations.meta;
        for(let key in metaTranslations) {
            let translation = metaTranslations[key];
            let value = data[key];
            metaDoc.setProperty(translation, value);
        }

        //need skn for ref in value docs
        let skn = metaDoc.getProperty("skn");
        //at least verify skn exists
        if(skn == undefined || skn == null) {
            throw new Error("SKN not found");
        }

        //if hit metadoc limit then don't complete and send off metadata doc
        if(metaDocsProcessed < options.metaLimit) {
            //set dataset
            metaDoc.setProperty("dataset", options.dataset);
            let wrappedMeta = {
                name: options.metaName,
                value: metaDoc.toJSON()
            };
            metaDocsProcessed++;
            //send off metadata doc
            yield wrappedMeta;
        }

        //construct and yield value docs
        let valueTranslations = translations.value;
        for(let key in valueTranslations) {
            if(valueDocsProcessed >= options.valueLimit || individualValueDocsProcessed >= options.valueLimitIndividual) {
                break;
            }
            let date = valueTranslations[key];
            let value = data[key];
            let wrappedValue = constructAndWrapValueDoc(skn, date, value);
            if(wrappedValue !== null) {
                valueDocsProcessed++;
                individualValueDocsProcessed++;
                //send out value doc
                yield wrappedValue;
            }
        }
        rowsProcessed++;
    }



    constructAndWrapValueDoc(skn, date, value) {
        let valueDoc = schema.getValueTemplate();
        let wrappedValue = null;
        //if nodata then return null (should be ignored)
        if(value != options.nodata) {
            //value should be numeric
            let valuef = parseFloat(value);
            //value not numeric, send warning and skip
            if(Number.isNaN(valuef)) {
                source.emit("warning", `Value not 'no data' or parseable as float. Skipping...`);
            }
            else {
                //gather value fields
                let valueFields = {
                    skn: skn,
                    date: date,
                    value: valuef,
                    dataset: options.dataset,
                    type: options.valueType,
                    units: options.units
                }
                //set values in doc
                for(let field in valueFields) {
                    let docValue = valueFields[field];
                    if(!valueDoc.setProperty(field, docValue)) {
                        //emit warning to source if could not set value in doc
                        source.emit("warning", `Could not set property ${label}, not found in template.`);
                    }
                }
                wrappedValue = {
                    name: options.valueName,
                    value: valueDoc.toJSON()
                };
            }
        }
        //send out value doc
        return wrappedValue;
    }


   

    translateKeys(keys) {
        let translations = {
            meta: {},
            value: {}
        }
        let dateRegex = new RegExp(schemaTrans.date);
        
        for(let key of keys) {
            let translation = schemaTrans.meta[key];
            if(translation !== undefined) {
                translations.meta[key] = translation;
            }
            else if(dateRegex.test(key)) {
                //parse date to ISO
                translation = dateParser(key);
                translations.value[key] = translation;
            }
            else {
                source.emit("warning", `No translation for key ${key}, check schema.`);
            }
        }

        return translations;
    }


    // pause() {
    //     //pause data source and pipeline if not already paused
    //     if(!paused && !destroyed) {
    //         paused = true;
    //         csvSource.pause();
    //         pipeline.pause();
    //     }
        
    // }

    // resume() {
    //     //resume everything if paused
    //     if(paused && !destroyed) {
    //         paused = false;
    //         csvSource.resume();
    //         pipeline.resume();
    //     }
    // }

    //emit close event
    destroy() {
        source.destroy();
        destroyed = true;

    }

}


