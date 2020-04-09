const {chain}  = require('stream-chain');
const {parser} = require('stream-csv-as-json');
const {asObjects} = require('stream-csv-as-json/AsObjects');
const {streamValues} = require('stream-json/streamers/StreamValues');
const GenericModule = require("../../genericModule");
const fs = require("fs");
const {EventEmitter} = require("events");
const schemaTrans = require("./schema_translation");
const schema = require("./doc_schema");


module.exports = class SiteControllerModule extends GenericModule {
    
    constructor(options) {
        let defaultOpts = {
            dataFile: undefined,
            dataset: undefined,
            valueType: undefined,
            units: undefined,
            valueName: "site_value",
            metaName: "site_meta",
            nodata: "NA",
            metaLimit: -1,
            valueLimit: -1,
            valueLimitIndividual: -1,
            rowLimit: -1
        };
        //check options and set default values
        for(let item in defaultOpts) {
            let value = defaultOpts[item];
            if(options[item] === undefined) {
                //no default, option is required
                if(value === undefined) {
                    throw new Error(`Invalid options, ${item} not defined`);
                }
                //set default
                else {
                    options[item] = value;
                }
            }
        }

        //convert limits less than zero to infinite
        if(options.metaLimit < 0) {
            options.metaLimit = Number.POSITIVE_INFINITY;
        }
        if(options.valueLimit < 0) {
            options.valueLimit = Number.POSITIVE_INFINITY;
        }
        if(options.valueLimitIndividual < 0) {
            options.valueLimitIndividual = Number.POSITIVE_INFINITY;
        }
        if(options.rowLimit < 0) {
            options.rowLimit = Number.POSITIVE_INFINITY;
        }

        let source = new EventEmitter();
        super(source, options);

        this.paused = false;
        this._metaKeys = [];
        this._valueKeys = [];
        this._metaDocsProcessed = 0;
        this._valueDocsProcessed = 0;
        this._rowsProcessed = 0;
        this.destroyed = false;

        this._csvSource = fs.createReadStream(this._options.dataFile);
        this._pipeline = chain([
            this._csvSource,
            parser(),
            asObjects(),
            //this._transformKeys.bind(this),
            streamValues(),
            this._stripValue.bind(this),
            this._convertToDocs.bind(this)
        ]);

        this._pipeline.on("data", (data) => {
            source.emit("data", data);
        });
        this._pipeline.on("error", (e) => {
            source.emit("error", `Error on pipeline\n${e}`);
        });
        this._pipeline.on("close", () => {
            this._source.emit("close");
        });
        this._pipeline.on("finish", () => {
            this._source.emit("finish");
        });
    }

    _stripValue(data) {
        return data.value;
    }

    //wrap the documents with their type (meta or value)
    _wrapDocument(type, value) {
        let wrapped = {
            type: type,
            value: value
        };
        return wrapped;
    }

    
    _endStream() {
        //emit finished signal
        this._source.emit("finish");
        //destroy the stream to end it
        this.destroy();
    }

    //generator to convert data to docs, returns meta doc first, then value docs
    * _convertToDocs(data) {
        //exceeded limit, complete stream
        if(this._rowsProcessed >= this._options.rowLimit || (this._metaDocsProcessed >= this._options.metaLimit && this._valueDocsProcessed >= this._options.valueLimit)) {
            this._endStream();
            return null;
        }

        let individualValueDocsProcessed = 0;

        let translations = this._translateKeys(Object.keys(data));

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
        if(this._metaDocsProcessed < this._options.metaLimit) {
            //set dataset
            metaDoc.setProperty("dataset", this._options.dataset);
            let wrappedMeta = {
                name: this._options.metaName,
                value: metaDoc.toJSON()
            };
            this._metaDocsProcessed++;
            //send off metadata doc
            yield wrappedMeta;
        }

        //construct and yield value docs
        let valueTranslations = translations.value;
        for(let key in valueTranslations) {
            if(this._valueDocsProcessed >= this._options.valueLimit || individualValueDocsProcessed >= this._options.valueLimitIndividual) {
                break;
            }
            let date = valueTranslations[key];
            let value = data[key];
            let wrappedValue = this._constructAndWrapValueDoc(skn, date, value);
            if(wrappedValue !== null) {
                this._valueDocsProcessed++;
                individualValueDocsProcessed++;
                //send out value doc
                yield wrappedValue;
            }
        }
        this._rowsProcessed++;
    }



    _constructAndWrapValueDoc(skn, date, value) {
        let valueDoc = schema.getValueTemplate();
        let wrappedValue = null;
        //if nodata then return null (should be ignored)
        if(value != this._options.nodata) {
            //value should be numeric
            let valuef = parseFloat(value);
            //value not numeric, send warning and skip
            if(Number.isNaN(valuef)) {
                this._source.emit("warning", `Value not 'no data' or parseable as float. Skipping...`);
            }
            else {
                //gather value fields
                let valueFields = {
                    skn: skn,
                    date: date,
                    value: valuef,
                    dataset: this._options.dataset,
                    type: this._options.valueType,
                    units: this._options.units
                }
                //set values in doc
                for(let field in valueFields) {
                    let docValue = valueFields[field];
                    if(!valueDoc.setProperty(field, docValue)) {
                        //emit warning to source if could not set value in doc
                        this._source.emit("warning", `Could not set property ${label}, not found in template.`);
                    }
                }
                wrappedValue = {
                    name: this._options.valueName,
                    value: valueDoc.toJSON()
                };
            }
        }
        //send out value doc
        return wrappedValue;
    }


    _dateParser(date) {
        //remove x at beginning
        let sd = date.slice(1);
        //let's manually convert to iso string so we don't have to worry about js date potentially adding a timezone offset
        let isoDate = sd.replace(/\./g, "-") + "T00:00:00.000Z";
        return isoDate;
    }

    _translateKeys(keys) {
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
                translation = this._dateParser(key);
                translations.value[key] = translation;
            }
            else {
                this._source.emit("warning", `No translation for key ${key}, check schema.`);
            }
        }

        return translations;
    }

    _test(data) {
        console.log(data);
        process.exit(0);
    }


    pause() {
        //pause data source and pipeline if not already paused
        if(!this.paused && !this.destroyed) {
            this.paused = true;
            this._csvSource.pause();
            this._pipeline.pause();
        }
        
    }

    resume() {
        //resume everything if paused
        if(this.paused && !this.destroyed) {
            this.paused = false;
            this._csvSource.resume();
            this._pipeline.resume();
        }
    }

    //emit close event
    destroy() {
        this.destroyed = true;
        this._pipeline.destroy();
        this._csvSource.destroy();
    }

    //specifies how to parse command line arguments to option, returns key value pair as array of length 2, a help message if help flag, or null if invalid
    static parseArgs(args) {

        console.log(`\n\n\n${args}\n\n\n`);

        let helpMessage = "Available arguments:\n"
        + "-f, --data_file: Required. CSV file containing the site metadata and values.\n"
        + "-d, --dataset: Required. Identifier for dataset being ingested.\n"
        + "-v, --value_type: Required. Type of the values in this dataset (e.g. rainfall, average temperature).\n"
        + "-u, --units: Required. Units values are represented in.\n"
        + "-vn, --value_name: Optional. Name to assign to value documents. Default value 'site_value'\n"
        + "-mn, --metadata_name: Optional. Name to assign to metadata documents. Default value 'site_meta'\n"
        + "-nd, --nodata: Optional. No data value. Cells with this value will be ignored and no document will be produced. Default value 'NA'.\n"
        + "-ml, --metadata_limit: Optional. Maximum number of metadata documents to stream. Negative value indicates no limit. Default value -1.\n"
        + "-vl, --value_limit: Optional. Maximum number of value documents to stream. Negative value indicates no limit. Default value -1.\n"
        + "-vli, --value_limit_individual: Optional. Maximum number of value documents to stream for each row. Negative value indicates no limit. Default value -1.\n"
        + "-rl, --row_limit: Optional. Maximum number of rows to process. Negative value indicates no limit. Default value -1.\n"
        + "-h, --help: Show this message.\n"




        let result = {
            success: true,
            result: {}
        };

        let setHelp = (error) => {
            if(error) {
                result.success = false;
            }
            result.result = helpMessage;
        }

        argLoop:
        for(let i = 0; i < args.length; i++) {
            switch(args[i]) {
                case "-f":
                case "--data_file": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    result.result.dataFile = args[i];
                    break;
                }
                case "-d":
                case "--dataset": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    result.result.dataset = args[i];
                    break;
                }
                case "-v":
                case "--value_type": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    result.result.valueType = args[i];
                    break;
                }
                case "-u":
                case "--units": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    result.result.units = args[i];
                    break;
                }
                case "-vn":
                case "--value_name": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    result.result.valueName = args[i];
                    break;
                }
                case "-mn":
                case "--metadata_name": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    result.result.metaName = args[i];
                    break;
                }
                case "-nd":
                case "--nodata": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    result.result.nodata = args[i];
                    break;
                }
                case "-ml":
                case "--metadata_limit": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    let valuei = parseInt(value);
                    if(isNaN(valuei)) {
                        setHelp(true);
                        break argLoop;
                    }
                    else {
                        result.result.metaLimit = args[i];
                    }
                    break;
                }
                case "-vl":
                case "--value_limit": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    let valuei = parseInt(value);
                    if(isNaN(valuei)) {
                        setHelp(true);
                        break argLoop;
                    }
                    else {
                        result.result.valueLimit = args[i];
                    }
                    break;
                }
                case "-vli":
                case "--value_limit_individual": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    let valuei = parseInt(value);
                    if(isNaN(valuei)) {
                        setHelp(true);
                        break argLoop;
                    }
                    else {
                        result.result.valueLimitIndividual = args[i];
                    }
                    break;
                }
                case "-rl":
                case "--row_limit": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    let valuei = parseInt(value);
                    if(isNaN(valuei)) {
                        setHelp(true);
                        break argLoop;
                    }
                    else {
                        result.result.rowLimit = args[i];
                    }
                    break;
                }
                case "-h":
                case "--help": {
                    setHelp(false);
                    break argLoop;
                }
                default: {
                    setHelp(true);
                    break argLoop;
                }
            }
        }

        return result;
    }

}