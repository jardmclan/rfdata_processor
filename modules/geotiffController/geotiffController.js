
const fs = require("fs");
const GenericModule = require("../../genericModule");
//index file for geotiffs, put as a cmd arg
// const geotiffIndex = require("./input/index.json");
const {parser} = require("stream-json");
const {pick} = require('stream-json/filters/Pick');
const streamArray = require("stream-json/streamers/StreamArray");
const {chain} = require("stream-chain");
const {join} = require("path");
const {EventEmitter} = require("events");
const {fork} = require("child_process");

const processorFile = join(__dirname, "geotiffProcessor.js");

module.exports = class GeotiffControllerModule extends GenericModule {
    
    /*
        options structure:
        {
            maxSpawn?: number default 5,
            //this is in json documents not actual size
            highWaterMark?: number, default 100M
            dataRoot: string,
            indexFile: string
        }
    */
    constructor(options) {
        let defaultOpts = {
            maxSpawn: 5,
            highWaterMark: "100M",
            createHeader: true,
            headerName: "raster_header",
            valueName: "value_map",
            dataRoot: undefined,
            indexFile: undefined
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


        //parse highwatermark arg to bytes
        let parseToBytes = (size) => {
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

        //convert high water mark string to number of bytes (if string)
        if(typeof options.highWaterMark == "string") {
            options.highWaterMark = parseToBytes(options.highWaterMark);
        }

        //convert limits less than zero to infinite
        if(options.maxSpawn < 0) {
            options.maxSpawn = Number.POSITIVE_INFINITY;
        }
        if(options.highWaterMark < 0) {
            options.highWaterMark = Number.POSITIVE_INFINITY;
        }
        

        let source = new EventEmitter();
        super(source, options);
        

        this.paused = false;

        this._pauseEmitter = new EventEmitter();
        this._pipelinePauseLock = 0;
        this._queue = [];
        this._first = true;
        this._fallbackIndexQueue = [];
        this._spawned = 0;
        this._totalDocs = 0;
        this._countComplete = false;
        this._completeDocs = 0;
        //assign sequential id to each processor for reference in processors storage
        this._processorID = 0;
        this._processors = {};
        this.destroyed = false;
        this._queueSizeInBytes = 0;
        this._throttled = false;

        this._indexSource = fs.createReadStream(this._options.indexFile);

        this._pipeline = chain([
            this._indexSource,
            parser(),
            pick({filter: "index"}),
            new streamArray(),
            //remove array indexes
            this._getSubfield("value"),
            //filter only statewide maps and remove spatial extent field since not needed
            this._filterField(["descriptor", "spatialExtent"], this._getValueFilter(["St"]), true),
            this._count.bind(this)
        ]);

        //wrap to properly lexically bind this and pass through event callback
        let wrapper = function(index) {
            this._firstIndexCB(index, wrapper);
        }.bind(this);

        //process first one that comes through separately to set header for verification
        this._pipeline.on("data", wrapper);
        this._pipeline.on("finish", () => {
            this._countComplete = true;
        });
        this._pipeline.on("error", (e) => {
            //forward error to source
            this._source.emit("error", `Error on pipeline\n${e}`);
        });
    }

    //------------------pipeline methods----------------------------------------

    _count(data) {
        this._totalDocs++;
        return data;
    }

    //second pick not working (why?), just create manual subfield filter
    _getSubfield(field) {
        return (data) => {
            return data[field];
        };
    }

    _getValueFilter(truthyValues) {
        return (value) => {
            return truthyValues.indexOf(value) >= 0;
        };
    }

    _filterField(fieldPath, filter, remove) {
        return (data) => {
            let modObject = data;
            for(let i = 0; i < fieldPath.length - 1; i++) {
                modObject = modObject[fieldPath[i]];
            }
            let field = fieldPath[fieldPath.length - 1];
            let filtered = null;
            if(filter(modObject[field])) {
                if(remove) {
                    delete modObject[field];
                }
                filtered = data;
            }
            return filtered;
        };
    }

    //------------------end pipeline methods-------------------------------------

    //eventCB represents the callback used to bind to the first data event so it can be removed (had to be wrapped for proper binding)
    _firstIndexCB(index, eventCB) {
        if(this._first) {
            this._first = false;
            //pause pipeline so don't get data while processing first
            this._pausePipeline();
            this._processFirstIndex(index).then((header) => {
                //process the rest of the index documents coming from pipeline
                this._pipeline.on("data", (index) => {
                    //add header to index
                    index.header = header;
                    this._processIndex(index);
                });
                //remove this listener and use the new one just constructed
                this._pipeline.off("data", eventCB);
                //process any extra indexes from fallback queue
                for(let item of this._fallbackIndexQueue) {
                    this._processIndex(item);
                }
                //resume pipeline
                this._tryResumePipeline();
            }, (e) => {
                this._source.emit("error", `Error processing initial geotiff file.\n${e.toString()}`);
            });
        }
        //on the off chance that multiple data events are emitted before pause called add extras to fallback queue (might be theoretically possible? best to be safe)
        else {
            this._fallbackIndexQueue.push(index);
        }
    };

    //process first one that comes through separately to set header for verification
    _processFirstIndex(index) {
        return new Promise((resolve, reject) => {
            let fpath = join(this._options.dataRoot, index.fpath);
            let processor = fork(processorFile, [fpath], {stdio: "pipe"});
            processor.on("message", (data) => {
                //if header document should be sent, then send off header document
                if(this._options.createHeader) {
                    //add one to total documents counter since this won't be caught in pipelines count method
                    this._totalDocs++;
                    this._postprocessHeader(index, data.header);
                }
                //send off value map document
                this._postprocess(index, data.values);
                resolve(data.header);
            });
            processor.stderr.on("data", (chunk) => {
                reject(chunk);
            });
        });
    }

    
    //multiple sources can pause pipeline, use counter to make sure that all signal unpause before actually unpausing
    _tryResumePipeline() {
        if(--this._pipelinePauseLock == 0) {
            this._indexSource.resume();
            this._pipeline.resume();
        }
    }

    //pause the pipeline if not already paused and increment pauselock counter
    _pausePipeline() {
        if(this._pipelinePauseLock++ == 0) {
            this._indexSource.pause();
            this._pipeline.pause();
        }
    }

    //queue the index and try to spawn it (or next in queue) if can
    _processIndex(index) {
        this._queueIndex(index);
        this._trySpawnProcessor();
    }

    _queueIndex(index) {
        //stringify for storage for easy size computation
        let indexString = JSON.stringify(index);
        let indexStringBytes = this._getStringSizeInBytes(indexString);
        this._queueSizeInBytes += indexStringBytes;
        this._queue.push(indexString);
        //if queue size newly at high water mark pause pipeline and indicate throttled (need to make sure only done once to maintain pause counter consistency)
        if(this._queueSizeInBytes >= this._options.highWaterMark && !this._throttled) {
            this._throttled = true;
            this._pausePipeline();
        }
    }

    _dequeueIndex() {
        let indexString = this._queue.shift();
        let indexStringBytes = this._getStringSizeInBytes(indexString);
        this._queueSizeInBytes -= indexStringBytes;
        //convert back to object
        let index = JSON.parse(indexString);
        //if newly below high water mark try to unpause (need to make sure only done once to maintain pause counter consistency)
        if(this._queueSizeInBytes < this._options.highWaterMark && this._throttled) {
            this._throttled = false;
            this._tryResumePipeline();
        }
        return index;
    }

    _getStringSizeInBytes(s) {
        return s.length * 2;
    }

    //test output and exit
    _test(data) {
        console.log(data);
        process.exit(0);
    }

    //spawn processor if max not exceeded and items in queue
    _trySpawnProcessor() {
        if(this._spawned < this._options.maxSpawn && this._queue.length > 0) {
            //check is paused
            if(this.paused) {
                //retry on resumed
                this._pauseEmitter.once("resume", () => {
                    this._trySpawnProcessor();
                });
            }
            //good to go
            else {
                let index = this._dequeueIndex();
                let fpath = join(this._options.dataRoot, index.fpath);
                let header = index.header;
                this._spawned++;
                this._sendToProcessor(fpath, header).then((data) => {
                    this._postprocess(index, data);
                }, (e) => {
                    this._source.emit("error", `Error processing geotiff file.\n${e.toString()}`);
                });
            }
        }
    }

    _sendToProcessor(fpath, header) {
        return new Promise((resolve, reject) => {
            let processor = fork(processorFile, [fpath, `'${JSON.stringify(header)}'`], {
                stdio: "pipe",
                execArgv: process.execArgv.concat(["--experimental-worker"])
            });
            //console.log(fork.toString());
            let id = this._processorID++;
            this._processors[id] = processor;
            processor.on("message", (data) => {
                //remove processor from processor store
                delete this._processors[id];
                //resolve with values
                resolve(data.values);
            });
            //spawn returned on exit
            processor.on("exit", () => {
                this._spawnReturned();
            });
            processor.stderr.on("data", (chunk) => {
                reject(chunk);
            });
        });
      
    }

    //send out metadoc if not paused, otherwise retry on resume
    _trySend(metaDoc) {
        //just return if the stream has been destroyed
        if(this.destroyed) {
            return;
        }
        if(this.paused) {
            this._pauseEmitter.once("resume", () => {
                this._trySend(metaDoc);
            });
        }
        else {
            this._source.emit("data", metaDoc);
            //increment the number of complete docs and send end event if done
            if(++this._completeDocs >= this._totalDocs && this._countComplete) {
                this._source.emit("finish");
                this._source.emit("close");
            }
        }
    }

    //when spawn returns, decrement spawned number and try to spawn a new one
    _spawnReturned() {
        this._spawned--;
        this._trySpawnProcessor();
    }

    _postprocess(index, values) {
        let metaDoc = index.descriptor;
        //add values to descriptor
        metaDoc.values = values;
        let wrapped = {
            name: this._options.valueName,
            value: metaDoc
        };
        this._trySend(wrapped);
    }

    _postprocessHeader(index, header) {
        //header should contain header object and dataset from index, anything else?
        let metaDoc = {
            dataset: index.descriptor.dataset,
            header: header
        }
        let wrapped = {
            name: this._options.headerName,
            value: metaDoc
        };
        this._trySend(wrapped);
    }


    //-------------------------override methods------------------------

    pause() {
        //stop emitting and throttle if not already paused and stream hasn't been destroyed
        if(!this.paused && !this.destroyed) {
            this._pausePipeline();
            this.paused = true;
            this._pauseEmitter.emit("pause");
        }
    }
    
    resume() {
        //resume pipeline if paused and stream hasn't been destroyed
        if(this.paused && !this.destroyed) {
            this._tryResumePipeline();
            this.paused = false;
            this._pauseEmitter.emit("resume");
        }
    }

    destroy() {
        //destroy queue
        this._queue = [];
        this._queueSizeInBytes = 0;
        //stop all processor processes
        for(let processorID in this._processors) {
            let processor = this._processors[processorID];
            processor.kill("SIGKILL");
        }
        //destroy index source and pipeline
        this._indexSource.destroy();
        this._pipeline.destroy();
        //indicate destroyed, should check for this before sending anything to ensure nothing sent out after destroyed
        this.destroyed = true;
        //send resume event and indicate not paused to flush out any functions waiting on resume
        this.paused = false;
        this._pauseEmitter.emit("resume");
        //send close event
        this._source.emit("close");
    }

    //super on event should work fine


    // maxSpawn: 5,
    // highWaterMark: 100M,
    // createHeader: true,
    // headerName: "raster_header",
    // valueName: "value_map",
    // dataRoot: undefined,
    // indexFile: undefined

    //parses a set of command line arguments into an options object for use in the constructor
    //returns an object containing a success flag, and the resulting options object or a help string if a help flag is specified or if parsing failed
    static parseArgs(args) {

        let helpMessage = "Available arguments:\n"
        + "-f, --index_file: Required. An index file containing the paths to the geotiff files and their respective metadata.\n"
        + "-r, --data_root: Required. The root folder for the geotiff files. Index file paths should be relative to this folder.\n"
        + "-s, --max_spawn: Optional. The maximum number of geotiff file processor processes to spawn at once. Negative values indicate no limit. Default value 5.\n"
        + "-hw, --high_water_mark: Optional. maximum index document queue size in before pausing document stream. Can be provided in bytes, KB (K suffix), MB (M suffix), or GB (G suffix). Negative value indicates no limit. Default value 100M\n"
        + "-nh, --no_header: Optional. Indicates that no header object should be generated. By default a document containing information on the geotiff file header will be produced.\n"
        + "-vn, --value_name: Optional. Name to assign to value documents. Default value 'value_map'.\n"
        + "-hn, --header_name: Optional. Name to assign to header document. Default value 'raster_header'.\n"
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
                case "-s":
                case "--max_spawn": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    let valuei = parseInt(args[i]);
                    if(isNaN(valuei)) {
                        setHelp(true);
                        break argLoop;
                    }
                    else {
                        result.result.maxSpawn = args[i];
                    }
                    break;
                }
                case "-hw":
                case "--high_water_mark": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    let valuei = parseInt(args[i]);
                    if(isNaN(valuei)) {
                        setHelp(true);
                        break argLoop;
                    }
                    else {
                        result.result.highWaterMark = args[i];
                    }
                    break;
                }
                case "-nh":
                case "--no_header": {
                    result.result.createHeader = false;
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
                case "-hn":
                case "--header_name": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    result.result.headerName = args[i];
                    break;
                }
                case "-r":
                case "--data_root": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    result.result.dataRoot = args[i];
                    break;
                }
                case "-f":
                case "--index_file": {
                    if(++i >= args.length) {
                        setHelp(true);
                        break argLoop;
                    }
                    result.result.indexFile = args[i];
                    break;
                }
                case "-h":
                case "--help": {
                    //set help message with no error and break out of argument loop (no other arguments matter)
                    setHelp(false);
                    break argLoop;
                }
                default: {
                    //invalid flag, set help with error and break out of arg loop
                    setHelp(true);
                    break argLoop;
                }
            }
        }

        return result;
    }


    //-------------------------end override methods---------------------
}
























// spawnLock.on("spawn", () => {
//     if(++spawned > maxSpawn) {
//         spawnLock.emit("lock");
//     }
// })

// spawnLock.on("spawnFinished", () => {

// })

// let header = null;
// //throttle lock
// throttle = new LockCoordinator([pauseLock, spawnLock]);
// function this._processIndex(index) {
    
//     return new Promise((resolve, reject) => {
//         //wait for throttle lock
//         throttle.getLock(() => {
//             let fpath = join(dataRoot, index.fpath);
//             spawnLock.emit("spawn");
//             let processor = fork("geotiffProcessor.js", [fpath, JSON.stringify(header)], {stdio: "pipe"});
            
//         });
        
//     });
    

    
//     //preprocessChain.unpipe(pipeline);
    

//     return getDataFromGeoTIFFFile(fpath, header).then((data) => {
//         //set header
//         header = data.header;
//         //strip out file name, keep only descriptor
//         let metaDoc = index.descriptor;
//         //add values to descriptor
//         metaDoc.values = data.values;
//         return metaDoc;
//     }, (e) => {
//         throw new Error(e);
//     });
// }



//index

//files expected to have single "0" band

//check if need custom no data still
//get file name from index, test for now
// let fpath = "";



// getDataFromGeoTIFFFile(fpath);











//you can't use async event emitters to get lock unlock signals... breaks whole point since multiple things could get through before signal received
//actually, you can... apparently emit triggers listeners immediately
//just used queueing, easier to keep track of
//this could work if revamped to use synchronous lock unlock calls

//coordinates multiple locks as event emitters
//multilevel lock runs the risk of outer lock locking while waiting for inner lock after multiple get through outer lock
//for spawn throttling since only one can get through a lock at a time until it finishes its synchronous context, just need to relock on coordinator in context
// class LockCoordinator {
//     constructor(locks, mode = "and") {
//         //are the locks locked
//         _lockSigs = [];
//         _locked = false;
//         _mode = mode;
//         _lock = new EventEmitter();
//         for(let i = 0; i < locks.length; i++) {
//             //lock starts unlocked
//             _lockSigs.push(false);
//             lock.on("lock", () => {
//                 _lockSigs[i] = true;
//             });
//             lock.on("unlock", () => {
//                 _lockSigs[i] = false;

//             });
//         }
//     }

//     _emitState(setStateLocked) {
//         sigLocked = _checkSig();
//         //only emit if changing state
//         if(sigLocked && !_locked) {
//             _locked = true;
//             _lock.emit("lock");
//         }
//         else if(!sigLocked && _locked) {
//             _locked = false;
//             _lock.emit("unlock");
//         }
//     }

//     _checkSig() {
//         //if and mode then locked if any lock is locked, if or mode only locked if all locks are locked
//         let locked = mode == "and" ? lockSigs.some((sig) => {
//             return sig;
//         }) : lockSigs.every((sig) => {
//             return sig;
//         });

//     }

//     isLocked() {
//         return locked;
//     }

//     //don't think promises then method is garenteed immediately on resolve, so use callback to guarentee immediate execution after secondary lock check
//     getLock(cb) {
//         //if not locked then just immediately run cb
//         if(!locked) {
//             cb();
//         }
//         else {
//             let unlock = () => {
//                 //make sure something else didn't lock it again after unlock signal
//                 if(isLocked()) {
//                     //if something did lock again, return and wait for next unlock signal
//                     return;
//                 }
//                 else {
//                     //remove listener once executed
//                     _lock.off("unlock", unlock);
//                     cb();
//                 }
//             }
//             //wait for unlock signal from lock
//             _lock.on("unlock", unlock);
            
//         }
//     }
// }