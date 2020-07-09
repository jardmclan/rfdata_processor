const {spawn} = require("child_process");
const fs = require("fs");

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

function writeMeta(fname, metadataString) {
    return new Promise((resolve, reject) => {
        fs.writeFile(fname, metadataString, {}, (e) => {
            if(e) {
                reject(e)
            }
            else {
                resolve();
            }
        });
    });
}


function ingestData(fname, metadata, cleanup, container) {
    metadataString = JSON.stringify(metadata);
    return new Promise((resolve, reject) => {
        writeMeta(fname, metadataString).then(() => {
            addMeta(fname, container).then(() => {
                if(cleanup) {
                    cleanupFile(fname).then(() => {
                        resolve(null);
                    }, (e) => {
                        resolve(e);
                    });
                }
            }, (e) => {
                reject(e)
                //still try to cleanup, but ignore output
                if(cleanup) {
                    cleanupFile(fname);
                }
            });
        }, (e) => {
            reject(e)
        });
        
    });
    
}

//no communication, set data handler

function dataHandlerRecursive(fname, metadata, retryLimit, cleanup, container, attempt = 0) {
    return ingestData(fname, metadata, cleanup, container).then((error) => {
        return error;
    }, (error) => {
        if(attempt >= retryLimit) {
            return Promise.reject(error);
        }
        else {
            console.log(attempt, retryLimit);
            return dataHandlerRecursive(fname, metadata, retryLimit, cleanup, container, attempt + 1);
        }
    });
}

function dataHandler(fname, metadata, retryLimit, cleanup, container = null) {
    return dataHandlerRecursive(fname, metadata, retryLimit, cleanup, container);
}

module.exports.dataHandler = dataHandler;