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


function ingestData(fname, metadata) {
    metadataString = JSON.stringify(metadata);
    return new Promise((resolve, reject) => {
        addMeta(fname, containerLoc).then(() => {
            if(cleanup) {
                cleanupFile(fname).then(() => {
                    resolve(null);
                }, (e) => {
                    resolve(e);
                })
            }
        }, (e) => {
            reject(e)
            //still try to cleanup, but ignore output
            cleanupFile(fname);
        })
    });
    
}

//no communication, set data handler

function dataHandlerRecursive(fname, metadata, attempt = 0) {
    return ingestData(fname, metadata).then((error) => {
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

function dataHandler(fname, metadata) {
    return dataHandlerRecursive(fname, metadata)
}