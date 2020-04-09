const fs = require("fs");
const {spawn} = require("child_process");

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

process.on("message", (message) => {
    let fname = message.fname;
    let cleanup = message.cleanup;
    let data = message.data;
    let container = message.container;

    let result = {
        success: true,
        pof: null,
        error: null
    }

    writeMeta(fname, data).then(() => {
        addMeta(fname, container).then(() => {
            if(cleanup) {
                cleanupFile(fname).then(() => {
                    process.send(result, callback = () => {
                        process.exit(0);
                    });
                }), (e) => {
                    //no need to set success to false, data ingestion worked, just couldn't clean file
                    result.pof = "clean";
                    result.error = e.toString();
                    process.send(result, callback = () => {
                        process.exit(1);
                    });
                };
            }
            else {
                process.send(result, callback = () => {
                    process.exit(0);
                });
            }
        }, (e) => {
            result.success = false
            result.pof = "add_meta";
            result.error = e.toString();
            //file was written, so try to cleanup, ignore any errors
            if(cleanup) {
                cleanupFile(fname).finally(() => {
                    //wait to send result until after cleanup finished so number of processes doesn't spike due to extra processing after result returned
                    process.send(result, callback = () => {
                        process.exit(1);
                    });
                });
            }
            
        });
    }, (e) => {
        result.success = false
        result.pof = "write";
        result.error = e.toString();
        process.send(result, callback = () => {
            process.exit(1);
        });
    });
});