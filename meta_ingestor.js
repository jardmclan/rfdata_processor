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

function addMeta(metaFile) {
    return new Promise((resolve, reject) => {
        child = spawn("bash", ["./bin/add_meta.sh", metaFile]);
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

    let result = {
        success: true,
        pof: null,
        error: null
    }

    writeMeta(fname, data).then(() => {
        addMeta(fname).then(() => {
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
            process.send(result, callback = () => {
                //file was written, so try to cleanup, ignore any errors
                if(cleanup) {
                    cleanupFile(fname).finally(() => {
                        process.exit(1);
                    });
                }
            });
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