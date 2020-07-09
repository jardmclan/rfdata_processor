
//synchronize api call process creation
//just use this with the main process

//0 request, 1 release
//0 accept, 1 error

const codes = require("syncCodes.json");
const Semaphore = require("../Semaphore");

module.exports.setSpawnLevel = setSpawnLevel;
module.exports.setChildHandler = setChildHandler;

spawnLock = null;

function setSpawnLevel(level) {
    if(level < 1) {
        throw new Error("Invalid spawn level, must be grater than 0.");
    }
    spawnLock = new Semaphore(level);
}


function setChildHandler(child) {
    child.on("message", (data) => {
        let code = data.code;
        let id = data.id;
        //request sent before initialized, reject
        if(spawnLock == null) {
            child.send({
                id: id,
                code: codes.response.reject
            });
            return;
        }
        switch(code) {
            case code.request.request: {
                spawnLock.acquire().then(() => {
                    child.send({
                        id: id,
                        code: child.response.accept
                    });
                });
                break;
            }
            case code.request.release: {
                //catch overflow and tell child that release was rejected (state error, someone released without acquiring)
                try {
                    this.spawnLock.release();
                }
                catch(e) {
                    child.send({
                        id: id,
                        code: child.response.reject
                    });
                }
                break;
            }
        }
    });
}

