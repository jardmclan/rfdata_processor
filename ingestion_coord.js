const {fork} = require("child_process");

function errorExit(error, code) {
    process.stderr.write(`${error.toString()}\n`, cb = () => {
        process.exit(code);
    });
}

//get max processes to spawn from args or exit if not defined
if(process.argv.length < 3) {
    errorExit("Invalid args. options argument is required.\n", 2);
}

//options packed as a stringified json document
let options = null;
try {
    options = JSON.parse(process.argv[2]);
}
catch(e) {
    errorExit(`Must provide a valid JSON string as the option parameter`, 2);
}
//no default options for this, defaults can be handled by executor
let optionsTemplate = ["highWaterMark", "maxSpawn", "retry"];
for(let option of optionsTemplate) {
    if(options[option] === undefined) {
        errorExit(`Invalid options, ${option} must be defined`, 2);
    }
}
//convert less than 0 parameters to infinity
if(options.maxSpawn < 0) {
    options.maxSpawn = Number.POSITIVE_INFINITY;
}
if(options.highWaterMark < 0) {
    options.highWaterMark = Number.POSITIVE_INFINITY;
}
if(options.retry < 0) {
    options.retry = Number.POSITIVE_INFINITY;
}


let complete = false;
let throttled = false;
let messageQueue = [];
let waiting = 0;
let queuedDataSize = 0;

function encodeMessage(message) {
    let info = null;
    let type = typeof message;
    //message can only be string, number, boolean, or object
    switch(type) {
        case "boolean": {
            //boolean is apparently 4 bytes
            info = {
                sizeInBytes: 4,
                message: message
            };
            break;
        }
        case "number": {
            info = {
                sizeInBytes: 8,
                message: message
            };
            break;
        }
        case "string": {
            //append s indicator to message
            info = {
                sizeInBytes: (message.length + 1) * 2,
                message: message + "s"
            };
            break;
        }
        case "object": {
            let converted = JSON.stringify(message);
            //append o indicator to message
            info = {
                sizeInBytes: (converted.length + 1) * 2,
                message: converted + "o"
            };
            break;
        }
    }
    //character is 2 bytes, multiply characters by 2
    return info;
}

//returns size of message in storage and decoded message, or null if encoded message is invalid
function decodeMessage(encodedMessage) {
    let info = null;
    let type = typeof encodedMessage;
    //encoded message should only be boolean, number, or string
    switch(type) {
        case "boolean": {
            //boolean is apparently 4 bytes
            info = {
                sizeInBytes: 4,
                message: encodedMessage
            };
            break;
        }
        case "number": {
            info = {
                sizeInBytes: 8,
                message: encodedMessage
            };
            break;
        }
        case "string": {
            //code is last character of string
            let code = encodedMessage.slice(-1);
            //remove code character
            let decoded = encodedMessage.slice(0, -1);
            //was an object
            if(code == "o"){
                //convert back to object
                decoded = JSON.parse(decoded);
            }
            //make sure code was s otherwise encoded message is invalid, break so returns null
            else if(code != "s") {
                break;
            }
            //check whether message is a string or a stringified 
            info = {
                sizeInBytes: encodedMessage.length * 2,
                message: decoded
            };
            break;
        }
    }

    return info;
}

function spawnIngestor(message, attempt = 0) {
    let result = {
        id: message.id,
        result: null
    };

    return new Promise((resolve, reject) => {
        //spin off a new process to do the message processing
        let ingestor = fork("meta_ingestor.js");

        //pass message through to new process
        ingestor.send(message);

        //send id and result back to main when complete, should only receive one message
        ingestor.once("message", (received) => {
            resolve(received)
        });

        ingestor.on("error", (e) => {
            reject(`Error in ingestor:\n${e.toString()}`);
        });
        
    }).then((received) => {
        //evaluate if ingestor was successful and retry if not (up to retry limit)
        if(received.success || attempt >= options.retry) {
            result.result = received;
            return result;
        }
        else {
            return spawnIngestor(message, attempt + 1);
        }
    }, (e) => {
        return Promise.reject(e);
    });
}


//pass messages as strings then parse at destination to avoid parsing overhead for storage computation
process.on("message", (message) => {
    //finish message stream with a null
    if(message == null) {
        complete = true;
        return;
    }
    //push error message to sender if message sent after terminator
    else if(complete) {
        process.stderr.write("Message sent after null terminator. Ignoring message...\n");
    }
    else {
        //convert messages into strings for storage for size computation
        let messageInfo = encodeMessage(message);
        //add converted message size to size
        queuedDataSize += messageInfo.sizeInBytes;
        messageQueue.push(messageInfo.message);
        //at or over high water mark, send control signal to throttle
        if(queuedDataSize >= options.highWaterMark) {
            throttled = true;
            process.send({
                type: "control",
                value: "pause"
            });
        }
        //message added to queue, try to spawn an ingestor
        trySpawnIngestor();
    }
});

function trySpawnIngestor() {
    //make sure there are items in the queue and not exceeding max spawn limit
    if(messageQueue.length > 0 && waiting < options.maxSpawn) {
        //get next message in the queue
        let message = messageQueue.shift();
        //decode
        let messageInfo = decodeMessage(message);
        //subtract message size
        queuedDataSize -= messageInfo.sizeInBytes;
        //no need to send constant control messages, only send if throttle message sent
        if(queuedDataSize < options.highWaterMark && throttled) {
            throttled = false;
            process.send({
                type: "control",
                value: "resume"
            });
        }
        //send off decoded message
        spawnIngestor(messageInfo.message).then((result) => {
            //wrap in message type
            let wrapped = {
                type: "result",
                value: result
            }
            process.send(wrapped);
            processCompleted();
        }, (e) => {
            process.stderr.write(`Error while spawning ingestor:\n${e.toString()}\n`);
        });
        waiting++;
    }
}


function processCompleted() {
    //somethings gone really wrong if waiting is less than 0 (more processes returned than were sent out)
    //send off message and terminate
    if(--waiting < 0) {
        process.stderr.write("Critical internal state error.\n");
        process.exit(1);
    }
    //try to spawn a new ingestor
    trySpawnIngestor();
    //no messages waiting, none queued, and complete signal received, exit process
    if(waiting == 0 && messageQueue.length == 0 && complete) {
        process.exit(0);
    }
}
