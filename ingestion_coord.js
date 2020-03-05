const {fork} = require("child_process");
const {EventEmitter} = require("events");

let container = null;

//get max processes to spawn from args or exit if not defined
if(process.argv.length < 3) {
    process.stderr.write("Invalid args. Requires max spawn value.");
    process.exit(2);
}

let maxSpawn = parseInt(process.argv[2]);
if(isNaN(maxSpawn)) {
    process.stderr.write("Invalid args. Requires numeric max spawn value.");
    process.exit(2)
}

//check if a container is defined
if(process.argv.length > 3) {
    container = process.argv[3];
}

const spawnMonitor = new EventEmitter();

let complete = false;
let messageQueue = [];
let waiting = 0;

function spawnIngestor(message) {
    return new Promise((resolve, reject) => {

        let result = {
            id: message.id,
            result: null
        };

        //spin off a new process to do the message processing
        let ingestor = container == null ? fork("meta_ingestor.js") : fork("meta_ingestor.js", [container]);

        //pass message through to new process
        ingestor.send(message);

        //send id and result back to main when complete
        ingestor.on("message", (message) => {
            result.result = message;
            process.send(result);
            resolve();
        });
        
    }).then(() => {
        spawnMonitor.emit("processCompleted");
    });
}

process.on("message", (message) => {
    //finish message stream with a null
    if(message == null) {
        complete = true;
        return;
    }
    //push error message to sender if message sent after terminator
    else if(complete) {
        process.stderr.write("Message sent after null terminator. Ignoring message...");
    }
    else {
        messageQueue.push(message);
        spawnMonitor.emit("messageQueued");
    }
    
});

spawnMonitor.on("messageQueued", () => {
    //possible that a process will complete and the message will get pulled by processComplete event before this event is triggered
    if(messageQueue.length > 0) {
        if(waiting < maxSpawn) {
            spawnIngestor(messageQueue.shift());
            waiting++;
        }
    }
});

spawnMonitor.on("processCompleted", () => {
    //somethings gone really wrong if waiting is less than 0 (more processes returned than were sent out)
    //send off message and terminate
    if(--waiting < 0) {
        process.stderr.write("Critical internal state error.");
        process.exit(1);
    }
    //replace with next in queue if exists
    //node has no race conditions, locking not necessary
    if(messageQueue.length > 0) {
        spawnIngestor(messageQueue.shift());
        waiting++;
    }
    //if last message returned and message queueing complete, end process
    else if(waiting == 0 && complete) {
        console.error("called");
        process.exit(0);
    }
});
