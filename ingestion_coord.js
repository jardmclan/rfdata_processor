const {fork} = require("child_process");


process.on("message", (message) => {
    let id = message.id;

    let result = {
        id: id,
        result: null
    };

    //spin off a new process to do the actual processing
    let ingestor = fork("meta_ingestor.js");
    //pass message through to new process
    ingestor.send(message);

    //send id and result back to main when complete
    ingestor.on("message", (message) => {
        result.result = message;
        process.send(result);
    });
});