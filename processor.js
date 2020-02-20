const fs = require("fs");
const schemaTrans = require("./schema_translation");
const schema = require("./doc_schema");
const {fork} = require("child_process");

const dataFile = "./data/daily_rf_data_2019_11_27.csv";
//const output = "./output/docs.json";
const noData = "NA";

let cleanup = true;
//just use sequential ids, also serves as a counter for the number of docs for exiting
docID = 0;
complete = false;
returned = 0;

function sendData(metadata) {
    wrappedMeta = {
        name: "test",
        value: metadata
    };
    let fname = `output/metadoc_${docID++}.json`;
    let message = {
        id: docID,
        data: JSON.stringify(wrappedMeta),
        fname: fname,
        cleanup: cleanup
    };
    ingestionCoordinator.send(message);
}

function dateParser(date) {
    let sd = date.slice(1);
    let formattedDate = new Date(sd);
    let isoDate = formattedDate.toISOString();
    return isoDate;
}


let ingestionCoordinator = fork("ingestion_coord.js");

ingestionCoordinator.on("message", (message) => {
    if(!message.result.success) {
        console.log(`Error: Metadata ingestion failed.\nID: ${message.id}\nPOF: ${message.result.pof}\nReason: ${message.result.error}\n`);
    }
    else if(message.result.pof != null) {
        console.log(`Warning: An error occured after metadata insertion.\nID: ${message.id}\nPOF: ${message.result.pof}\nReason: ${message.result.error}\n`);
    }
    //if all docs sent for processing and number returned matches number sent kill ingestor process and exit
    if(++returned >= docID && complete) {
        ingestionCoordinator.kill();
        console.log("Complete!");
        process.exit(0);
    }
});

fs.readFile(dataFile, "utf8", (e, data) => {
    if(e) {
        throw e;
    }

    let rows = data.trim().split("\n");

    let headers = rows.shift().split(",").map((header) => {
        return header.replace(/^\s*('|")?|(\s|'|")?\s*$/g, "");
    });

    let dataRows = [];
    rows.forEach((row) => {
        let rowData = row.split(",").map((datum) => {
            return datum.replace(/^\s*('|")?|('|")?\s*$/g, "");
        });
        dataRows.push(rowData);
    });

    dateRegex = new RegExp(schemaTrans.date);

    // let documents = {
    //     meta: [],
    //     value: []
    // }; 

    dataRows.forEach((row) => {

        let metadata = {};
        let values = {};

        headers.forEach((label, i) => {
            let value = row[i];
            let docLabel = schemaTrans.meta[label];
            if(docLabel != undefined) {
                metadata[docLabel] = value;
            }
            else if(dateRegex.test(label)) {
                //if no data don't generate a document, just skip
                if(value != noData) {
                    let date = dateParser(label);
                    //probably want the value to be stored numerically
                    let valuef = parseFloat(value);
                    if(Number.isNaN(valuef)) {
                        console.log(`Warning: Value not 'no data' or parseable as float. Skipping...`);
                    }
                    else {
                        values[date] = valuef;
                    }
                }
            }
            else {
                console.log(`Warning: No translation for label ${label}, check schema. Skipping column...`);
            }
        });

        //generate and add metadata doc and value docs
        let metaDoc = schema.getMetaTemplate();
        Object.keys(metadata).forEach((label) => {
            if(!metaDoc.setProperty(label, metadata[label])) {
                console.log(`Warning: Could not set property ${label}, not found in template.`);
            }
        });
        
        //at least verify skn exists
        let skn = metaDoc.getProperty("skn");
        if(skn == undefined || skn == null) {
            console.log(`Warning: SKN not set. Skipping row...`);
        }
        else {
            //!here
            //sendData(metaDoc.toJSON());
            // documents.meta.push(metaDoc.toJSON());

            //value docs
            valueFields = {
                skn: skn,
                date: null,
                value: null
            }
            Object.keys(values).forEach((date) => {
                valueFields.date = date;
                valueFields.value = values[date];
                let valueDoc = schema.getValueTemplate();
                Object.keys(valueFields).forEach((label) => {
                    if(!valueDoc.setProperty(label, valueFields[label])) {
                        console.log(`Warning: Could not set property ${label}, not found in template.`);
                    }
                });
                //!here
                //send to ingestor
                sendData(valueDoc.toJSON());
                //documents.value.push(valueDoc.toJSON());
            });
        }
        
    });

    complete = true;


    

    // let toAdd = [documents.meta[1]];
    // toAdd.forEach((doc) => {
    //     let wrapped = {
    //         name: "test",
    //         value: doc
    //     }
    //     fs.writeFileSync(output, JSON.stringify(wrapped), "utf8");
        
    // });


    
    



    // let docJSON = JSON.stringify(documents.meta);
    // fs.writeFile(output, docJSON, "utf8", (e) => {
    //     if(e) {
    //         throw e;
    //     }
    //     console.log("Complete!");
    // })

});