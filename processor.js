const fs = require("fs");
const schemaTrans = require("schema_translation");
import {getMetaTemplate, getValueTemplate, DocTemplate} from "doc_schema";

const dataFile = "data/daily_rf_data_2019_11_27.csv";
const output = "output/docs.json";
const noData = "NA";

let docs = {
    metadata: [],
    values: []
}

fs.readFile(dataFile, "utf8", (e, data) => {
    if(e) {
        throw e;
    }

    let rows = data.split("\n").trim();

    let headers = rows[0].split(",").trim();

    let data = [];
    rows.forEach((row) => {
        let rowData = row.split(",").trim();
        data.push(rowData);
    });

    dateRegex = new Regex(schemaTrans.date);
    headers.forEach((label, i) => {
        let docLabel = schemaTrans.meta[label];
        if(docLabel != undefined) {
            //need multiple copies, make an object generation function instead of a template object
            meta[docLabel] = 
        }
        else if(dateRegex.test(docLabel)) {
            let date = dateParser(docLabel);
        }
        else {
            console.log(`Warning: No translation for label ${label}, check schema. Skipping column...`);
        }
    });

});

function dateParser(date) {
    let sd = date.slice(1);
    let formattedDate = new Date(sd);
    let isoDate = formattedDate.toISOString();
    return isoDate;
}