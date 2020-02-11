const fs = require("fs");

const dataFile = "data/daily_rf_data_2019_11_27.csv";
const noData = "NA";

const dataset = "rfstat_021120";

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

});

function dateParser(date) {
    date = date.slice(1);
    let split = date.split(".");
    dateInfo = {
        year: split[0],
        month: split[1],
        day: split[2]
    }
}