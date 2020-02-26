const fs = require("fs");

dataFile = "./data/daily_rf_data_2019_11_27.csv";

fs.readFile(dataFile, "utf8", (e, data) => {
    if(e) {
        throw e;
    }

    //this is problematic if there are newlines in quotes, just parse rows with regex, then parse fields from the row groups
    //split unix or windows style line endings, trim in case last line has line break
    //let rows = data.trim().split(/\r?\n/);

    let linebreak = /\r?\n/;
    let rowend = `(?:${linebreak}|$)`;
    let fieldend = `(?:,|$)`;
    //. doesnt match linebreak characters
    let inclusiveDot = `(?:.|${linebreak})`;

    
    let standardField = /([^",\n\r]*?)/;
    let standardFieldNoCap = /(?:[^",\n\r]*?)/;
    

    let escapedQuote = /""/;
    //special def that greedily pulls text blocks including escaped quotes so don't hit end quote
    let textWEscQuotes = `(?:${escapedQuote}${inclusiveDot}*?)*`;
    //non-greedily take any characters in field
    let textAny = `${inclusiveDot}*?`;
    //any text followed by text including escape quotes
    //capture inside quotes
    let quotedField = `"(${textAny}${textWEscQuotes})"`;
    let quotedFieldNoCap = `"(?:${textWEscQuotes})"`;

    //use captures from field defs since want to capture quoted fields inside quotes (create non-capture group here)
    let field = `(?:${standardField}|${quotedField})`;
    let fieldNoCap = `(?:${standardFieldNoCap}|${quotedFieldNoCap})`;

    //any number of fields followed by commas followed by a field without a comma (don't capture group)
    let rowDef = new RegExp(`((?:${fieldNoCap},)*${fieldNoCap})${rowend}`, "g");
    let columnDef = new RegExp(`${field}${fieldend}`, "g");

    //starred groups only capture last instance, ugh
    let testDef = new RegExp("(.*?)(?:,|$)", "g");

    console.log(rowDef.source);
    
    for(let i = 0; i < rows.length; i++) {
        let row = rows[i];
        // if(!rowDef.test(row)) {
        //     console.log("Invalid row");
        // }

        let match = row.match(testDef);
        console.log(match.length);
        //test length, first row should be fine
        console.log(row.split(",").length);

        break;        
    }
});