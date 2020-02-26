const fs = require("fs");

dataFile = "./data/daily_rf_data_2019_11_27.csv";

fs.readFile(dataFile, "utf8", (e, data) => {
    if(e) {
        throw e;
    }

    //this is problematic if there are newlines in quotes, just parse rows with regex, then parse fields from the row groups
    //split unix or windows style line endings, trim in case last line has line break
    //let rows = data.trim().split(/\r?\n/);

    //handles windows or unix style rowend (think the spec only specified windows style, but better to be robust)
    let linebreak = /\r?\n/;
    //rows delimited by linebreaks or end of string
    let rowend = `(?:${linebreak}|$)`;
    let fieldend = `(?:,|$)`;
    //. doesnt match linebreak characters
    let inclusiveDot = `(?:.|${linebreak})`;

    //standard field not wrapped in doublequotes cannot contain doublequotes, commas, or linebreak characters
    let standardField = /([^",\n\r]*?)/;
    //non-capture group version
    let standardFieldNoCap = /(?:[^",\n\r]*?)/;
    

    let escapedQuote = /""/;
    //special def that greedily pulls text blocks including escaped quotes so don't hit end quote
    let textWEscQuotes = `(?:${escapedQuote}${inclusiveDot}*?)*`;
    //non-greedily take any characters in field
    let textAny = `${inclusiveDot}*?`;
    //any text followed by text including escape quotes
    //capture inside quotes
    let quotedField = `"(${textAny}${textWEscQuotes})"`;
    //non-capture group version
    let quotedFieldNoCap = `"(?:${textWEscQuotes})"`;

    //field is one of the two field types, use captures from field defs since want to capture quoted fields inside quotes (create non-capture group here)
    let field = `(?:${standardField}|${quotedField})`;
    let fieldNoCap = `(?:${standardFieldNoCap}|${quotedFieldNoCap})`;

    //use global defs to avoid kleene star capture issue
    //row is any number of fields followed by commas followed by a field followed by a row delimiter (capture row without row delim)
    let rowDef = new RegExp(`((?:${fieldNoCap},)*${fieldNoCap})${rowend}`, "g");
    //column is a field followed by a field delimeter, capture handled by field defs
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