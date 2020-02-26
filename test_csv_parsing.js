const fs = require("fs");

dataFile = "./data/daily_rf_data_2019_11_27.csv";

fs.readFile(dataFile, "utf8", (e, data) => {
    if(e) {
        throw e;
    }

    //split unix or windows style line endings, trim in case last line has line break
    let rows = data.trim().split(/\r?\n/);

    
    let standardField = new RegExp(/([^",\n\r]*?)/);
    
    //. doesnt match newline

    let escapedQuote = new RegExp(/""/);
    //don't capture this group
    let textWEscQuotes = new RegExp(`(?:${escapedQuote.source}?.*?)*`);
    //capture inside quotes
    let quotedField = new RegExp(`"(${textWEscQuotes.source})"`);

    //use captures from field defs since want to capture quoted fields inside quotes (create non-capture group here)
    let field = new RegExp(`(?:${standardField.source}|${quotedField.source})`);

    //any number of fields followed by commas followed by a field without a comma (don't capture group)
    let rowDef = new RegExp(`^(?:${field.source},)*${field.source}$`);

    //starred groups only capture last instance, ugh
    let testDef = new RegExp("^(?:(.*?),)*(.*?)$");

    console.log(rowDef.source);
    
    for(let i = 0; i < rows.length; i++) {
        let row = rows[i];
        // if(!rowDef.test(row)) {
        //     console.log("Invalid row");
        // }

        let match = row.match(testDef);
        console.log(match[1]);

        break;        
    }
});