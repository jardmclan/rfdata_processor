const fs = require("fs");
const assert = require("assert");

const TEST = false;

//a (hopefully) RFC 4180 compliant csv parser
function parseCSV(dataFile, firstRowHeader) {

    return new Promise((resolve, reject) => {
        fs.readFile(dataFile, "utf8", (e, data) => {
            if(e) {
                reject(e);
            }
    
            //handles windows or unix style rowend (think the spec only specified windows style, but better to be robust)
            let linebreak = `\r?\n`;
            //rows delimited by linebreaks or end of string
            let rowend = `(?:${linebreak}|$)`;
            let fieldend = `(?:,|$)`;
            //anything that isnt a double quote (double quotes handled in special escaped quote blocks)
            let notQuote = `[^"]`;
    
            //standard field not wrapped in doublequotes cannot contain doublequotes, commas, or linebreak characters
            let standardField = `([^",\n\r]*?)`;
            //non-capture group version
            let standardFieldNoCap = `[^",\n\r]*?`;
    
            let escapedQuote = `""`;
            //special def that greedily pulls text blocks including escaped quotes so don't hit end quote
            let textWEscQuotes = `(?:${escapedQuote}${notQuote}*?)*`;
            //non-greedily take any characters in field that aren't quotes
            let textAny = `${notQuote}*?`;
            //any text followed by text including escape quotes
            //capture inside quotes
            let quotedField = `"(${textAny}${textWEscQuotes})"`;
            //non-capture group version
            let quotedFieldNoCap = `"${textAny}${textWEscQuotes}"`;
    
            //field is one of the two field types, use captures from field defs since want to capture quoted fields inside quotes (create non-capture group here)
            let field = `(?:${standardField}|${quotedField})`;
            let fieldNoCap = `(?:${standardFieldNoCap}|${quotedFieldNoCap})`;
    
            //use global defs to avoid kleene star capture issue
            //row is any number of fields followed by commas followed by a field followed by a row delimiter (capture row without row delim)
            let rowDef = new RegExp(`((?:${fieldNoCap},)*${fieldNoCap})${rowend}`, "g");
            //column is a field followed by a field delimeter, capture handled by field defs
            let columnDef = new RegExp(`${field}${fieldend}`, "g");
    
            let matches;
            let rows = [];
            //check if last index is equal to the string length to prevent infinite loop with empty strings (parser assumes an empty string is a valid row, spec isn't super clear about this)
            while(rowDef.lastIndex < data.length && ((matches = rowDef.exec(data)) != null)) {
                rows.push(matches[1]);
            }
            //manually reset last index after completion due to empty string issue not triggering exec reset
            rowDef.lastIndex = 0;
    
            if(TEST) validateRows(data, rows);
    
            let cols = [];
            rows.forEach((row, i) => {    
                cols.push([]);
                while(columnDef.lastIndex < row.length && ((matches = columnDef.exec(row)) != null)) {
                    //2 capture groups, one will be undefined depending on which field def is used
                    let field = matches[1] == undefined ? matches[2] : matches[1];
                    cols[i].push(field);
                }
                //manually reset last index after completion due to empty string issue not triggering exec reset
                columnDef.lastIndex = 0;
            });
            
            if(TEST) validateCol(cols, rows[0]);
    
            //if first row is not a header, then just give numbered array as the header
            let results = {
                headers: firstRowHeader ? cols.shift() : Array.from(cols[0].keys()),
                values: cols
            };
            resolve(results);
        });
    });
}


//---------------------test functions------------------------------------------------------------

//if no fields contain newlines, can validate the number of rows
function validateRows(data, rows) {
    assert(data.trim().split(/\r?\n/).length == rows.length, "Number of rows does not match.");
}

//verify all rows have same number of fields, and give a row with no commas to validate number of fields
function validateCol(cols, noCommaRow) {
    let numFields;
    if(noCommaRow != undefined) {
        numFields = noCommaRow.split(",").length;
    }
    else {
        numFields = cols[0].length;
    }
    cols.forEach((col, i) => {
        if(col.length != numFields) {
            console.log(col);
        }
        assert(col.length == numFields, "Number of fields is does not match.");
    });
}

//---------------------end test functions------------------------------------------------------------


exports.parseCSV = parseCSV;