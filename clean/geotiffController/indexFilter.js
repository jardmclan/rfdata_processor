let index = require("./input/index.json").index;
const fs = require("fs");

let filteredIndexFile = "./input/filteredIndex.json";


let filtered = [];

//filter out only statewide maps and mm
//use keys to iterate to ensure integrity not broken when removing objects
for(let meta of index) {
    let descriptor = meta.descriptor;
    if(descriptor.spatialExtent == "St" && descriptor.unit == "mm") {
        filtered.push(meta);
    }
}

filtered = {
    index: filtered
};

fs.writeFile(filteredIndexFile, JSON.stringify(filtered, null,  4), (e) => {
    if(e) {
        console.error(e);
    }
});