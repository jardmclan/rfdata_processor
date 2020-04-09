const GenericModule = require("./genericModule");

//attempts to load modules by name or location
function load(moduleName, indexFile = "./modules/index.json") {
    let index;
    try {
        index = require(indexFile);
    }
    catch(e) {
        throw new Error(`Could not load index file: ${indexFile}.`);
    }
    
    let modulePath = index[moduleName];
    if(modulePath === undefined) {
        modulePath = moduleName;
    }
    let LoadedModule;
    try {
        LoadedModule = require(modulePath);
        //let test = require("./modules/geotiffProcessor/geotiffProcessor");
    }
    catch(e) {
        throw new Error(`Could not load module: ${modulePath}.\n${e}`);
    }

    //modules must extend the module class, if it doesn't then return null
    if(!(LoadedModule.prototype instanceof GenericModule)) {
        throw new Error(`Loaded module does not properly implement generic module. Compatible modules must implement generic module.`);
    }
    return LoadedModule;
    
}

exports.load = load;