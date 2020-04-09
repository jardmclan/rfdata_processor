const moduleLoader = require("./module_loader.js");




// controller.on("data", (data) => {
//     delete data.value.values;
//     console.log(data);
// });

// controller.on("end", () => {
//     console.log("complete");
// });

// // controller.pause();
// // setTimeout(() => {
// //     controller.resume();
// // }, 100000);

// // setTimeout(() => {
// //     controller.pause();
// //     setTimeout(() => {
// //         console.log("resume");
// //         controller.resume();
// //     }, 100000);
// //     console.log("pause");
// // }, 5000);

// controller.on("error", (e) => {
//     console.error(e.toString());
// });

let GeotiffController = moduleLoader.load("geotiffController");
let SiteController = moduleLoader.load("siteController");
let args = process.argv.slice(2);
//let parsed = SiteController.parseArgs(args);
let parsed = GeotiffController.parseArgs(args);
// console.log(parsed.success);
// console.log(parsed.result);
//let controller = new SiteController(parsed.result);
console.log();
let controller = new GeotiffController(parsed.result);
//node test_driver -f "./data/daily_rf_data_2019_11_27.csv" -d "test" -v "rainfall" -u "mm"

// let controller = new SiteController({
//     dataFile: "./data/daily_rf_data_2019_11_27.csv",
//     dataset: "test",
//     valueType: "daily",
//     //valueLimit: 10,
//     //valueLimitIndividual: 1,
//     //metaLimit: 0,
//     rowLimit: -1,
//     nodata: "NA"
// });

// let controller = new GeotiffController({
//     maxSpawn: 5,
//     dataRoot: "./modules/geotiffController/input/RF_Monthly_3_Yr_Sample/",
//     indexFile: "./modules/geotiffController/input/index.json"
// });


//node --max-old-space-size=4096 processor.js -o "./output" -hw 1 -fl -1 -rl 3 -m "siteController" -f "./data/daily_rf_data_2019_11_27.csv" -d "test" -v "rainfall" -u "mm"
//node --max-old-space-size=4096 processor.js -o "./output" -hw 1 -fl -1 -rl 3 -m "geotiffController" -f "./modules/geotiffController/input/index.json" -r "./modules/geotiffController/input/RF_Monthly_3_Yr_Sample/" -ms 5

let count = 0;
setInterval(() => {
    console.log(count);
    
}, 5000);

// setTimeout(() => {
//     controller.destroy();
// }, 5000);


// setTimeout(() => {
//     controller.pause();
//     setTimeout(() => {
//         controller.resume();
//     }, 5000);
// }, 5000);

// setTimeout(() => {
//     controller.destroy();
// }, 20000);


controller.on("data", (data) => {
    count++;
    delete data.value.values;
    // console.log(count++);
    //console.log(data);
});

controller.on("close", (data) => {
    console.log("closed");
});

controller.on("finish", () => {
    console.log(count);
    console.log("complete");
});

controller.on("warning", (message) => {
    console.log(message);
});

controller.on("error", (e) => {
    console.log(e);
});

// const {fork} = require("child_process");

// //console.log(process.kill.toString());
// // let spawn = fork("testSpawn", [], {stdio: "inherit"});

// // setTimeout(() => {
// //     spawn.kill();
// //     setTimeout(() => {
// //         console.log("OK");
// //     }, 2000);
// // }, 5000);

// // spawn.on("exit", (code, signal) => {
// //     console.log(code, signal);
// // });

// console.log(process.argv[2].replace(/'/g, '"'));
// console.log(JSON.parse(process.argv[2].replace(/'/g, '"')));


//console.log(process.env.NODE_V8_COVERAGE);