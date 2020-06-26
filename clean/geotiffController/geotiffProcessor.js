const geotiff = require("geotiff");

// if(process.argv.length < 3) {
//     process.stderr.write("Invalid args. Requires filepath.", cb = () => {
//         process.exit(2);
//     });
// }

// fpath = process.argv[2];

// header = null;
// if(process.argv.length > 3) {
//     try {
//         header = JSON.parse(header);
//     }
//     catch(e) {
//         process.stderr.write(`Error parsing header\n${e}`, cb = () => {
//             process.exit(2);
//         });
//     }
    
// }
// getDataFromGeoTIFFFile(fpath, header).then((data) => {
//     //send data back to parent and exit
//     process.send(data, callback = () => {
//         process.exit(0);
//     });
// }, (e) => {
//     //write error and exit
//     process.stderr.write(`An error has occured while getting geotiff data\n${e}`, cb = () => {
//         process.exit(1);
//     });
// });

module.exports.getDataFromGeoTIFFFile = getDataFromGeoTIFFFile;


function getDataFromGeoTIFFFile(fpath) {
    return new Promise((resolve, reject) => {
        getRasterDataFromGeoTIFFArrayBuffer(fpath, -3.3999999521443642e+38, ["0"]).then((raster) => {
            //resolve with indexed values
            resolve({
                header: raster.header,
                values: raster.bands["0"]
            });
        }, (e) => {
            reject(e);
        });
    });
}


//return header and bands
function getRasterDataFromGeoTIFFArrayBuffer(fpath, customNoData = undefined, bands = undefined) {
    return geotiff.fromFile(fpath).then((tiff) => {
      return tiff.getImage().then((image) => {
        //are tiepoints indexed by cooresponding band? Assume at 0
        let tiepoint = image.getTiePoints()[0];
        let fileDirectory = image.getFileDirectory();
        return image.readRasters().then((rasters) => {
            return new Promise((resolve, reject) => {
                let geotiffData =  {
                    header: null,
                    bands: {}
                };
    
                //get scales from file directory
                let [xScale, yScale] = fileDirectory.ModelPixelScale;
    
                //if unspecified or null assume all bands
                if(bands == undefined || bands == null) {
                    bands = Array.from(rasters.keys());
                }
    
                let noData = Number.parseFloat(fileDirectory.GDAL_NODATA);
                geotiffData.header = {
                    nCols: image.getWidth(),
                    nRows: image.getHeight(),
                    xllCorner: tiepoint.x,
                    yllCorner: tiepoint.y - image.getHeight() * yScale,
                    cellXSize: xScale,
                    cellYSize: yScale,
                }
                
    
                for(band of bands) {
                    let raster = rasters[band];
                    let valueMap = {};
                    if(raster == undefined) {
                        return reject("Could not find band: " + band);
                    }
                    for(let i = 0; i < raster.length; i++) {
                        let value = raster[i];
                        //map value to index if value exists
                        if(value != noData && value != customNoData) {
                            valueMap[i] = value;
                        }
                    }
                    geotiffData.bands[band] = valueMap;
                }
                resolve(geotiffData);
            });
        });
      });
      
    });
  }