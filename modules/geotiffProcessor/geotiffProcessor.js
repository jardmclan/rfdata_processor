import geotiff from "geotiff";
const fs = require("fs");

//index

//files expected to have single "0" band

//check if need custom no data still
//get file name from index, test for now
let fpath = "";


this.header = null;


function getDataFromGeoTIFFFile(fpath) {

    return new Promise((resolve, reject) => {
        fs.readFile(fpath, (e, data) => {
            if(e) {
                return reject(e);
            }
            getRasterDataFromGeoTIFFArrayBuffer(data, bands = ["0"]).then((raster) => {
                raster.header;
                raster.bands["0"];
                if(this.header == null) {
                    this.header = raster.header;
                }
                else {
                    //compare with stored header and reject if doesnt match
                    for(let field in header) {
                        if(raster.header[field] != this.raster.header[field]) {
                            return reject("Header mismatch");
                        }
                    }
                }
                resolve(raster.bands["0"]);
            }, (e) => {
                reject(e);
            });
            
        });
    });

    //if raster header already set then verify header the same (reject if not), otherwise set header
    //return indexed values (should only have the one band)

}


//return header and bands
function getRasterDataFromGeoTIFFArrayBuffer(data, customNoData = undefined, bands = undefined) {
    return geotiff.fromArrayBuffer(data).then((tiff) => {
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