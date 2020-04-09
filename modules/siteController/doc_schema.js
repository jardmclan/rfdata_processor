//const dataset = "rfs_11_27_19";

function getMetaTemplate() {
    return new DocTemplate({
        skn: null,
        name: null,
        observer: null,
        network: null,
        island: null,
        elevation_m: null,
        lat: null,
        lon: null,
        ncei_id: null,
        nws_id: null,
        nesdis_id: null,
        scan_id: null,
        smart_node_rf_id: null,
        dataset: null
    });
}

function getValueTemplate() {
    return new DocTemplate({
        units: null,
        skn: null,
        type: null,
        //parse values as numbers, check if values are properly processed as numeric types during ingestion
        value: null,
        //did this even work?
        date: {
            $date: null
        },
        hourly: [],
        dataset: null
    }, {
        date: "$date"
    });
}

class DocTemplate {

    constructor(schema, typedValues = {}) {
        this.schema = schema;
        this.typedValues = typedValues;
    }

    setProperty(label, value) {
        if(this.schema[label] === undefined) {
            return false;
        }
        let typeLabel = this.typedValues[label];
        if(typeLabel != undefined) {
            this.schema[label][typeLabel] = value;
        }
        else {
            this.schema[label] = value;
        }

        return true;
    }

    getProperty(label) {
        let typeLabel = this.typedValues[label];
        if(typeLabel != undefined) {
            return this.schema[label][typeLabel];
        }
        else {
            return this.schema[label];
        }
    }

    toJSON() {
        return this.schema;
    }

    toString() {
        return JSON.stringify(this.schema);
    }
}

exports.getMetaTemplate = getMetaTemplate;
exports.getValueTemplate = getValueTemplate;