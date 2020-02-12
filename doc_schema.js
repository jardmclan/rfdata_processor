const dataset = "rfs_11_27_19";

export function getMetaTemplate() {
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
        dataset: dataset
    });
}

export function getValueTemplate() {
    return new DocTemplate({
        skn: null,
        type: "daily",
        //parse values as numbers, check if values are properly processed as numeric types during ingestion
        value: null,
        date: {
            $date: null
        },
        hourly: [],
        dataset: dataset
    }, {
        date: "$date"
    });
}

export class DocTemplate {
    schema;
    typedValues;

    constructor(schema, typedValues) {
        this.schema = schema;
        this.typedValues = typedValues == undefined ? {} : typedValues;
    }

    setProperty(label, value) {
        let typeLabel = typedValues[label];
        if(typeLabel != undefined) {
            schema[label][typeLabel] = value;
        }
        else {
            schema[label] = value;
        }
    }

    toJSON() {
        return schema;
    }
}