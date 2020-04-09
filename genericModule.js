module.exports = class GenericModule {
    constructor(source, options) {
        this._options = options;
        this._source = source;
        if(this.constructor === GenericModule) {
            throw new Error("Cannot instantiate GenericModule directly");
        }
    }

    //provides next json document
    //json doc should have name and value fields
    on(event, cb) {
        this._source.on(event, cb)
    }

    pause() {
        this._source.pause();
    }

    resume() {
        this._source.resume();
    }

    destroy() {
        this.source.destroy();
    }

    //parses a set of command line arguments into an options object for use in the constructor
    //returns an object containing a success flag, and the resulting options object or a help string if a help flag is specified or if parsing failed
    static parseArgs(args) {
        //not implemented in subclass, return error result with unsupported message
        return {
            success: false,
            result: "Module does not support argument parsing, try passing a JSON options string."
        };
    }
}