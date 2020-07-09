module.exports = class Semaphore {
    level = null;
    max = null
    // syncPromise = null;
    q = null;

    constructor(level) {
        this.max = level;
        this.level = level;
        // this.syncPromise = new Promise((resolve) => {
        //     this.unlock = resolve;
        // });
        this.acquired = 0;
        this.q = [];
    }

    acquire() {
        let unblock = null;
        let block = new Promise((resolve) => {
            unblock = resolve;
        });
        if(level > 0) {
            level--;
            unblock();
        }
        else {
            this.q.push(unblock);
        }
        return block;
    }

    release() {
        if(++this.acquired > max) {
            throw new Error("Semaphore released too many times");
        }
        if(this.q.length > 0) {
            this.q.shift()();
        }
    }


}