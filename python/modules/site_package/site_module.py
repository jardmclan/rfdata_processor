
import csv
import json
import re

from events import Event

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, Queue


#NEW PLAN, WRITE JSON FILES IN MODULES, HAVE REQUIRED OPTION FOR MODULES THAT GIVES OUTPUT QUEUE, SEND FNAMES IN OUTPUT QUEUE, RUN THESE IN SEPARATE PROCESS
#SPECIFY THREADS FOR MODULES TOO, WRITE IN THREADS (I/O BOUND)
#BREAKING UP ACTUAL PROCESSING INTO PROCESSES OR THREADS HANDLED BY MODULE NOT DRIVER

#how about instead of making inherently multiprocessed just run a callback on filenames
#allows module to be multiprocessed if want
#otherwise main thread in original process just idle, since the ingestion stuff is I/O/subprocess bound it shouldn't gain much from having modules run in a separate process
#if they weren't they would interfere with each other anyway, basically just wasting main thread
#except writting files gets a separate processes thread pool if you do it in a separate process
#maybe that is the way to go?
#can still do callback structure, have callback push fnames to queue
#modules not inherently set up for separate process if change back

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

#add to options
t_limit = 5

class Source:
    
    def __init__(self, options):
        self.options = options
        self.t_exec = ThreadPoolExecutor(t_limit)

    def __date_header_to_iso(self, date_header):
        return
        
    def execute(self):
        in_file = self.options["in_file"]
        schema_file = self.options["schema_file"]

        schema = None
        with open(schema, "r") as f:
            schema = json.load(f)

        date_regex = re.compile(schema["date"])
        
        with open(in_file, "r") as f:
            reader = csv.reader(f, skipinitialspace = True)
            fields = reader.next()

            metadata_fields = []
            value_fields = []

            #need to make sure at least has skn field
            got_skn = False
            index = 0
            #push index translation tuples
            for field in fields:
                translation = schema["meta"].get(field)
                if translation is None:
                    if date_regex.match(field) is not None:
                        translation = __date_header_to_iso(field)
                        value_fields.append((index, translation))
                    else:
                        eprint("No schema translation for csv header %s" % field)
                else:
                    metadata_fields.append((index, translation))
                    if(translation == "skn"):
                        got_skn = True
                index += 1
            
            if not got_skn:
                raise TypeError("SKN header must be defined. No SKN header found.") 


            doc_id = 0
            for row in reader:
                metadata_doc = {}
                #should any of these be converted to numbers?
                for field in metadata_fields:
                    index = field[0]
                    value = row[index]
                    translation = field[1]
                    metadata_doc[translation] = value
                metadata_doc["dataset"] = self.option.dataset

                wrapped_metadata_doc = {
                    "name": self.options,
                    "value": metadata_doc
                }
                #lets get this working then move to the value docs
                file = "test"
                future = self.t_exec.submit(self.__write_doc, file, metadata_doc, self.options.retry)
                cb = self.__get_write_doc_cb(file)
                future.add_done_callback(cb)

                for field in value_fields:
                    index = field[0]
                    value = row[index]
                    #skip columns with no value
                    if value != self.options.novalue:
                        value_doc = {
                            "skn": metadata_doc["skn"],
                            "date": {
                                    "$date": date
                                },
                            "value": valuef,
                            "type": self.options.valueType,
                            "units": self.options.units,
                            "dataset": self.options.dataset
                        }



    def __write_doc(fpath, doc, retry):
        def try_write_doc(fpath, doc):
            error = None
            try:
                with open(self, fpath, "w") as of:
                        json.dump(doc, of)
            except Exception as e:
                error = e
            return error

        for i in range(retry):
            e = try_write_doc(fpath, doc)
            if e is None:
                break

        

    


    def __get_write_doc_cb(self, fname):
        def write_doc_cb(future):
            self.options.out_q.put(fname, True, None)
        return write_doc_cb


def main():
    s = Source({
        "in_file": "input/daily_rf_data_2019_11_27.csv",
        "scheme_file": "schema_translation.json"
    })

    s.execute()

if __name__ == "__main__":
    main()