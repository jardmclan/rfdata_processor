import importlib
import json
import subprocess


from threading import Thread
from multiprocessing import Process, Manager


from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor



module_name = "test"
out_dir = "./"

t_limit = 5
p_limit = 1

Module = importlib.import_module(module_name)

options = {

}


retry = 5
cleanup = True

t_exec = ThreadPoolExecutor(t_limit)

source = Module.Source(options)

def main():
    #handle process spawning
    p_exec = ProcessPoolExecutor(p_limit)
    #use main process for one
    for i in range(p_limit - 1):
        p_exec.submit(processData, i)
    processData(p_limit - 1)
    




def __write_doc(fpath, doc):
    error = None
    try:
        with open(fpath, "w") as of:
                json.dump(doc, of)
    except Exception as e:
        error = e
    return error


import os
def __cleanup_doc(fpath):
    error = None
    try:
        os.remove(fpath)
    except Exception as e:
        error = e
    return error


def __ingest_doc(fpath):
    error = None
    try:
        res = subprocess.run(["sh", ingestor_script, fpath], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            
        if len(res.stderr) > 0:
            error = res.stderr.decode("utf-8")
        elif res.returncode != 0:
            error = "Ingestor returned with code %d" % res.returncode
    except Exception as e:
        error = e
    return error
        


#most of data in file after write, all need
#because of GIL, computation in processes, I/O in threads
#pretty much all I/O, agave stuff in a separate process anyway
#IPC uses network com, comparatively slow

def __run_ingestor(info, cleanup):
    status = {
        "success": True,
        "pof": None,
        "message": None
    }

    error = __write_doc(info["out_file"], info["data"])
    if error is not None:
        status["success"] = False
        status["pof"] = "write"
        status["message"] = str(error)
    else:
        error = __ingest_doc(info["out_file"])
        if error is not None:
            status["success"] = False
            status["pof"] = "ingest"
            status["message"] = str(error)
            #still cleanup, but dont store status
            if cleanup:
                #execute in separate thread since status don't need output, can return status immediately
                #submit aquires lock internally so should be threadsafe (_shutdown_lock)
                #note this may cause shutdown issues, workaround>?
                t_exec.submit(__cleanup_doc, info["out_file"])
        elif cleanup:
            error = __cleanup_doc(info["out_file"])
            if error is not None:
                status["pof"] = "cleanup"
                status["message"] = str(error)

    return status


ingestor_script = "../bin/agave_local/add_meta.sh"
#to add update can add param to info docs and branch to an update proc
def doc_ingestor(info, retry, cleanup):
    status = None
    for i in range(retry):
        status = __run_ingestor(info, cleanup)
        if status["success"]:
            break
    return status



def processData(chunk):
    doc_id = 0

    def ingestor_complete_cb(future):
        status = future.result()
        print(status)

   
    def data_handler(data):
        nonlocal doc_id
        print(doc_id)
        fname = "test_%s.json" % str(doc_id)
        #need to lock? not atomic, but not multithreaded unless threading document generator
        doc_id = doc_id + 1
        out_file = os.path.join(out_dir, fname)
        info = {
            "data": data,
            "out_file": out_file
        }
        future = t_exec.submit(doc_ingestor, info, retry, cleanup)
        future.add_done_callback(ingestor_complete_cb)

    source.on("data", data_handler)

    #use main thread to execute, callbacks are fast so can be run inline, maybe switch to threaded event system? is it worth it?
    source.execute()



if __name__ == "__main__":
    main()




        




