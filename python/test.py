
class Source:

    


    def __init__(self, options):
        self.event_register = {
            "data": [],
            "complete": [],
            "finished": []
        }

        self.options = options


    def execute(self):

        for i in range(1):
            self.__trigger_event("data", {"value": i})

        self.__trigger_event("complete")
        self.__trigger_event("finished")

    def on(self, event, cb):
        self.event_register[event].append(cb)

    def __trigger_event(self, event, value = None):
        for cb in self.event_register[event]:
            cb(value)