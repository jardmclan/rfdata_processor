
from events import Event

from modules.site_module.site_module import Source


class Source:


    def __init__(self, options):
        self.__event_source = Event()

        self.__options = options


    def execute(self):

        for i in range(10):
            acc = 0
            for j in range(1000000):
                acc += 2 + pow(1.22 * i / 1200.3, (j / 1990) * 1.1)
            self.__event_source.emit("data", {"name": "test", "value": i})

        self.__event_source.emit("complete")
        self.__event_source.emit("finished")

    def on(self, event, cb):
        self.__event_source.on(event, cb)



    