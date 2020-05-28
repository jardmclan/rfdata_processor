#synchronous event handler (similar to js implementation)
class Event:

    def __init__(self):
        self.event_register = {}

        

    def emit(self, event, *args):
        cbs = self.event_register.get(event)
        if cbs is None:
            cbs = []
        for cb in cbs:
            cb(*args)
        

    def on(self, event, cb):
        cbs = self.event_register.get(event)
        if cbs is None:
            cbs = []
            self.event_register[event] = cbs
        cbs.append(cb)
