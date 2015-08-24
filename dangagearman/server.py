
import logging
import random
import time
import eventloop as asyncore
import socket
from collections import deque
from dangagearman.protocol import DEFAULT_PORT, ProtocolError, parse_command, pack_command

class GearmanServerClient(asyncore.dispatcher):
    def __init__(self, sock, addr, server, manager):
        asyncore.dispatcher.__init__(self, sock)
        self.addr = addr
        self.server = server
        self.manager = manager
        self.in_buffer = ""
        self.out_buffer = ""
        manager.register_client(self)

    def log_info(self, message, type='info'):
        logging.warning(message)

    def writable(self):
        return len(self.out_buffer) != 0

    def handle_close(self):
        try:
            self.close()
        except Exception:
            logging.error('close error %s'%str(self.addr))
        self.manager.deregister_client(self)

    def handle_read(self):
        data = self.recv(8192)
        if not data:
            return

        self.in_buffer += data

        while True:
            try:
                func, args, cmd_len = parse_command(self.in_buffer, response=False)
            except ProtocolError, exc:
                logging.error("[%s] ProtocolError: %s" % (self.addr, str(exc)))
                self.close()
                return

            if not func:
                break

            self.handle_command(func, args)

            self.in_buffer = buffer(self.in_buffer, cmd_len)

    def handle_command(self, func, args):
        if func == "echo_req":
            self.send_command("echo_res", args)
        elif func == "submit_job":
            handle = self.manager.add_job(self, **args)
            self.send_command("job_created", {'handle': handle})
        elif func == "submit_job_high":
            handle = self.manager.add_job(self, high=True, **args)
            self.send_command("job_created", {'handle': handle})
        elif func == "submit_job_bg":
            handle = self.manager.add_job(self, bg=True, **args)
            self.send_command("job_created", {'handle': handle})
        elif func in ("can_do", "can_do_timeout"):
            self.manager.can_do(self, **args)
        elif func == "cant_do":
            self.manager.cant_do(self, **args)
        elif func == "grab_job":
            job = self.manager.grab_job(self)
            if job:
                self.send_command("job_assign", {'handle':job.handle, 'func':job.func, 'arg':job.arg})
            else:
                self.send_command("no_job")
        elif func == "pre_sleep":
            if not self.manager.sleep(self):
                self.wakeup()
        elif func == "work_complete":
            self.manager.work_complete(self, **args)
        elif func == "work_data":
            self.manager.work_data(self, **args)
        elif func == "work_fail":
            self.manager.work_fail(self, **args)
        # Text commands
        elif func == "status":
            status = self.manager.get_status(self)
            for s in status:
                self.send_buffered("%s\t%d\t%d\t%d\n" % (s['func'], s['num_jobs'], s['num_working'], s['num_workers']))
            self.send_buffered(".\n")
        elif func == "version":
            from dangagearman import __version__
            self.send_buffered("%s\n" % __version__)
        elif func == "workers":
            for client, state in self.manager.states.items():
                # if not state.abilities:
                #     continue
                self.send_buffered("%d %s %s : %s\n" % (client.socket.fileno(), client.addr[0], state.client_id, " ".join(state.abilities)))
            self.send_buffered(".\n")
        # elif func == "maxqueue":
        # 
        #     This sets the maximum queue size for a function. If no size is
        #     given, the default is used. If the size is negative, then the queue
        #     is set to be unlimited. This sends back a single line with "OK".
        # 
        #     Arguments:
        #     - Function name.
        #     - Optional maximum queue size.
        # 
        elif func == "shutdown":
            # TODO: optional "graceful" argument - close listening socket and let all existing connections complete
            self.server.stop()
        elif func == "set_client_id":
            if self in self.manager.states:
                self.manager.states[self].client_id = args['client_id']
        else:
            logging.error("Unhandled command %s: %s" % (func, args))

    def handle_write(self):
        if len(self.out_buffer) == 0:
            return 0

        try:
            nsent = self.send(self.out_buffer)
        except socket.error:
            self.close()
            return

        self.out_buffer = buffer(self.out_buffer, nsent)

    def send_buffered(self, data):
        self.out_buffer += data

    def send_command(self, name, kwargs={}):
        self.send_buffered(pack_command(name, response=True, **kwargs))

    def wakeup(self):
        self.send_command('noop')

    def work_complete(self, handle, result):
        self.send_command('work_complete', {'handle':handle, 'result':result})

    def work_data(self, handle, data):
        self.send_command('work_data', {'handle':handle, 'data':data})

    def work_fail(self, handle):
        self.send_command('work_fail', {'handle':handle})

class Job(object):
    def __init__(self, owner, handle, func, arg, bg=False, high=False, uniq=None):
        self.owner = owner
        self.handle = handle
        self.func = func
        self.arg = arg
        self.bg = bg
        self.high = high
        self.uniq = uniq
        self.worker = None
        self.timeout = None

class ClientState(object):
    def __init__(self, client):
        self.client = client
        self.sleeping = False
        self.client_id = "-"
        # Clients
        self.jobs = []
        # Workers
        self.abilities = {}
        self.working = []

class GearmanTaskManager(object):
    def __init__(self, trytimes=0):
        self.max_id = 0
        self.states = {}     # {client: ClientState}
        self.jobqueue = {}   # {function, [job]}
        self.jobs = {}       # {handle: job}
        self.uniq_jobs = {}  # {function: {uniq: job}}
        self.workers = {}    # {function: [state]}
        self.working = set() # set([job])
        self.trytimes = trytimes

    def add_job(self, client, func, arg, uniq=None, high=False, bg=False):
        state = self.states[client]
        job = Job(state, self.new_handle(), func=func, arg=arg, uniq=uniq, high=False, bg=False)
        state.jobs.append(job)
        if func not in self.jobqueue:
            self.jobqueue[func] = deque([job])
        else:
            self.jobqueue[func].append(job)
        self.jobs[job.handle] = job
        workers = self.workers.get(func, [])
        wakeTimes = 0
        for w in workers:
            if w.sleeping:
                if self.trytimes and wakeTimes > self.trytimes:
                    break
                w.client.wakeup()
                wakeTimes = wakeTimes + 1
        return job.handle

    def can_do(self, client, func, timeout=None):
        state = self.states[client]
        state.abilities[func] = int(timeout) if timeout else None

        if func not in self.workers:
            self.workers[func] = set((state,))
        else:
            self.workers[func].add(state)

    def cant_do(self, client, func):
        state = self.states[client]
        state.abilities.pop(func, None)
        self.workers[func].pop(state, None)

    def grab_job(self, client, grab=True):
        state = self.states[client]
        state.sleeping = False
        abilities = state.abilities.keys()
        random.shuffle(abilities)
        for f in abilities:
            jobs = self.jobqueue.get(f)
            if jobs:
                if not grab:
                    return True

                job = jobs.popleft()
                job.worker = state
                timeout = state.abilities[f]
                job.timeout = time.time() + timeout if timeout else None
                self.working.add(job)
                state.working.append(job)
                return job
                
        return None

    def sleep(self, client):
        has_job = self.grab_job(client, False)
        if has_job:
            return False
        state = self.states[client]
        state.sleeping = True
        return True

    def work_complete(self, client, handle, result):
        try:
            job = self.jobs[handle]
        except KeyError:
            logging.error('work_complete jod not found handle:%s'%str(handle))
            return
        job.owner.client.work_complete(handle, result)
        self._remove_job(job)

    def work_data(self, client, handle, data):
        try:
            job = self.jobs[handle]
        except KeyError:
            logging.error('work_data jod not found handle:%s'%str(handle))
            return
        job.owner.client.work_data(handle, data)

    def work_fail(self, client, handle):
        try:
            job = self.jobs[handle]
        except KeyError:
            logging.error('work_fail jod not found handle:%s'%str(handle))
            return False

        job.owner.client.work_fail(handle)
        self._remove_job(job)
        return True

    def _remove_job(self, job):
        job.owner.jobs.remove(job)
        job.worker.working.remove(job)
        self.working.discard(job)

    def get_status(self, client):
        funcs = set(self.workers.keys()) | set(self.jobqueue.keys())
        status = []
        for f in sorted(funcs):
            workers = self.workers.get(f, [])
            num_workers = len(workers)
            num_working = len(self.working)
            num_jobs = num_working + len(self.jobs.get(f, []))
            status.append(dict(
                func = f,
                num_jobs = num_jobs,
                num_working = num_working,
                num_workers = num_workers,
            ))
        return status

    def check_timeouts(self):
        now = time.time()
        to_fail = []
        for job in self.working:
            if job.timeout and job.timeout < now:
                to_fail.append(job)
        for job in to_fail:
            if not self.work_fail(None, job.handle):
                self.working.remove(job)

    def register_client(self, client):
        self.states[client] = ClientState(client)

    def deregister_client(self, client):
        try:
            state = self.states[client]
            del self.states[client]
        except KeyError:
            logging.error('deregister_client not found %s'%str(client.addr))
            return

        for f in state.abilities:
            self.workers[f].remove(state)

        for j in state.jobs:
            del self.jobs[j.handle]
            self.jobqueue[j.func].remove(j)

    def new_handle(self):
        #2^32
        if self.max_id >= 4294967295:
            self.max_id = 0
        self.max_id += 1
        return str(self.max_id)

class GearmanServer(asyncore.dispatcher):
    def __init__(self, port=DEFAULT_PORT, trytimes=0, backlog=64):
        asyncore.dispatcher.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(('', port))
        self.listen(backlog)
        self.manager = GearmanTaskManager(trytimes=trytimes)

    def handle_accept(self):
        sock, addr = self.accept()
        GearmanServerClient(sock, addr, self, self.manager)

    def start(self):
        self.running = True
        while self.running:
            asyncore.loop(timeout=1, use_poll=True, count=2)
            self.manager.check_timeouts()

    def stop(self):
        self.running = False
