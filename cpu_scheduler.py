import random, heapq
from collections import deque


class Scheduler:
    def __init__(self, arrival_rate, service_mean, no_cores=4, sim_time=3600):
        self.lambda_rate = arrival_rate
        self.service_mean = service_mean
        self.no_cores = no_cores
        self.sim_time = sim_time

    def run(self):
        now = 0.0
        event_q = []
        ready_q = deque()
        cores = [None]*self.no_cores

        tasks = {}
        task_id = 0
        total_busy = [0.0]*self.no_cores
        last_time = 0.0

        #First arrival
        first_arrival = random.expovariate(self.lambda_rate)
        heapq.heappush(event_q, (first_arrival, 'arrival'))

        while event_q:
            time, evtype = heapq.heappop(event_q)
            if time > self.sim_time:
                break
            dt = time - last_time
            for i in range(self.no_cores):
                if cores[i] is not None:
                    total_busy[i] += dt
            last_time = time
            now = time

            if evtype == 'arrival':
                task_id += 1
                service_time = random.expovariate(1/self.service_mean)
                tasks[task_id] = {'arrival': now, 'service': service_time, 'start': None, 'finish': None}
                ready_q.append(task_id)
                next_arrival = now + random.expovariate(self.lambda_rate)
                heapq.heappush(event_q, (next_arrival, 'arrival'))
                self.dispatch(now, ready_q, cores, tasks, event_q)

            elif evtype == 'complete':
                core_idx, tid = heapq.heappop(event_q)
                pass