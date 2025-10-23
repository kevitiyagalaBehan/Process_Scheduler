import random, heapq
from collections import deque
import statistics
import matplotlib.pyplot as plt


class Scheduler:
    def __init__(self, arrival_rate, service_mean, no_cores=4, sim_time=3600):
        self.lambda_rate = arrival_rate
        self.service_mean = service_mean
        self.no_cores = no_cores
        self.sim_time = sim_time

    def run(self, seed=None):
        if seed:
            random.seed(seed)

        now = 0.0
        event_q = []
        ready_q = deque()
        cores = [None] * self.no_cores

        tasks = {}
        task_id = 0
        total_busy = [0.0] * self.no_cores
        last_time = 0.0

        #first arrival
        first_arrival = random.expovariate(self.lambda_rate)
        heapq.heappush(event_q, (first_arrival, 'arrival', None, None))

        while event_q:
            event = heapq.heappop(event_q)
            time, evtype, core_idx, tid = event
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
                service_time = random.expovariate(1 / self.service_mean)
                tasks[task_id] = {'arrival': now, 'service': service_time, 'start': None, 'finish': None}

                ready_q.append(task_id)

                next_arrival = now + random.expovariate(self.lambda_rate)
                heapq.heappush(event_q, (next_arrival, 'arrival', None, None))

                self.dispatch(now, ready_q, cores, tasks, event_q)

            elif evtype == 'finish':
                tasks[tid]['finish'] = now
                cores[core_idx] = None
                self.dispatch(now, ready_q, cores, tasks, event_q)

        waits = [t['start'] - t['arrival'] for t in tasks.values() if t['start']]
        turns = [t['finish'] - t['arrival'] for t in tasks.values() if t['finish']]
        avg_wait = statistics.mean(waits) if waits else 0
        avg_turn = statistics.mean(turns) if turns else 0
        util = sum(total_busy) / (self.no_cores * self.sim_time)
        throughput = len(turns) / (self.sim_time / 3600)

        print(f"\nSimulation completed:")
        print(f" Tasks completed: {len(turns)}")
        print(f" Average Wait: {avg_wait:.2f}s")
        print(f" Average turnaround: {avg_turn:.2f}s")
        print(f" CPU Utilization: {util:.2%}")
        print(f" Throughput: {throughput:.1f} tasks/hr")

        return {
            'avg_wait': avg_wait,
            'avg_turn': avg_turn,
            'utilization': util,
            'throughput': throughput
        }

    def dispatch(self, now, ready_q, cores, tasks, event_q):
        for i in range(self.no_cores):
            if cores[i] is None and ready_q:
                tid = ready_q.popleft()
                tasks[tid]['start'] = now
                service = tasks[tid]['service']
                finish_time = now + service
                cores[i] = tid
                heapq.heappush(event_q, (finish_time, 'finish', i, tid))


if __name__ == "__main__":
    sim = Scheduler(arrival_rate=0.05, service_mean=10, no_cores=4, sim_time=3600)
    result = sim.run(seed=42)

    loads = [0.02, 0.05, 0.08]
    results = []
    for rate in loads:
        sim = Scheduler(arrival_rate=rate, service_mean=10, no_cores=4, sim_time=3600)
        results.append(sim.run())

    plt.figure(figsize=(6, 4))
    plt.plot([r * 3600 for r in loads], [res['avg_wait'] for res in results], marker='o')
    plt.title("Average waiting time vs Load (FCFS)")
    plt.xlabel("Arrival rate (tasks/hr)")
    plt.ylabel("Average wait (s)")
    plt.tight_layout()
    plt.show()
