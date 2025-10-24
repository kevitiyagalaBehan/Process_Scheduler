import heapq
import random
import statistics
from collections import deque

import matplotlib.pyplot as plt


# Implements a First-Come-First-Served Scheduling Policy
class SchedulerFCFS:
    def __init__(self, arrival_rate, service_mean, no_cores=4, sim_time=3600):
        self.lambda_rate = arrival_rate  # Average task arrival rate (tasks/sec)
        self.service_mean = service_mean  # Mean service time for each task (seconds)
        self.no_cores = no_cores  # Number of CPU cores available
        self.sim_time = sim_time  # Total simulation duration (seconds)

    def run(self, seed=None):
        if seed:
            random.seed(seed)

        # Simulation time and structures
        now = 0.0
        event_q = []
        ready_q = deque()
        cores = [None] * self.no_cores

        # Performance tracking
        tasks = {}  # Task dictionary to store metrics
        task_id = 0
        total_busy = [0.0] * self.no_cores
        last_time = 0.0

        # Schedule first task arrival
        first_arrival = random.expovariate(self.lambda_rate)
        heapq.heappush(event_q, (first_arrival, 'arrival', None, None))

        while event_q:
            event = heapq.heappop(event_q)
            time, evtype, core_idx, tid = event
            if time > self.sim_time:
                break

            # Update CPU Utilization
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

                # Schedule next arrival
                next_arrival = now + random.expovariate(self.lambda_rate)
                heapq.heappush(event_q, (next_arrival, 'arrival', None, None))

                self.dispatch(now, ready_q, cores, tasks, event_q)

            elif evtype == 'finish':
                tasks[tid]['finish'] = now
                cores[core_idx] = None
                self.dispatch(now, ready_q, cores, tasks, event_q)

        # Performance calculations
        waits = [t['start'] - t['arrival'] for t in tasks.values() if t['start']]
        turns = [t['finish'] - t['arrival'] for t in tasks.values() if t['finish']]
        avg_wait = statistics.mean(waits) if waits else 0
        avg_turn = statistics.mean(turns) if turns else 0
        util = sum(total_busy) / (self.no_cores * self.sim_time)
        throughput = len(turns) / (self.sim_time / 3600)

        return {
            'avg_wait': avg_wait,
            'avg_turn': avg_turn,
            'utilization': util,
            'throughput': throughput
        }

    # Assign waiting tasks to available CPU cores
    def dispatch(self, now, ready_q, cores, tasks, event_q):
        for i in range(self.no_cores):
            if cores[i] is None and ready_q:
                tid = ready_q.popleft()
                tasks[tid]['start'] = now
                service = tasks[tid]['service']
                finish_time = now + service
                cores[i] = tid
                heapq.heappush(event_q, (finish_time, 'finish', i, tid))


# Implements a Round Robin Scheduling Policy
class SchedulerRR:
    def __init__(self, arrival_rate, service_mean, time_quantum=5, no_cores=2, sim_time=3600):
        self.time_quantum = time_quantum
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

        # Schedule first arrival
        first_arrival = random.expovariate(self.lambda_rate)
        heapq.heappush(event_q, (first_arrival, 'arrival', None, None))

        while event_q:
            time, evtype, core_idx, tid = heapq.heappop(event_q)
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
                tasks[task_id] = {'arrival': now, 'remaining': service_time, 'start': None, 'finish': None}

                ready_q.append(task_id)

                # Schedule next arrival
                next_arrival = now + random.expovariate(self.lambda_rate)
                heapq.heappush(event_q, (next_arrival, 'arrival', None, None))

                self.dispatch(now, ready_q, cores, tasks, event_q)

            elif evtype in ('timeslice', 'finish'):
                if evtype == 'finish':
                    tasks[tid]['finish'] = now
                elif evtype == 'timeslice':
                    ready_q.append(tid)
                cores[core_idx] = None
                self.dispatch(now, ready_q, cores, tasks, event_q)

        waits = [t['start'] - t['arrival'] for t in tasks.values() if t['start']]
        turns = [t['finish'] - t['arrival'] for t in tasks.values() if t['finish']]
        avg_wait = statistics.mean(waits) if waits else 0
        avg_turn = statistics.mean(turns) if turns else 0
        util = sum(total_busy) / (self.no_cores * self.sim_time)
        throughput = len(turns) / (self.sim_time / 3600)

        return {'avg_wait': avg_wait, 'avg_turn': avg_turn, 'utilization': util, 'throughput': throughput}

    def dispatch(self, now, ready_q, cores, tasks, event_q):
        for i in range(self.no_cores):
            if cores[i] is None and ready_q:
                tid = ready_q.popleft()
                if tasks[tid]['start'] is None:
                    tasks[tid]['start'] = now
                remaining = tasks[tid]['remaining']
                run_time = min(self.time_quantum, remaining)
                tasks[tid]['remaining'] -= run_time
                finish_time = now + run_time
                cores[i] = tid
                evtype = 'finish' if tasks[tid]['remaining'] <= 0 else 'timeslice'
                heapq.heappush(event_q, (finish_time, evtype, i, tid))


# Run schedulers
if __name__ == "__main__":
    loads = [0.05, 0.1, 0.2]
    fcfs_res, rr_res = [], []

    for rate in loads:
        print(f"\n=== Load: {rate * 3600:.0f} tasks/hour ===")

        fcfs = SchedulerFCFS(arrival_rate=rate, service_mean=10)
        rr = SchedulerRR(arrival_rate=rate, service_mean=10, time_quantum=5)

        fcfs_result = fcfs.run(seed=42)
        rr_result = rr.run(seed=42)

        # Print comparative summary
        print(f"\n[FCFS Scheduler]")
        print(f"  Avg Wait Time:     {fcfs_result['avg_wait']:.2f} s")
        print(f"  Avg Turnaround:    {fcfs_result['avg_turn']:.2f} s")
        print(f"  CPU Utilization:   {fcfs_result['utilization']:.2%}")
        print(f"  Throughput:        {fcfs_result['throughput']:.1f} tasks/hr")

        print(f"\n[Round Robin Scheduler]")
        print(f"  Avg Wait Time:     {rr_result['avg_wait']:.2f} s")
        print(f"  Avg Turnaround:    {rr_result['avg_turn']:.2f} s")
        print(f"  CPU Utilization:   {rr_result['utilization']:.2%}")
        print(f"  Throughput:        {rr_result['throughput']:.1f} tasks/hr")

        fcfs_res.append(fcfs_result)
        rr_res.append(rr_result)


    # Plot 1 - Average Waiting Time vs Load
    plt.figure(figsize=(7, 5))
    plt.plot([r * 3600 for r in loads], [r['avg_wait'] for r in fcfs_res], marker='o', label='FCFS')
    plt.plot([r * 3600 for r in loads], [r['avg_wait'] for r in rr_res], marker='o', label='Round Robin')
    plt.title("Average Waiting Time vs Load (FCFS vs RR)")
    plt.xlabel("Arrival Rate (tasks/hr)")
    plt.ylabel("Average Wait (s)")
    plt.legend()
    plt.tight_layout()
    plt.show()

    # Plot 2 - CPU Utilization vs Load
    plt.figure(figsize=(7, 5))
    plt.plot([r * 3600 for r in loads], [r['utilization'] * 100 for r in fcfs_res], marker='o', label='FCFS')
    plt.plot([r * 3600 for r in loads], [r['utilization'] * 100 for r in rr_res], marker='o', label='Round Robin')
    plt.title("CPU Utilization vs Load")
    plt.xlabel("Arrival Rate (tasks/hr)")
    plt.ylabel("CPU Utilization (%)")
    plt.legend()
    plt.tight_layout()
    plt.show()

    # Bar Chart - CPU Utilization at High Load
    methods = ['FCFS', 'Round Robin']
    util_values = [fcfs_res[-1]['utilization'] * 100, rr_res[-1]['utilization'] * 100]

    plt.figure(figsize=(6, 4))
    plt.bar(methods, util_values, color=['blue', 'orange'])
    plt.title("CPU Utilization Comparison (High Load)")
    plt.ylabel("Utilization (%)")
    plt.tight_layout()
    plt.show()
