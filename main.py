from typing import List
import numpy as np
from queue import PriorityQueue


class Job:
    def __init__(self, arrival, service_time, timeout, priority) -> None:
        self.arrival = arrival
        self.service_time = service_time
        self.timeout = timeout
        self.waiting_time = 0
        self.priority  = priority

    def __lt__(self, other: 'Job') -> bool:
        return (self.priority, self.waiting_time) > (other.priority, other.waiting_time)

    def __repr__(self) -> str:
        return f"arrival: {self.arrival},\t service_time: {round(self.service_time, 3)},\t timeout: {round(self.timeout, 3)},\t waiting_time: {self.waiting_time},\t priority: {self.priority}\n"

""" this function creates tasks with rate X in a poisson process.
    service time of these tasks follow an exponential distribution with
    mean Y. Each task has a timeout value which follows an exponential
    distribution with mean Z.
    priority of each task is determined randomly based on the below table:

    priority    |  low  |   normal  |   high
    ------------------------------------------
    probability |   0.7 |   0.2     |   0.1
"""
def job_creator(number_of_jobs, X, Y, Z) -> List[Job]:
    arrivals = np.random.poisson(lam=X, size=number_of_jobs)
    service_times = np.random.exponential(scale=Y, size=number_of_jobs)
    timeouts = np.random.exponential(scale=Z, size=number_of_jobs)
    priorities = np.random.choice(np.arange(1, 4), p=[0.7, 0.2, 0.1], size=number_of_jobs)
    jobs = []
    sum_arrival = 0
    for arrival, service_time, timeout, priority in zip(arrivals, service_times, timeouts, priorities):
        j = Job(arrival + sum_arrival, service_time, timeout, priority)
        sum_arrival += arrival
        jobs.append(j)
    return jobs

""" Loads the top k jobs from the priority queue and inserts them
    into the RR-T1
"""
def job_loader(k: int):
    for _ in range(k):
        waiting_list_round_robin_t1.append(priority_q.get())

""" Check if number of jobs in the second layer queues are less than k
    to transfer new jobs from priority queue to RR-T1
"""
def transfer_tasks_from_priority_queue(k: int):
    if len(waiting_list_round_robin_t1) + \
        len(waiting_list_round_robin_t2) + \
        len(waiting_list_FCFS) < k:
        job_loader(k)

def select_second_layer_queue() -> str:
    prob = [0.8, 0.1, 0.1]
    queues = ['RR-T1', 'RR-T2', 'FCFS']
    queue = np.random.choice(queues, size=1, p=prob)
    return queue

def update_waiting_time(passed_time):
    for j in waiting_list_round_robin_t1:
        j.waiting_time += passed_time
    for j in waiting_list_round_robin_t2:
        j.waiting_time += passed_time
    for j in waiting_list_FCFS:
        j.waiting_time += passed_time

    for j in priority_q.queue:
        j.waiting_time += passed_time

def dispatcher():
    queue = select_second_layer_queue()
    execution_time = 0
    job = None
    if queue == 'RR-T1':
        if len(waiting_list_round_robin_t1) > 0:
            job = waiting_list_round_robin_t1.pop(0)
            execution_time = T1
        else:
            queue = 'RR-T2'

    if queue == 'RR-T2':
        if len(waiting_list_round_robin_t2) > 0:
            job = waiting_list_round_robin_t2.pop(0)
            execution_time = T2
        else:
            queue = 'FCFS'

    if queue == 'FCFS':
        if len(waiting_list_FCFS) > 0:
            job = waiting_list_FCFS.pop(0)
            execution_time = job.service_time

    return execution_time

if __name__ == "__main__":
    X = 1
    Y = 5
    Z = 10
    NUMBER_OF_JOBS = 20
    SIMULATION_TIME = 30
    K = 5
    T1 = 10
    T2 = 10

    jobs = job_creator(NUMBER_OF_JOBS, X, Y, Z)
    jobs.sort(key=lambda x: x.arrival)

    waiting_list_round_robin_t1: List[Job] = []
    waiting_list_round_robin_t2: List[Job] = []
    waiting_list_FCFS: List[Job] = []
    priority_q = PriorityQueue()


    # for i, job in enumerate(jobs):
    #     priority_q.put(job)

    # while not priority_q.empty():
    #     print(priority_q.get())

    current_time = 0
    execution_time = 0
    while current_time < SIMULATION_TIME:
        #assning the created jobs into priority_q based on their arrival time
        while len(jobs) > 0:
            if current_time == jobs[0].arrival:
                j = jobs.pop(0)
                priority_q.put(j)
            else:
                break

        if current_time % K == 0:
            transfer_tasks_from_priority_queue(K)

        if execution_time == 0:
            execution_time = dispatcher()

        current_time += 1
        execution_time -= 1
        update_waiting_time(1)
