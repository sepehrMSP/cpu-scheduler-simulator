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
        return f"arrival: {self.arrival},\t service_time: {round(self.service_time, 3)},\t timeout: {round(self.timeout, 3)},\t waiting_time: {self.waiting_time},\t priority: {self.priority}"

""" this function creates tasks with rate X in a poisson process.
    service time of these tasks follow an exponential distribution with
    mean Y. Each task has a timeout value which follows an exponential
    distribution with mean Z.
    priority of each task is determined randomly based on the below table:

    priority    |  low  |   normal  |   high
    ------------------------------------------
    probability |   0.7 |   0.2     |   0.1
"""
def job_creator(number_of_jobs, X, Y, Z):
    arrivals = np.random.poisson(lam=X, size=number_of_jobs)
    service_times = np.random.exponential(scale=Y, size=number_of_jobs)
    timeouts = np.random.exponential(scale=Z, size=number_of_jobs)
    priorities = np.random.choice(np.arange(1, 4), p=[0.7, 0.2, 0.1], size=number_of_jobs)
    jobs = []
    for arrival, service_time, timeout, priority in zip(arrivals, service_times, timeouts, priorities):
        j = Job(arrival, service_time, timeout, priority)
        jobs.append(j)
    return jobs

""" Loads the top k jobs from the priority queue and inserts them
    into the RR-t1
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


if __name__ == "__main__":
    X = 1
    Y = 5
    Z = 10
    NUMBER_OF_JOBS = 20
    jobs = job_creator(NUMBER_OF_JOBS, X, Y, Z)

    waiting_list_round_robin_t1 = []
    waiting_list_round_robin_t2 = []
    waiting_list_FCFS = []
    priority_q = PriorityQueue()


    for i, job in enumerate(jobs):
        priority_q.put(job)

    while not priority_q.empty():
        print(priority_q.get())
