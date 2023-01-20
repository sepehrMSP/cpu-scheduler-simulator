import numpy as np


class Job:
    def __init__(self, arrival, service_time, timeout, priority) -> None:
        self.arrival = arrival
        self.service_time = service_time
        self.timeout = timeout
        self.waiting_time = 0
        self.priority  = priority

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
        jobs.append(Job(arrival, service_time, timeout, priority))
    return jobs

def job_loader():
    pass

jobs = job_creator(20, 1, 5, 10)
for j in jobs:
    print(j)