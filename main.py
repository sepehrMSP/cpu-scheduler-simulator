from typing import List
import numpy as np
from heapq import heappush, heappop, heapify
from termcolor import colored


class Job:
    def __init__(self, id, arrival, service_time, timeout, priority) -> None:
        self.id = id
        self.arrival = arrival
        self.service_time = service_time
        self.timeout = timeout
        self.waiting_time = 0
        self.priority  = priority

    def __eq__(self, other: 'Job') -> bool:
        if other is not None and self is not None:
            return self.id == other.id
        else:
            return False

    def __lt__(self, other: 'Job') -> bool:
        return (self.priority, self.waiting_time) > (other.priority, other.waiting_time)

    def __repr__(self) -> str:
        return f"arrival: {self.arrival},\t service_time: {round(self.service_time, 3)},\t timeout: {round(self.timeout, 3)},\t waiting_time: {self.waiting_time},\t priority: {self.priority}\n"


class Logger:
    def __init__(self, simulation_time):
        self.rr_t1_length = []
        self.rr_t2_length = []
        self.fcfs_length = []
        self.priority_q_length = []
        self.cpu_utilization = []
        self.simulation_time = simulation_time
    
    def log_length(self, rr_t1_length, rr_t2_length, fcfs_length, priority_q_length):
        self.rr_t1_length.append(rr_t1_length)
        self.rr_t2_length.append(rr_t2_length)
        self.fcfs_length.append(fcfs_length)
        self.priority_q_length.append(priority_q_length)

    def log_cpu_utilization(self, cpu_utilization):
        self.cpu_utilization.append(cpu_utilization)

    def text_output(self):
        print(f"""RR-T1 length: {sum(self.rr_t1_length)/self.simulation_time},
RR-T2 length: {sum(self.rr_t2_length)/self.simulation_time},
FCFS length: {sum(self.fcfs_length)/self.simulation_time},
Priority Queue length: {sum(self.priority_q_length)/self.simulation_time},
CPU utilization: {sum(self.cpu_utilization)/self.simulation_time}""")



def job_creator(number_of_jobs, X, Y, Z) -> List[Job]:
    """ 
    this function creates tasks with rate X in a poisson process.
    service time of these tasks follow an exponential distribution with
    mean Y. Each task has a timeout value which follows an exponential
    distribution with mean Z.
    priority of each task is determined randomly based on the below table:

    priority    |  low  |   normal  |   high
    ------------------------------------------
    probability |   0.7 |   0.2     |   0.1
    """
    arrivals = np.round(np.random.exponential(scale=X, size=number_of_jobs)) 
    service_times = np.random.exponential(scale=Y, size=number_of_jobs)
    timeouts = np.random.exponential(scale=Z, size=number_of_jobs)
    priorities = np.random.choice(np.arange(1, 4), p=[0.7, 0.2, 0.1], size=number_of_jobs)
    jobs: List[Job] = []
    sum_arrival = 0
    id_counter = 0
    for arrival, service_time, timeout, priority in zip(arrivals, service_times, timeouts, priorities):
        id_counter += 1
        j = Job(id_counter, arrival + sum_arrival, service_time, timeout, priority)
        sum_arrival += arrival
        jobs.append(j)

    jobs.sort(key=lambda x: x.arrival)
    return jobs

def job_loader(k: int):
    """ 
    Loads the top k jobs from the priority queue and inserts them
    into the RR-T1
    """
    counter = 0
    while counter < k and len(priority_q) > 0:
        waiting_list_round_robin_t1.append(heappop(priority_q))
        counter += 1

def transfer_tasks_from_priority_queue(k: int):
    """ 
    Check if number of jobs in the second layer queues are less than k
    to transfer new jobs from priority queue to RR-T1
    """
    if len(waiting_list_round_robin_t1) + \
        len(waiting_list_round_robin_t2) + \
        len(waiting_list_FCFS) < k:
        job_loader(k)

def select_second_layer_queue() -> str:
    """
    Function that selects the second layer queue based on the below table:

    queue       | RR-T1 |  RR-T2  |   FCFS
    ------------------------------------------
    probability |   0.8 |   0.1   |   0.1
    """
    prob = [0.8, 0.1, 0.1]
    queues = ['RR-T1', 'RR-T2', 'FCFS']
    queue = np.random.choice(queues, size=1, p=prob)
    return queue

def update_waiting_time(passed_time, running_job):
    """
    Adds the passed time to the waiting time of all jobs in all queues.
    """
    for j in waiting_list_round_robin_t1:
        if j != running_job:
            j.waiting_time += passed_time
    for j in waiting_list_round_robin_t2:
        if j != running_job:
            j.waiting_time += passed_time
    for j in waiting_list_FCFS:
        if j != running_job:
            j.waiting_time += passed_time
    for j in priority_q:
        j.waiting_time += passed_time

def remove_starved_jobs_from_list(l: List[Job]):
    """
    Generic function to take a list as input and remove every starved task from the list.
    """
    removed_list: List[Job] = []
    i = 0
    while i<len(l):
        if l[i].waiting_time > l[i].timeout:
            removed_list.append(l.pop(i))
        else:
            i += 1
    return removed_list


def remove_starved_jobs_from_all_lists():
    """
    function that removes starved jobs from all queues.
    """
    removed_jobs: List[Job] = []
    removed_jobs = removed_jobs + remove_starved_jobs_from_list(waiting_list_round_robin_t1)
    removed_jobs = removed_jobs + remove_starved_jobs_from_list(waiting_list_round_robin_t2)
    removed_jobs = removed_jobs + remove_starved_jobs_from_list(waiting_list_FCFS)
    removed_jobs = removed_jobs + remove_starved_jobs_from_list(priority_q)
    heapify(priority_q)
    return removed_jobs

def dispatcher():
    """
    Dispatcher function that behaves like the suggested algorithm in the project documentation.
    """
    queue = select_second_layer_queue()
    execution_time = 0
    job = None
    if queue == 'RR-T1':
        if len(waiting_list_round_robin_t1) > 0:
            job = waiting_list_round_robin_t1.pop(0)
            execution_time = min(T1, job.service_time)
            if job.service_time > T1:
                job.service_time -= T1
                waiting_list_round_robin_t2.append(job)
            else:
                job.service_time = 0
        else:
            queue = 'RR-T2'

    if queue == 'RR-T2':
        if len(waiting_list_round_robin_t2) > 0:
            job = waiting_list_round_robin_t2.pop(0)
            execution_time = min(T2, job.service_time)
            if job.service_time > T2:
                job.service_time -= T2
                waiting_list_FCFS.append(job)
            else:
                job.service_time = 0
        else:
            queue = 'FCFS'

    if queue == 'FCFS':
        if len(waiting_list_FCFS) > 0:
            job = waiting_list_FCFS.pop(0)
            execution_time = job.service_time
            job.service_time = 0

    return execution_time, job

def print_all_waiting_times():
    """
    Helper function for calculating the average waiting time of jobs in all queues and printing them to output.
    """
    waiting_times: List[int] = []
    waiting_times = waiting_times + [j.waiting_time for j in waiting_list_round_robin_t1]
    waiting_times = waiting_times + [j.waiting_time for j in waiting_list_round_robin_t2]
    waiting_times = waiting_times + [j.waiting_time for j in waiting_list_FCFS]
    waiting_times = waiting_times + [j.waiting_time for j in priority_q]
    waiting_times = waiting_times + [j.waiting_time for j in removed_jobs]
    waiting_times = waiting_times + [j.waiting_time for j in finished_jobs]
    print(f"Waiting times: {sum(waiting_times)/NUMBER_OF_JOBS}")

# Tasks:
# 6. Write the final report in the README.md
# 8. Write docstrings
if __name__ == "__main__":
    X = 10
    Y = 5
    Z = 10
    NUMBER_OF_JOBS = 1000
    SIMULATION_TIME = 10000
    SIMULATION_PERIOD = 4
    K = 5
    T1 = 5
    T2 = 5    

    logger = Logger(SIMULATION_TIME)

    jobs = job_creator(NUMBER_OF_JOBS, X, Y, Z)

    waiting_list_round_robin_t1: List[Job] = []
    waiting_list_round_robin_t2: List[Job] = []
    waiting_list_FCFS: List[Job] = []
    priority_q: List[Job] = []
    removed_jobs: List[Job] = []
    finished_jobs: List[Job] = []
    
    current_time = 0
    execution_time = 0
    running_job = None
    while (current_time < SIMULATION_TIME):
        #assining the created jobs into priority_q based on their arrival time
        while len(jobs) > 0:
            if current_time == jobs[0].arrival:
                j = jobs.pop(0)
                heappush(priority_q, j)
            else:
                break

        if current_time % SIMULATION_PERIOD == 0:
            transfer_tasks_from_priority_queue(K)

        if execution_time == 0:
            execution_time, job = dispatcher()
            running_job = job

        current_time += 1
        if 0 < execution_time <= 1:
            logger.log_cpu_utilization(execution_time)
            if running_job.service_time == 0:
                finished_jobs.append(running_job)
            running_job = None
        elif execution_time > 1:
            logger.log_cpu_utilization(1)
        execution_time = max(execution_time - 1, 0)
        update_waiting_time(1, running_job)
        removed_jobs = removed_jobs + remove_starved_jobs_from_all_lists()
        logger.log_length(len(waiting_list_round_robin_t1), len(waiting_list_round_robin_t2), len(waiting_list_FCFS), len(priority_q))
        
        if running_job is None and len(finished_jobs) + len (removed_jobs) == NUMBER_OF_JOBS:
            print(colored(f"All tasks are either finished or removed! Finishing execution after {current_time} seconds!", color="red"))
            break
    logger.simulation_time = current_time
    logger.text_output()

    print_all_waiting_times()
    print(f"Removed jobs(/All ended jobs): {len(removed_jobs)/(len(finished_jobs)+len(removed_jobs))}")
    print(f"Removed jobs(/All jobs): {len(removed_jobs)/NUMBER_OF_JOBS}")
