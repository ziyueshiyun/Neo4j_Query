# -*- coding=utf-8 -*-
from multiprocessing import Process, queues
import multiprocessing
import os
import time


def long_time_task(i):
    print('process:{}----任务{} 开始了'.format(os.getpid(), i))
    time.sleep(10)
    print('子进程:{}----任务{} 结束了'.format(os.getpid(), i))
    return i


if __name__ =='__main__':
    t1 = time.time()
    print('母亲进程:{}---开始了'.format(os.getpid()))
    queues = []
    for i in range(4):
        queue = multiprocessing.Queue()
        queues.append(queue)
        
    t2 = time.time()
    print('mother_process end {}total time={}'.format(os.getpid(), round(t2-t1,3)))
