import os
import ast
from multiprocessing import Pool, Process, Manager
from subprocess import Popen, PIPE
import socket
import select

m1 = Manager()
processes = m1.list()
cores = m1.list()
tpc_results = m1.dict()

tasks = []
edges = []

s = [socket.socket() for _ in range(3)]
port = [10001, 10002, 10003]
client = []
addr = []

for i in range(len(s)):
    s[i].bind(('127.0.0.1', port[i]))
    print "Socket",i,"binded to",port[i]
    s[i].listen(5)
    print "Socket",i,"is listening"
    cl,ad = s[i].accept()
    client.append(cl)
    addr.append(ad)
    print "Got connection from",addr[i]

#Find children of a node
def find_children(edge_list, parent):
    children = []
    for element in edge_list:
        if element[0] == parent:
            children.append(element[1])
    return children

#Find parents of a node
def find_parents(edge_list, child):
    parents = []
    for element in edge_list:
        if element[1] == child:
            parents.append(element[0])
    return parents

# Read the task graph
def read_task_graph():
    f = open("bench","r")
    if f.mode=="r":
        contents = f.readlines()

        #store the graph nodes(tasks) and dependencies(edges) in the lists
        for x in contents:
            if x.find("TASK")!=-1:
                start_index = 5
                while True:
                    if x[start_index]==' ':
                        break
                    start_index+=1
                task_name = x[5:start_index]
                tasks.append(task_name)  #add the task to the task list

            elif x.find("ARC")!=-1:
                start_index = 14
                while True:
                    if x[start_index]==' ':
                        break
                    start_index+=1
                t1 = x[14:start_index]          #first task of the edge

                start_index2 = start_index+4
                while True:
                    if x[start_index2]==' ':
                        break
                    start_index2+=1
                t2 = x[start_index+4:start_index2]      #second task of the edge

                edges.append([t1,t2])    #add the edge to the edge list

def schedule_process(processes, tpc_results, cores, edges):
    i=0
    cpu = 0
    while True:
        #Find an idle core to schedule the process
        while True:
            if cores[i]==0:
                cpu = i
                break
            else:
                i = (i+1)%3

        if len(processes)==0:
            continue
        tname = processes[0]
        processes.remove(tname)
        cores[cpu]=1

        cmdarg = tname
        par = find_parents(edges,tname)
        for k in par:
            cmdarg += ' '
            cmdarg += tpc_results[k]
        print tname,"scheduled in cpu",cpu
        client[cpu].send(cmdarg)
        if tname=='sink':
            return

def receive_results(processes, tpc_results, cores, edges):
    timeout = 0.5  # in seconds
    j=0
    while True:
        ready_sockets, _, _ = select.select([client[j]], [], [], timeout)
        if ready_sockets:
            data = client[j].recv(10000)
            cores[j]=0
            print "Data received from socket",j
            name,result = data.split(" ")
            # print "---------------------------------------------------------"
            # print "Name, Result",name,result
            if  name == 'sink':
                print list(map(int,result.split(",")))
                return
            tpc_results[name] = result
            c = find_children(edges,name)
            ancestors_executed = 0
            for child in c:
                par = find_parents(edges,child)
                for parent in par:
                    if parent not in tpc_results:
                        ancestors_executed = 1
                        break
                if ancestors_executed == 0:
                    processes.append(child)
            # print "processes",processes
        else:
            j=(j+1)%3



# ---------------------------Main program--------------------------------------
if __name__ == '__main__':
    read_task_graph()
    processes.append('src')
    for _ in range(3):
        cores.append(0)
    p1 = Process(target = schedule_process, args=[processes, tpc_results, cores, edges])
    p2 = Process(target = receive_results, args=[processes, tpc_results, cores, edges])
    p1.start()
    p2.start()
    p1.join()
    p2.join()
