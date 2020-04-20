import os
import ast
from multiprocessing import Pool, Process, Manager
from subprocess import Popen, PIPE
import socket

m1 = Manager()
processes = m1.list()
cores = m1.list()
tpc_results = m1.dict()

tasks = []
edges = []

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


#Run the processes on different cores
def run_process(process,tname,cpu,cores,tpc_results,processes,edges):
    # x = os.system('python {}.py'.format(process))
    print "tname cpu",tname,cpu
    stdout = Popen('taskset -c {} python {}'.format(cpu,process), shell=True, stdout=PIPE).stdout
    output = ast.literal_eval(stdout.read().rstrip())
    outstr = ''
    for j in range(len(output)):
        outstr+=(str(output[j]))
        if j!=len(output)-1:
            outstr+=','
    print "tname outstr",tname,outstr
    tpc_results[tname] = outstr

    c = find_children(edges,tname)
    ancestors_executed = 0
    for j in c:
        par = find_parents(edges,j)
        for parent in par:
            if parent not in tpc_results:
                ancestors_executed = 1
                break
        if ancestors_executed == 0:
            processes.append(j)
    print "processes",processes
    if tname == 'sink':
        print tpc_results[tname]
        return

    cores[cpu-1]=0              #Mark the cpu as idle


# ---------------------------Main program--------------------------------------
if __name__ == '__main__':
    processes.append('src')
    for _ in range(3):
        cores.append(0)
    read_task_graph()
    # processes = ['src']
    # cores = [0,0,0]
    # tpc_results={}
    while True:
        if len(processes)==0:
            break
        tname = processes[0]
        cmdarg = tname
        cmdarg += '.py '
        par = find_parents(edges,tname)
        for k in par:
            cmdarg += tpc_results[k]
            cmdarg += ' '

        #Find an idle core to schedule the process
        i=0
        cpu = 0
        while True:
            if cores[i]==0:
                cpu = i+1
                break
            else:
                i = (i+1)%3

        processes.remove(tname)                                     #Remove the executed task from the list

        cores[cpu-1]=1                                              #Mark the cpu as busy
        p = Process(target=run_process, args=[cmdarg,tname,cpu,cores,tpc_results,processes,edges])
        p.start()
        # os.system("taskset -p -c %d %d" % (cpu, p.pid))             #Set the cpu affinity of the process to the availabe core
        p.join()
