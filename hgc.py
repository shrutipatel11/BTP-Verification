import os
import ast
import time
from multiprocessing import Pool
from multiprocessing import Process
from subprocess import Popen, PIPE
import socket

tpc_results = {}
tpc_executed = {}
processes = ['src']
hgc_executed = {}     #0 if the task is not executed, 1 if it is executed
hgc_results = {}     #stores the output of a task in the form of a string

tasks = []
edges = []

#recive the task outputs of 3pc via sockets
def get_result_from_3pc():
    s = socket.socket()
    port = 12345
    s.connect(('127.0.0.1', port))
    while 1:
        str_rcvd = s.recv(1024)
        arr = map(str,str_rcvd.split(' '))
        print "-------hgc received-----------"
        tpc_results[arr[0]]=arr[1]
        tpc_executed[arr[0]]=1
        c = find_children(edges,arr[0])
        for j in c:
            hgc_executed[j]=0
            processes.append(j)
        if arr[0]=='sink':
            break

#Find children of a node(task)
def find_children(edge_list, parent):
    children = []
    for element in edge_list:
        if element[0] == parent:
            children.append(element[1])
    return children

#Find parents of a node(task)
def find_parents(edge_list, child):
    parents = []
    for element in edge_list:
        if element[1] == child:
            parents.append(element[0])
    return parents

#Run the processes on different cores
def run_process(process):
    # x = os.system('python {}.py'.format(process))
    stdout = Popen('python {}'.format(process), shell=True, stdout=PIPE).stdout
    output = ast.literal_eval(stdout.read().rstrip())
    return output


def process_task_graph():
    while True:
        print processes
        print tpc_results

        #remove the processes whose ancestors have not yet completed the execution in 3pc and hgc both
        if len(processes)!=1 and processes[0]!='src':
            rmproc = []
            for i in processes:
                p = find_parents(edges,i)
                for j in p:
                    if hgc_executed[j]!=1 and (j not in tpc_results):       #ISSUE 1 : tpc_results is always empty empty so it removes the processses
                        rmproc.append(i)                                    #for which we have the inpur fron 3PC. tpc_results is updated in the
                        break                                               #function get_result_from_3pc
            for i in rmproc:
                processes.remove(i)
                print "removed",i

        # print "processes",processes

        #for the remaining processes,find the inputs (arguments) from the outputs stored in hgc_results or tpc_results dict
        cmdarg = []
        for j in processes:
            tempstr = j
            tempstr+='.py '
            par = find_parents(edges,j)
            for k in par:
                if k in hgc_results:
                    print "Output taken from HGC"
                    tempstr+=hgc_results[k]
                    tempstr+=' '
                else:
                    print "Output taken from 3PC"                           #ISSUE 2 : Control never goes to the else block due to ISSUE 1
                    tempstr+=tpc_results[k]
                    tempstr+=' '
            cmdarg.append(tempstr)

        #schedule the remaining processes
        k = len(processes)
        pool = Pool(processes=k)
        z = pool.map(run_process, cmdarg)


        #if the last task is reached, end the loop
        if 'sink' in processes:
            print z[0][0]
            break

        #set value 1 for completed processes
        for i in range(len(processes)):
            hgc_executed[processes[i]]=1
            #store the output of the task as a string
            outstr = ''
            for j in range(len(z[i])):
                outstr+=(str(z[i][j]))
                if j!=len(z[i])-1:
                    outstr+=','
            hgc_results[processes[i]]=outstr

        #add the children processes of the completed processes
        for i in processes:
            c = find_children(edges,i)
            for j in c:
                if j not in processes:
                    hgc_executed[j]=0
                    processes.append(j)

        #remove the processes that have completed its execution
        rm = []
        for i in processes:
            if hgc_executed[i]==1:
                rm.append(i)
        for i in rm:
            processes.remove(i)

        time.sleep(1)


#--------------------------------------Main function------------------------------------------#

# Read the task graph
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



p1 = Process(target = get_result_from_3pc)
p1.start()
p2 = Process(target = process_task_graph)
p2.start()
p1.join()
p2.join()
