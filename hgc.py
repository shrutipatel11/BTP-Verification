import os
import ast
import time
from multiprocessing import Pool, Process
from subprocess import Popen, PIPE
import socket
import multiprocessing

manager = multiprocessing.Manager()
tpc_results = manager.dict()
processes = manager.list()
processes.append('src')
hgc_results = {}     #stores the output of a task in the form of a string

tasks = []
edges = []

#recive the task outputs of 3pc via sockets
def get_result_from_3pc(tpc_results,processes):
    s = socket.socket()
    port = 12345
    s.connect(('127.0.0.1', port))
    while 1:
        str_rcvd = s.recv(1024)
        arr = map(str,str_rcvd.split(' '))
        # print "-------hgc received-----------"
        name = str(arr[0])
        toutput = str(arr[1])
        tpc_results[name]=toutput               #Stores the result received fron 3PC to tpc_results dictionary
        c = find_children(edges,name)           #Appends the children tasks of the received task
        for jj in c:
            if jj not in processes:
                processes.append(jj)
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


def process_task_graph(tpc_results,processes):
    flag = 0
    while True:
        print "processes",processes
        # print tpc_results

        #remove the processes whose ancestors have not yet completed the execution in 3pc and hgc both
        if len(processes)!=1 and processes[0]!='src':
            rmproc = []
            for i in processes:
                p = find_parents(edges,i)
                for j in p:
                    if (j not in hgc_results) and (j not in tpc_results):
                        rmproc.append(i)
                        break
            for i in rmproc:
                processes.remove(i)
                print "removed",i


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
                    print "Output taken from 3PC"
                    tempstr+=tpc_results[k]
                    tempstr+=' '
            cmdarg.append(tempstr)

        #schedule the remaining processes
        k = len(processes)
        pool = Pool(processes=k)
        z = pool.map(run_process, cmdarg)
        # print "z",z,processes

        #Store the results of the completed processes
        for i in range(k):
            #store the output of the task as a string
            outstr = ''
            for j in range(len(z[i])):
                outstr+=(str(z[i][j]))
                if j!=len(z[i])-1:
                    outstr+=','
            hgc_results[processes[i]]=outstr
            # print "Result added",processes[i],outstr

        #Check if any miscomputation has happened
        if len(tpc_results)!=0:
            misc = []
            for key in hgc_results:                                                          #Check if the 3PC and HGC results matches
                if (key in tpc_results) and tpc_results[key] != hgc_results[key]:            #Add those tasks whose results does not match in misc array
                    # print "Added process",key
                    misc.append(key)
                    flag=1
        if flag==1:                                                 #If miscomputation has occured
            tpc_results.clear()                                     #Clear all the results recevied fron #PC
            processes = []
            processes.extend(misc)                                  #Remove the outputs of miscomputed tasks and their children tasks fron hgc_results
            while 1:
                if len(misc)==0:
                    break
                if misc[0] in hgc_results:
                    del hgc_results[misc[0]]
                rmchild = misc[0]
                fchild = find_children(edges,rmchild)
                misc.remove(misc[0])
                misc.extend(fchild)
            flag=0

        #if the last task is reached, end the loop
        if 'sink' in processes:
            if 'sink' in hgc_results:
                print  hgc_results['sink']
                break
            elif 'sink' in tpc_results:
                print tpc_results['sink']
                break

        #add the children processes of the completed processes
        for i in processes:
            c = find_children(edges,i)
            for j in c:
                if j not in processes:
                    processes.append(j)

        #remove the processes that have completed its execution
        rm = []
        for i in processes:
            if i in hgc_results:
                rm.append(i)
        for i in rm:
            processes.remove(i)
        # print "processes and removed",processes,rm

        # time.sleep(1)


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



p1 = Process(target = get_result_from_3pc, args=[tpc_results,processes])
p2 = Process(target = process_task_graph, args=[tpc_results,processes])
p1.start()
p2.start()
p1.join()
p2.join()
