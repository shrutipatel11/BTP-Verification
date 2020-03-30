import os
import ast
from multiprocessing import Pool
from subprocess import Popen, PIPE
import socket

tasks = []
edges = []
s = socket.socket()
port  = 12345
s.bind(('127.0.0.1',port))
print "socket binded to %s" %(port)
s.listen(5)
print "socket is listening"

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

def run_process(process):
    # x = os.system('python {}.py'.format(process))
    stdout = Popen('python {}'.format(process), shell=True, stdout=PIPE).stdout
    output = ast.literal_eval(stdout.read().rstrip())
    return output

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

#Schedule the processes and start execution with the node 'src'
# processes = ['src.py', 'dotproduct.py 1,2,3,4,5,6']
# k=2
# pool = Pool(processes=k)
# z = pool.map(run_process, processes)
# print type(z)

processes = ['src']
dict = {'src':0}
output = {'src':''}

cli, addr = s.accept()
print 'Got connection from', addr

while True:
    #remove the processes whose ancestors have not yet completed the execution
    if len(processes)!=1 and processes[0]!='src':
        rmproc = []
        for i in processes:
            p = find_parents(edges,i)
            for j in p:
                if dict[j]!=1:
                    rmproc.append(i)
                    break
        for i in rmproc:
            processes.remove(i)

    print "processes",processes

    #for the remaining processes,find the inputs (arguments) from the outputs stored in output dict
    cmdarg = []
    for j in processes:
        tempstr = j
        tempstr+='.py '
        par = find_parents(edges,j)
        for k in par:
            tempstr+=output[k]
            tempstr+=' '
        cmdarg.append(tempstr)

    #schedule the remaining processes
    k = len(processes)
    pool = Pool(processes=k)
    z = pool.map(run_process, cmdarg)


    #if the last task is reached, end the loop
    if k==1 and processes[0]=='sink':
        last_str = 'sink '
        last_str+= str(z[0][0])
        cli.send(last_str)
        print z[0][0]
        cli.close()
        break

    #set value 1 for completed processes
    for i in range(len(processes)):
        dict[processes[i]]=1
        #store the output of the task as a string
        outstr = ''
        for j in range(len(z[i])):
            outstr+=(str(z[i][j]))
            if j!=len(z[i])-1:
                outstr+=','
        output[processes[i]]=outstr

        sendString = processes[i]+' '+outstr
        cli.send(sendString)

    #add the children processes of the completed processes
    for i in processes:
        c = find_children(edges,i)
        for j in c:
            dict[j]=0
            processes.append(j)

    #remove the processes that have completed its execution
    rm = []
    for i in processes:
        if dict[i]==1:
            rm.append(i)
    for i in rm:
        processes.remove(i)
