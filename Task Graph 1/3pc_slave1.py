import os
import ast
from multiprocessing import Pool, Process, Manager
from subprocess import Popen, PIPE
import socket
import select
import time

s = socket.socket()
port = 10001
s.connect(('127.0.0.1', port))

tsock = socket.socket()
tport = 9001
ip = '127.0.0.1'
tsock.bind(('127.0.0.1', tport))

def run_process(task,ip,port):
    os.system('./{} {} {}'.format(task,ip,port))

def connect(s,tsock,args,task):
    tsock.listen(5)
    cli, addr = tsock.accept()
    print 'Got connection from', addr
    if len(args)>0:
        for inputdata in args:
            print inputdata
            inplen = str(len(list(inputdata.split(","))))
            print "Inplen",inplen
            cli.send(inplen)
            time.sleep(0.5)
            cli.send(inputdata)

    result = task+' '
    while True:
        timeout = 0.5  # in seconds
        ready_sockets, _, _ = select.select([cli], [], [], timeout)
        if ready_sockets:
            result += cli.recv(10000)
            if len(cmdarg) !=0 :
                print ""
                print "Received from the client : ", result
                tsock.close()
                s.send(result)
                break


while True:
    timeout = 0.5  # in seconds
    ready_sockets, _, _ = select.select([s], [], [], timeout)
    if ready_sockets:
        cmdarg = s.recv(10000)
        if len(cmdarg)>0:
            print "--------------------------------------------------------------------"
            print "Received",cmdarg
            # stdout = Popen('python {}'.format(cmdarg), shell=True, stdout=PIPE).stdout
            # output = ast.literal_eval(stdout.read().rstrip())
            tosend = list(cmdarg.split(" "))

            task = tosend[0]
            args = []
            if len(tosend)>1:
                task = tosend[0]
                args = tosend[1:]
            p2 = Process(target = run_process, args=[task,ip,tport])
            p1 = Process(target = connect, args = [s,tsock,args,task])
            p1.start()
            p2.start()
            p1.join()
            p2.join()
