import os
import ast
from multiprocessing import Pool, Process, Manager
from subprocess import Popen, PIPE
import socket
import select

s = socket.socket()
port = 10003
s.connect(('127.0.0.1', port))

while True:
    timeout = 0.5  # in seconds
    ready_sockets, _, _ = select.select([s], [], [], timeout)
    if ready_sockets:
        cmdarg = s.recv(1024)
        if len(cmdarg)>0:
            print "--------------------------------------------------------------------"
            print "Received",cmdarg
            stdout = Popen('python {}'.format(cmdarg), shell=True, stdout=PIPE).stdout
            output = ast.literal_eval(stdout.read().rstrip())
            print "Output",output

            tname = cmdarg.split(" ")[0][:-3]
            outstr = ''
            for i in range(len(output)):
                outstr += str(output[i])
                if i!=len(output)-1:
                    outstr+=','
            sendString = tname+' '+outstr
            s.send(sendString)
