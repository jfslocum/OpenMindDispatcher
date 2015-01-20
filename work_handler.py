#!/cm/shared/openmind/anaconda/2.1.0/bin/python

print("Initializing work handler... ")

from subprocess import call
import subprocess
import sys
import os.path
import os
import cPickle
import atexit
from mpi4py import MPI

print("Imports successful")

#"/home/jslocum/OpenMindDispatcher/testing/test.txt"
jobfilename = sys.argv[1]
logfile_path = ''
if(len(sys.argv) > 2):
    logfile_path = sys.argv[2]

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
num_workers = (int(os.environ['SLURM_CPUS_PER_TASK'])* int(os.environ['SLURM_JOB_NUM_NODES'])) - 1

def constructJoblist(jobfile):
    joblist = [{},{}] #incomplete, complete
    jobno = 0;
    for line in jobfile:
        if line[0] == '#':
            continue
        joblist[0][jobno] = line
        jobno = jobno+1
    return joblist


def getNextJob(joblist):
    if(len(joblist[0].keys()) is 0):
        return [9001, 'echo "JOBLIST EMPTY ON DISPATCHER BUT STILL RECIEVED JOB"']
    else:
        return joblist[0].popitem()

def makeLogDir(logfile_path):
    if not os.path.exists(logfile_path):
        os.makedirs(logfile_path)
            
#each worker sends its work requests, and the dispatcher loops with an indiscriminate recieve,
#servicing requests as they arrive

if rank > 0:
    print("worker %i online - asking for jobs" % rank)
    while(True):
        print("worker %i asking for a job" % rank)
        comm.send(("work_request", rank), dest=0, tag=0)
        print("job request for worker %i recieved; now waiting for response" % rank)
        msg = comm.recv(source=0, tag=rank)
        if msg:
            (work_ID, cmd) = msg
            print("""worker %i now executing task `%s`""" % (rank, work_ID))
            ret_code = -1
            if logfile_path:
                fp = os.path.join(logfile_path,'task_%s' % work_ID)
            else:
                fp = jobfilename+('.task_%s' % work_ID)
            with open(fp+'.out', 'w') as output:
                with open(fp+'.err', 'w') as err:
                    ret_code = call(cmd, shell=True, stdout=output, stderr=err)
            print("""worker %i completed task %i with return code %i""" % (rank, work_ID, ret_code))
            comm.send(("work_done", rank, work_ID, ret_code, cmd),  dest=0, tag=0)
        else:
            print("worker %i recieved kill request; exiting" % rank)
            exit(0)
else:
    if num_workers ==0:
        exit(0)
    print("dispatcher now online; comm rank is " + str(comm.Get_rank()))
    with open(jobfilename) as jobfile:
        pcklfilename = jobfilename + ".pckl"
        joblist = []
        if os.path.isfile(pcklfilename):
            with open(pcklfilename, 'rb') as picklefile:
                joblist = cPickle.load(picklefile)
        else:
            #first time this job has run; must construct a new joblist
            joblist = constructJoblist(jobfile)
        print(joblist)
        #register atexit function to save joblist/log
        def saveJobs():
            with open(pcklfilename, 'wb') as picklefile:
                cPickle.dump(joblist, picklefile)
        def writeLog():
            with open(jobfilename+".log", 'wb') as logfile:
                if not(len(joblist)>1 and isinstance(joblist[1], dict)):
                    return
                logfile.write("JOB_ID\tRETVAL\tCMD\n")
                for key in joblist[1].keys():
                    logfile.write(str(key) + '\t' + str(joblist[1][key][0]) + '\t$' + str(joblist[1][key][2])+"\n")
        atexit.register(saveJobs)
        atexit.register(writeLog)        
        workers = {}
        num_workers_killed = 0
        if logfile_path:
            makeLogDir(logfile_path);
        while True:
            print("Dispatcher looking for job requests")
            msg = comm.recv(source = MPI.ANY_SOURCE, tag = 0);
            print("recieved message over MPI: " + str(msg))
            if(isinstance(msg, tuple)) and len(msg) > 0:
                if(msg[0] == "work_request" and len(msg) > 1):
                    worker_rank = msg[1]
                    workers[worker_rank]=True
                    #find work
                    if(len(joblist[0].keys()) == 0):
                        print("No jobs left: asking worker to terminate")
                        comm.send(False, dest=msg[1], tag=msg[1])
                        num_workers_killed +=1
                        if(num_workers_killed == num_workers):
                            print("No workers left: terminating")
                            exit(0)
                    work_ID, cmd = getNextJob(joblist)
                    #then send it to the worker
                    print("Dispatcher sending out job: " + str((work_ID, cmd)))
                    comm.send((work_ID, cmd), dest=worker_rank, tag=worker_rank)
                elif(msg[0] == "work_done" and len(msg) > 4):
                    #write in the log file that the job has been done
                    _, rank, work_ID, ret_code, cmd = msg
                    print("dispatcher received work done notification for job: " + str(msg))
                    joblist[1][work_ID] = (ret_code, rank, cmd)
