#!/cm/shared/openmind/anaconda/1.9.2/bin/python
from mpi4py import MPI
from subprocess import call

import sys, os.path, cPickle, atexit

jobfilename = sys.argv[1]

comm = MPI.COMM_WORLD
rank = int(os.environ['SLURM_PROCID'])


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
            ret_code = call(cmd, shell=True)
            print("""worker %i completed task %i with return code %i""" % (rank, work_ID, ret_code))
            comm.send(("work_done", rank, work_ID, ret_code, cmd),  dest=0, tag=0)
        else:
            print("worker %i recieved kill request; exiting" % rank)
            exit(0)
else:
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
                    logfile.write(str(key) + str(joblist[1][key][0]) + str(joblist[1][key][2])+"\n")
        atexit.register(saveJobs)
        atexit.register(writeLog)        
        active_workers = 0
        while True:
            if len(joblist[0].keys()) is 0 and active_workers == 0:
                print("Dispatcher asking workers to shut down")
                comm.bcast(False, root=0)
                exit(0)
            print("Dispatcher looking for job requests")
            msg = comm.recv(source = MPI.ANY_SOURCE, tag = 0);
            print("recieved message over MPI: " + str(msg))
            if(isinstance(msg, tuple)) and len(msg) > 0:
                if(msg[0] == "work_request" and len(msg) > 1):
                    active_workers +=1
                    worker_rank = msg[1]
                    #find work
                    work_ID, cmd = getNextJob(joblist)
                    #then send it to the worker
                    print("Dispatcher sending out job: " + str((work_ID, cmd)))
                    comm.send((work_ID, cmd), dest=worker_rank, tag=worker_rank)
                elif(msg[0] == "work_done" and len(msg) > 4):
                    active_workers -=1
                    #write in the log file that the job has been done
                    _, rank, work_ID, ret_code, cmd = msg
                    print("dispatcher received work done notification for job: " + str(msg))
                    joblist[1][work_ID] = (ret_code, rank, cmd)
                        
            









