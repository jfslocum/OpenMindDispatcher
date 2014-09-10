#!/cm/shared/openmind/anaconda/1.9.2/bin/python
from mpi4py import MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

if rank == 0:
    data = {'a': 7, 'b': 3.14}
    print("rank 0 sending data")
    comm.send(data, dest=1, tag=11)
    print ("data sent")
elif rank == 1:
    print("rank 1 receiving data")
    data = comm.recv(source=0, tag=11)
    print("received data: " + str(data))
