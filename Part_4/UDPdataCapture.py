import socket 
import time
import subprocess

UDP_IP = "127.0.0.1" #IP
UDP_PORT = 9876 #port
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # UDP
sock.bind((UDP_IP, UDP_PORT)) #bind socket with port
counter = 1
start = 0
diff = 60000 #time difference
total = 0
dsum = 0
while True:
    data, addr = sock.recvfrom(1024)
    data_split = str(data).split(',') #split data with comma (,)
    total += 1
    dsum+= int(data_split[15]) #total sum of the dOctet
    if(start == 0):
        filename = str(counter) + ".txt" #file creation
        f = open(filename,"w+")
        start = int(data_split[3])
    f.write(str(data_split[15]+"\n"))
    #file creation logic
    if(start + diff <= (int(data_split[3])) + 1):
        counter= counter + 1
        filename = str(counter) + ".txt"
        f = open(filename,"w+")
        start = int(data_split[3])