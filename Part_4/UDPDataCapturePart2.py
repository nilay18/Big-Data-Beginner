import socket 
import time
import subprocess

cmd = 'submit-spark script.py'
UDP_IP = "127.0.0.1"
UDP_PORT = 9876
sock = socket.socket(socket.AF_INET, # Internet
socket.SOCK_DGRAM) # UDP
sock.bind((UDP_IP, UDP_PORT))
counter = 1
start = 0
diff = 60000
total = 0
dsum = 0
while True:
    data, addr = sock.recvfrom(1024)
    data_split = str(data).split(',') #data split
    total += 1
    if(start == 0):
        filename = str(counter) + ".txt"
        f = open(filename,"w+")
        start = int(data_split[3])
    f.write(str(data_split[9]) + "," + str(data_split[10]) +"\n")
    if(start + diff <= (int(data_split[3])) + 1):
        counter= counter + 1
        #file merging logic for sliding window
        if(counter>2):
            print("inside")
            with open("./merge/"+str(counter-2) + str(counter-1)+".txt", "wb") as outfile:
                for f in (counter-2,counter-1):
                    print(f)
                    with open(str(f)+".txt", "rb") as infile:
                        outfile.write(infile.read())
        filename = str(counter) + ".txt"
        f = open(filename,"w+")
        start = int(data_split[3])