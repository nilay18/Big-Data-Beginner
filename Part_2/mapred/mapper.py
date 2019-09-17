import sys
import itertools
import operator

map_list = []

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    # increase counters
    for word in words:
        #Create tuples with first parameter as a key and second parameter as a value (default=1)
        if(len(word)>3):
                map_list.append((word, 1))
#combine tuples with same first parameter and stream the output to the reducer
for group in itertools.groupby(map_list, operator.itemgetter(0)):
        print(group[0],'\t',len(list(map(operator.itemgetter(1), group[1]))))