import operator
import sys
import heapq

current_word = None
current_count = 0
word = None
reducer_dict = {}
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    if(line==''):
        break
    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
    
    reducer_dict[word] = reducer_dict.get(word,0) + 1

k_keys_sorted = heapq.nlargest(100, reducer_dict,key=reducer_dict.get)
for k in k_keys_sorted:
	print(k,reducer_dict[k])