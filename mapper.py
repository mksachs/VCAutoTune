#!/usr/bin/env python

import sys

events_by_section = {}

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the event data
    e_data = line.split('\t')
    try:
        dt = e_data[0]
        st = e_data[1]
        year = float(e_data[2])
        mag = float(e_data[3])
        sections = e_data[4::]
        
        for section in sections:
            try:
                events_by_section['{section}_{dt}_{st}'.format(dt=dt,st=st,section=section)].append([year,mag])
            except KeyError:
                events_by_section['{section}_{dt}_{st}'.format(dt=dt,st=st,section=section)] = [[year,mag]]
    except IndexError:
        ef = open('run_errors.txt', 'a')
        ef.write(line)
        ef.close()




for key in events_by_section.iterkeys():
    intervals = [
                x[0] - events_by_section[key][n-1][0]
                for n,x in enumerate(events_by_section[key])
                if n != 0]
    print '{key}\t{intervals}'.format(key=key, intervals=' '.join([str(x) for x in intervals]))

    # increase counters
    #for word in words:
        # write the results to STDOUT (standard output);
        # what we output here will be the input for the
        # Reduce step, i.e. the input for reducer.py
        #
        # tab-delimited; the trivial word count is 1
        #print '%s\t%s' % (word, 1)