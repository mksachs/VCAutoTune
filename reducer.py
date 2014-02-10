#!/usr/bin/env python

from operator import itemgetter
import sys
import numpy as np
import logging

current_section = None
results = {}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, intervals_text = line.split('\t')
    
    section, dt, st = key.split('_')
    intervals = map(float,intervals_text.split(' '))
    
    mean = np.mean(intervals)
    std = np.std(intervals)
    
    #if current_section == section:
    try:
        results[section]['{dt} {st}'.format(dt=dt,st=st)] = {'mean':mean, 'std':std}
    except KeyError:
        results[section] = {'{dt} {st}'.format(dt=dt,st=st) : {'mean':mean, 'std':std} }

    logging.warn('{}, {}, {}, {}'.format(section, dt, st, intervals_text))
    #else:
    #    if current_section:
    #        pass
        
    #    current_section = section

# do not forget to output the last word if needed!
#if current_word == word:
#    print '%s\t%s' % (current_word, current_count)

for section in results.iterkeys():
    result = []
    for dtst in results[section].iterkeys():
        dt,st = dtst.split()
        result.append('{dt} {st} : {mean} {std}'.format(dt=dt, st=st, mean=results[section][dtst]['mean'], std=results[section][dtst]['std']))
    
    print '{section}\t{result}'.format(section=section, result=' :: '.join(result))