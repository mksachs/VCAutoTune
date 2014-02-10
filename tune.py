#!/usr/bin/env python
import sys
import site
import glob
import os
import h5py
import Queue
import multiprocessing
from optparse import OptionParser
import re
import subprocess

#site.addsitedir('/Users/sachs/Documents/PyVC/')
#from pyvc import *



#-------------------------------------------------------------------------------
# Comprehensive CPU availability check.
# http://stackoverflow.com/questions/1006289/how-to-find-out-the-number-of-cpus-in-python
#-------------------------------------------------------------------------------
def available_cpu_count():
    """ Number of available virtual or physical CPUs on this system, i.e.
    user/real as output by time(1) when called with an optimally scaling
    userspace-only program"""

    # cpuset
    # cpuset may restrict the number of *available* processors
    try:
        m = re.search(r'(?m)^Cpus_allowed:\s*(.*)$',
                      open('/proc/self/status').read())
        if m:
            res = bin(int(m.group(1).replace(',', ''), 16)).count('1')
            if res > 0:
                return res
    except IOError:
        pass

    # Python 2.6+
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except (ImportError, NotImplementedError):
        pass

    # http://code.google.com/p/psutil/
    try:
        import psutil
        return psutil.NUM_CPUS
    except (ImportError, AttributeError):
        pass

    # POSIX
    try:
        res = int(os.sysconf('SC_NPROCESSORS_ONLN'))

        if res > 0:
            return res
    except (AttributeError, ValueError):
        pass

    # Windows
    try:
        res = int(os.environ['NUMBER_OF_PROCESSORS'])

        if res > 0:
            return res
    except (KeyError, ValueError):
        pass

    # jython
    try:
        from java.lang import Runtime
        runtime = Runtime.getRuntime()
        res = runtime.availableProcessors()
        if res > 0:
            return res
    except ImportError:
        pass

    # BSD
    try:
        sysctl = subprocess.Popen(['sysctl', '-n', 'hw.ncpu'],
                                  stdout=subprocess.PIPE)
        scStdout = sysctl.communicate()[0]
        res = int(scStdout)

        if res > 0:
            return res
    except (OSError, ValueError):
        pass

    # Linux
    try:
        res = open('/proc/cpuinfo').read().count('processor\t:')

        if res > 0:
            return res
    except IOError:
        pass

    # Solaris
    try:
        pseudoDevices = os.listdir('/devices/pseudo/')
        res = 0
        for pd in pseudoDevices:
            if re.match(r'^cpuid@[0-9]+$', pd):
                res += 1

        if res > 0:
            return res
    except OSError:
        pass

    # Other UNIXes (heuristic)
    try:
        try:
            dmesg = open('/var/run/dmesg.boot').read()
        except IOError:
            dmesgProcess = subprocess.Popen(['dmesg'], stdout=subprocess.PIPE)
            dmesg = dmesgProcess.communicate()[0]

        res = 0
        while '\ncpu' + str(res) + ':' in dmesg:
            res += 1

        if res > 0:
            return res
    except OSError:
        pass

    raise Exception('Can not determine number of CPUs on this system')

#-------------------------------------------------------------------------------
# A class to handle parallel hdf5 file conversions
#-------------------------------------------------------------------------------
class Hdf5ToText(multiprocessing.Process):
    def __init__(self, work_queue, result_queue, text_data_dir):
        
        # base class initialization
        #multiprocessing.Process.__init__(self)
        super(Hdf5ToText,self).__init__()
 
        # job management stuff
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.kill_received = False
        
        self.text_data_dir = text_data_dir
        #self.field_1d = field_1d
        #self.event_element_data = event_element_data
        #self.event_element_slips = event_element_slips
        #self.lat_size = lat_size
        #self.lon_size = lon_size
        #self.cutoff = cutoff
        #self.event_center = event_center
        #self.event_radius = event_radius
    
        #self.counter = counter
        #self.total_tasks = total_tasks
    
    def run(self):
        while not self.kill_received:
            # get a file
            try:
                h5files = self.work_queue.get_nowait()
            except Queue.Empty:
                break
            
            sys.stdout.write('{} files, '.format(len(h5files)))
            sys.stdout.flush()
            #print 'processing elements {} - {}'.format(start, end)
            
            # parse the files
            for h5file in h5files:
                filename = os.path.basename(h5file).split('.h5')[0]
                textFile = '{}{}.txt'.format(self.text_data_dir,filename)
                bf = h5py.File(h5file,'r')
                tf = open(textFile, 'w')
                
                dt = float('.'.join(filename.split('_dyn-')[-1].split('_')[0].split('-')))
                st = float('.'.join(filename.split('_st-')[-1].split('_')[0].split('-')))
                
                events = bf['event_table']
                geometrty = bf['block_info_table']
                sweeps = bf['event_sweep_table']
                
                for event in events:
                    e_mag = event[3]
                    e_year = event[1]
                    e_blocks = set(sweeps['block_id'][event[8]:event[9]])
                    e_sections = '\t'.join(list(set([str(geometrty['section_id'][bid]) for bid in e_blocks])))
                    tf.write('{}\t{}\t{}\t{}\t{}\n'.format(dt, st, e_year, e_mag, e_sections))
            
                bf.close()
                tf.close()
            
                self.result_queue.put(textFile)

def main(argv=None):
    if argv is None:
        argv = sys.argv
    
    # Get the options from the command line
    parser = OptionParser()
    parser.add_option("-d", "--data-dir", "--dd",
                dest="data_dir", default=None,
                help="The full path to the directory containing HDF5 Virtual California simulation output files.",
                metavar="FILE"
                )
    (options, args) = parser.parse_args()
    data_dir       = options.data_dir

    sys.stdout.write('Creating event text files :: ')
    
    # Clean up the directory paths
    if not data_dir.endswith('/'):
        data_dir += '/'

    # Check if the directory for the event text files exists. These are the
    # files that will be processed by Hadoop.
    text_data_dir = '/'.join(data_dir.split('/')[0:-2])+'/text/'
    if not os.path.isdir(text_data_dir):
        os.makedirs(text_data_dir)

    # Make a list of hdf5 files to process. Only process a file if it hasnt been
    # processed already (ie. no text file exists).
    files_to_process = []
    for h5file in glob.glob('{}*.h5'.format(data_dir)):
        filename = os.path.basename(h5file).split('.h5')[0]
        if not os.path.isfile('{}{}.txt'.format(text_data_dir,filename)):
            files_to_process.append(h5file)

    # How many seperate CPUs do we have
    num_processes = available_cpu_count()
    # How many files to process
    num_files = float(len(files_to_process))

    sys.stdout.write('{} processors : {} files : '.format(num_processes, int(num_files)))

    # Figure out how many segments we will break the task up into
    seg = int(round(num_files/float(num_processes)))
    if seg < 1:
        seg = 1

    # Break up the job.
    segmented_files = []
    if num_files < num_processes:
        segments = int(num_files)
    else:
        segments = int(num_processes)
    
    for i in range(segments):
        if i == num_processes - 1:
            end_index = len(files_to_process)
        else:
            end_index = seg*int(i + 1)
        start_index = int(i) * seg
        if start_index != end_index:
            segmented_files.append(files_to_process[start_index:end_index])

    # Add all of the jobs to a work queue
    work_queue = multiprocessing.Queue()
    for job in segmented_files:
        work_queue.put(job)
    
    # Create a queue to pass to workers to store the results
    result_queue = multiprocessing.Queue()
    
    # Spawn workers
    for i in range(len(segmented_files)):
        #print available_cpu_count()
        worker = Hdf5ToText(work_queue, result_queue, text_data_dir)
        worker.start()
    
    # Collect the results off the queue
    results = []
    for i in range(len(segmented_files)):
        results.append(result_queue.get())

    sys.stdout.write('done\n')

    # Copy the text event files to the Hadoop file system

    sys.stdout.write('Copying files to HFS :: {} files : '.format(len(glob.glob('{}*.txt'.format(text_data_dir)))))

    #subprocess.check_call(['hadoop', 'dfs', '-ls', 'hdfs://localhost:9000//Users/sachs/Hadoop/hadoop-sachs/'])
    #subprocess.check_call(['hadoop', 'dfs', '-copyFromLocal', 'local/text', 'hdfs://localhost:9000//Users/sachs/Hadoop/hadoop-sachs/vc-data'], stderr=subprocess.STDOUT)
    try:
        subprocess.check_call(['hadoop', 'dfs', '-test', '-d',  'hdfs://localhost:9000//Users/sachs/Hadoop/hadoop-sachs/vc-data'], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError:
        subprocess.check_call(['hadoop', 'dfs', '-copyFromLocal', text_data_dir,  'hdfs://localhost:9000//Users/sachs/Hadoop/hadoop-sachs/vc-data'], stderr=subprocess.STDOUT)

    sys.stdout.write('done\n')

    # Run Hadoop on the files

    sys.stdout.write('Running Hadoop :: {} files : '.format(len(glob.glob('{}*.txt'.format(text_data_dir)))))
    
    try:
        subprocess.check_call(['hadoop', 'dfs', '-rmr', 'hdfs://localhost:9000/Users/sachs/Hadoop/hadoop-sachs/vc-output'])
    except subprocess.CalledProcessError:
        pass
    
    ef = open('run_errors.txt', 'w')
    ef.close()
    
    subprocess.check_call(['hadoop', 'jar', '/usr/local/Cellar/hadoop/1.2.1/libexec/contrib/streaming/hadoop-streaming-1.2.1.jar', '-file', 'mapper.py', '-mapper', 'mapper.py', '-file', 'reducer.py', '-reducer', 'reducer.py', '-input', 'hdfs://localhost:9000/Users/sachs/Hadoop/hadoop-sachs/vc-data/*', '-output', 'hdfs://localhost:9000/Users/sachs/Hadoop/hadoop-sachs/vc-output/', '-file', 'run_errors.txt'], stderr=subprocess.STDOUT)

    #for textfile in glob.glob('{}*.txt'.format(text_data_dir)):
    #    filename = os.path.basename(textfile)
    #    subprocess.check_call(['hadoop', 'dfs', '-copyFromLocal', textfile,  'hdfs://localhost:9000//Users/sachs/Hadoop/hadoop-sachs/vc-data/{}'.format(filename)], stderr=subprocess.STDOUT)
        #try:
        #    subprocess.check_call(['hadoop', 'dfs', '-test', '-e',  'hdfs://localhost:9000//Users/sachs/Hadoop/hadoop-sachs/vc-data/{}'.format(filename)], stderr=subprocess.STDOUT)
        #except subprocess.CalledProcessError:
        #    print filename, 'does not exist'

    #sys.stdout.write( out )

    # hadoop dfs -copyFromLocal /tmp/gutenberg /user/hduser/gutenberg

    #print results
    #hadoop jar /usr/local/Cellar/hadoop/1.2.1/libexec/contrib/streaming/hadoop-streaming-1.2.1.jar -file /Users/sachs/Documents/Hadoop/mapper.py -mapper /Users/sachs/Documents/Hadoop/mapper.py -file /Users/sachs/Documents/Hadoop/reducer.py -reducer /Users/sachs/Documents/Hadoop/reducer.py -input hdfs://localhost:9000/Users/sachs/Hadoop/hadoop-sachs/tmp-data/* -output hdfs://localhost:9000/Users/sachs/Hadoop/hadoop-sachs/tmp-output/

if __name__ == "__main__": 
    sys.exit(main())

'''
if not os.path.isfile('local/text/{}.txt'.format(filename)):
    bf = h5py.File(h5file,'r')
    tf = open('local/text/{}.txt'.format(filename), 'w')
    
    events = bf['event_table']
    geometrty = bf['block_info_table']
    sweeps = bf['event_sweep_table']
    
    for event in events:
        e_mag = event[3]
        e_year = event[1]
        e_blocks = set(sweeps['block_id'][event[8]:event[9]])
        e_sections = list(set([str(geometrty['section_id'][bid]) for bid in e_blocks]))
        print e_year, e_mag, ' '.join(e_sections)
'''

#dt = float('.'.join(h5file.split('.h5')[0].split('_dyn-')[-1].split('_')[0].split('-')))
#st = float('.'.join(h5file.split('.h5')[0].split('_st-')[-1].split('_')[0].split('-')))
    
'''
with VCSimData() as sim_data:
    # open the simulation data file
    sim_data.open_file(h5file)
    
    # instantiate the vc classes passing in an instance of the VCSimData
    # class
    events = VCEvents(sim_data)
    geometry = VCGeometry(sim_data)

    event_data = events.get_event_data(['event_range_duration'], event_range=None, magnitude_filter=None, section_filter=None)

    print event_data['event_range_duration']
'''
