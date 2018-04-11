#!/usr/bin/env python
"""
NAME: {{cookiecutter.scriptname}}
=========

DESCRIPTION
===========

INSTALLATION
============

USAGE
=====

VERSION HISTORY
===============

{{cookiecutter.version}}    {{cookiecutter.date}}    Initial version.

LICENCE
=======
{{cookiecutter.date}}, copyright {{cookiecutter.author_name}}, ({{cookiecutter.author_email}}), {{cookiecutter.author_www}}

template version: 1.9 (2017/12/08)
"""
from timeit import default_timer as timer
from multiprocessing import Pool
from signal import signal, SIGPIPE, SIG_DFL
import sys
import os
import os.path
import argparse
import csv
import collections
import gzip
import bz2
import zipfile
import time

# When piping stdout into head python raises an exception
# Ignore SIG_PIPE and don't throw exceptions on it...
# (http://docs.python.org/library/signal.html)
signal(SIGPIPE, SIG_DFL)

__version__ = '{{cookiecutter.version}}'
__date__ = '{{cookiecutter.date}}'
__email__ = '{{cookiecutter.author_email}}'
__author__ = '{{cookiecutter.author_name}}'

# For color handling on the shell
try:
    from colorama import init, Fore, Style
    # INIT color
    # Initialise colours for multi-platform support.
    init()
    reset=Fore.RESET
    colors = {'success': Fore.GREEN, 'error': Fore.RED, 'warning': Fore.YELLOW, 'info':''}
except ImportError:
    sys.stderr.write('colorama lib desirable. Install with "conda install colorama".\n\n')
    reset=''
    colors = {'success': '', 'error': '', 'warning': '', 'info':''}


def alert(atype, text, log):
    textout = '%s [%s] %s\n' % (time.strftime('%Y%m%d-%H:%M:%S'),
                                atype.rjust(7),
                                text)
    log.write('%s%s%s' % (colors[atype], textout, reset))
    if atype=='error': sys.exit()


def success(text, log=sys.stderr):
    alert('success', text, log)
    

def error(text, log=sys.stderr):
    alert('error', text, log)
    

def warning(text, log=sys.stderr):
    alert('warning', text, log)
    

def info(text, log=sys.stderr):
    alert('info', text, log)  


def parse_cmdline():
    """ Parse command-line args. """
    ## parse cmd-line -----------------------------------------------------------
    description = 'Read files and process them using multiple cores.'
    version = 'version %s, date %s' % (__version__, __date__)
    epilog = 'Copyright %s (%s)' % (__author__, __email__)

    parser = argparse.ArgumentParser(description=description, epilog=epilog)

    parser.add_argument('--version',
                        action='version',
                        version='%s' % (version))

    parser.add_argument(
        'files',
        metavar='FILE',
        nargs='+',
        help=
        'File to process.')

    parser.add_argument('-o',
                        '--out',
                        metavar='STRING',
                        dest='outfile_name',
                        default=None,
                        help='Out-file. [default: "stdout"]')

    group1 = parser.add_argument_group('Input file(s)',
                                       'Arguments:')
    group1.add_argument('-a',
                        '--header',
                        dest='header_exists',
                        action='store_true',
                        default=False,
                        help='Header in files. [default: False]')
    group1.add_argument('-d',
                        '--delimiter',
                        metavar='STRING',
                        dest='delimiter_str',
                        default='\t',
                        help='Delimiter used in files.  [default: "tab"]')
    group1.add_argument('-f',
                        '--field',
                        metavar='INT',
                        type=int,
                        dest='field_number',
                        default=1,
                        help='Field / Column in file to use in files. [default: 1]')
   

    group2 = parser.add_argument_group('Threading',
                                       'Multithreading arguments:')

    group2.add_argument(
        '-p',
        '--processes',
        metavar='INT',
        type=int,
        dest='process_number',
        default=1,
        help=
        'Number of sub-processes (workers) to use.'+\
        ' It is only logical to not give more processes'+\
        ' than cpus/cores are available. [default: 1]')
    group2.add_argument(
        '-t',
        '--time',
        action='store_true',
        dest='show_runtime',
        default=False,
        help='Time the runtime and print to stderr. [default: False]')

   
    # if no arguments supplied print help
    if len(sys.argv)==1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()
    return args, parser


def load_file(filename):
    """ LOADING FILES """
    if filename in ['-', 'stdin']:
        filehandle = sys.stdin
    elif filename.split('.')[-1] == 'gz':
        filehandle = gzip.open(filename, 'rt')
    elif filename.split('.')[-1] == 'bz2':
        filehandle = bz2.open(filename, 'rt')
    elif filename.split('.')[-1] == 'zip':
        filehandle = zipfile.ZipFile(filename)
    else:
        filehandle = open(filename)
    return filehandle


def my_func(args):
    """
    THIS IS THE ACTUAL WORKFUNCTION THAT HAS TO BE EXECUTED MULTIPLE TIMES.
    This funion will be distributed to the cores requested.
    # do stuff
    res = ...
    return (args, res)
    """
    file = args[0]
    header_bool = args[1]
    delimiter = args[2]
    field = args[3]

    fileobj = load_file(file)
    csv_reader_obj = csv.reader(fileobj, delimiter=delimiter)
    if header_bool:
        header = next(csv_reader_obj)

    res = []
    for a in csv_reader_obj:
        x = a[field]
        res.append(x)

    fileobj.close()
    return (args, res)


def main():
    """ The main funtion. """
    args, parser = parse_cmdline()

    # get field number to use in infile
    field_number = args.field_number - 1
    if field_number < 0:
        parser.error('Field -f has to be an integer > 0. EXIT.')

    # create outfile object
    if not args.outfile_name:
        outfileobj = sys.stdout
    elif args.outfile_name in ['-', 'stdout']:
        outfileobj = sys.stdout
    elif args.outfile_name.split('.')[-1] == 'gz':
        outfileobj = gzip.open(args.outfile_name, 'wt')
    else:
        outfileobj = open(args.outfile_name, 'w')

    # ------------------------------------------------------
    #  THREADING
    # ------------------------------------------------------
    # get number of subprocesses to use
    process_number = args.process_number
    if process_number < 1:
        parser.error('-p has to be > 0: EXIT.')

    # FILL ARRAY WITH PARAMETER SETS TO PROCESS
    #
    # this array contains the total amount of jobs : here 1 file = 1 job
    job_list = []
    for f in args.files:
        job_list.append((f, args.header_exists, args.delimiter_str, field_number))

    # For timing
    start_time = timer()  # very crude
    # create pool of workers ---------------------
    pool = Pool(processes=process_number)

    # "chunksize"" usually only makes a noticeable performance
    # difference for very large iterables
    # Here I set it to one to get the progress bar working nicely
    # Otherwise it will not give me the correct number of processes left
    # but chunksize number.
    chunksize = 1

    result_list = pool.map_async(my_func, job_list, chunksize=chunksize)
    pool.close()  # No more work

    jobs_total = len(job_list)
    # Progress bar
    #==============================
    # This can be changed to make progressbar bigger or smaller
    progress_bar_length = 60
    #==============================
    while not result_list.ready():
        num_not_done = result_list._number_left
        num_done = jobs_total - num_not_done
        num_bar_done = int(num_done * progress_bar_length / jobs_total)
        bar_str = ('=' * num_bar_done).ljust(progress_bar_length) 
        percent = int(num_done * 100 / jobs_total)
        sys.stderr.write("JOBS (%s): [%s] (%s) %s%%\r" % (str(num_not_done).rjust(len(str(jobs_total))),
                                            bar_str,
                                            str(num_done).rjust(len(str(jobs_total))),
                                            str(percent).rjust(3)))
 
        sys.stderr.flush()
        time.sleep(0.1)  # wait a bit: here we test all .1 secs if jobs done
    # Finish the progress bar
    bar_str = '=' * progress_bar_length
    sys.stderr.write("JOBS (%s): [%s] (%i) 100%%\n" % ('0'.rjust(len(str(jobs_total))),
                                            bar_str,
                                            jobs_total))
    ## info
    end_time = timer()
    if args.show_runtime:
        info('PROCESS-TIME: %.4f sec' % (end_time - start_time))
    
    result_list = result_list.get()
    # --------------------------------------------
    # NOW DO SOMETHING WITH THE PROCESSED RESULTS PER FILE
    # ...

    ## info
    print_time_start = timer()
    if args.show_runtime:
        info('WRITING-RESULTS...')

        
    # Do stuff with the results
    for job_args, res in result_list:
        print(job_args, res)  # needs adjustment, only for testing
    
        
    ## info
    print_time_stop = timer()
    if args.show_runtime:
       info(' %.4f sec' % (print_time_stop - print_time_start))
    # ------------------------------------------------------
    outfileobj.close()
    return


if __name__ == '__main__':
    sys.exit(main())
