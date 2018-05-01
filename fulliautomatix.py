#!/depot/darkmatter/etc/conda/env/asterix/bin/python3

import os
import argparse
import logging
import datetime
import sqlite3 as sql
import time
import datetime

import config
import Daemons

parser = argparse.ArgumentParser(description='Fulliautomatix')
parser.add_argument('--transfer', action='store_true', help='Transfer data from daq to cluster')
parser.add_argument('--process', action='store_true', help='Process data')
parser.add_argument('--log', default='info', type=str, choices=['debug','info','warning','error','critical'], help='Logging level, default \'info\'')
parser.add_argument('--run', nargs='+', help='Specific runs (name or number) to handle. Space-separated list')
parser.add_argument('--dry_run', action='store_true', help='Dry run (don\'t actually transfer or process anything)')
parser.add_argument('--version', action='store_true', help='Output version and return')

args = parser.parse_args()
start_time = time.time()
max_runtime = 3000

_version = 1.1

def main():
    print('Welcome to Fulliautomatix!')
    if args.version:
        print('Version %s' % _version)
        return
    print('It is currently %s' % datetime.datetime.now().isoformat(sep=' '))
    if not os.path.exists(config.runs_db_address):
        print('No runs database??')
        return
    db = sql.connect(config.runs_db_address, isolation_level=None)
    db.row_factory = sql.Row
    cur = db.cursor()
    logging.basicConfig(filename=os.path.join(config.log_directory,
                                              datetime.date.today().isoformat() + '.log'),
                        level=getattr(logging, args.log.upper()),
                        format='%(asctime)s | %(module)s | %(levelname)s | %(message)s')
    transfer_daemon = Daemons.TransferDaemon(db=db,dry=args.dry_run)
    process_daemon = Daemons.ProcessDaemon(db=db,dry=args.dry_run)

    if args.transfer:
        print('Transferring runs...')
        if args.run:
            for run in args.run:
                if '_' not in run:
                    print('I only deal with run names, what is %s?' % run)
                for row in cur.execute('SELECT raw_status,raw_location FROM runs WHERE name==?', (run,)):
                    if 'zinc' not in row['raw_location']:
                        print('%s isn\'t on zinc, what am I supposed to do with it?' % run)
                    else:
                        transfer_daemon.DoOneRun(run)
        else:
            for row in cur.execute('SELECT name FROM runs WHERE raw_status=?;', ('transferring',)):  # check transfers in progress
                logging.debug('Checking %s' % row['name'])
                if transfer_daemon.CheckIfDoing(row['name']):
                    cur.execute('UPDATE runs SET raw_status=? WHERE name=?;', ('acquired',row['name']))
                    db.commit()

            num_runs = 0
            select = 'name'
            requirements = 'raw_location==? AND raw_status==?'
            values = ('zinc','acquired')
            for row in cur.execute('SELECT count(*) FROM runs WHERE %s' % requirements, values): # transfer new stuff
                num_runs = int(row[0])
            print('Found %i runs to transfer' % num_runs);
            while num_runs > 0 and time.time()-start_time < max_runtime:
                for row in cur.execute('SELECT %s FROM runs WHERE %s' % (select, requirements), values):
                    transfer_daemon.DoOneRun(row['name'])
                    if transfer_daemon.dry:
                        break
                if transfer_daemon.dry:
                    num_runs = 0
                else:
                    for row in db.execute('SELECT count(*) FROM runs WHERE %s' % requirements, values):
                        num_runs = int(row[0])
    del transfer_daemon

    if args.process:
        print('Processing runs...')
        if args.run:
            for run in args.run:
                if '_' not in run:
                    print('I only deal with run names, what is %s?' % run)
                for row in cur.execute('SELECT raw_status,processed_status,name,events,source FROM runs WHERE name==?', (run,)):
                    print(row['name'])
                    if row['raw_status'] != 'ondeck':
                        print('%s isn\'t on deck, I can\'t process it' % run)
                    elif row['processed_status'] == 'queueing' and process_daemon.CheckIfDoing(row):
                        print('%s is currently queueing' % run)
                    elif row['processed_status'] == 'processing' and process_daemon.CheckIfDoing(row):
                        print('%s is currently processing' % run)
                    else:
                        process_daemon.DoOneRun({'name' : row['name'],'events' : int(row['events']), 'source' : row['source']})
        else:
            for row in cur.execute('SELECT name FROM runs WHERE processed_status=?;', ('processing',)):
                logging.debug('Checking %s' % row['name'])
                if process_daemon.CheckIfDoing(row['name']):
                    cur.execute('UPDATE runs SET processed_status=? WHERE name=?;', ('none',row['name']))
                    db.commit()

            num_runs = 0
            select = 'name,events,source'
            requirements = 'raw_location==? AND raw_status==? AND processed_status==?'
            values=('depot','ondeck','none')
            for row in cur.execute('SELECT count(*) FROM runs WHERE %s' % requirements, values):
                num_runs = int(row[0])
            print('Found %i runs to process' % num_runs)
            while num_runs > 0 and time.time()-start_time < max_runtime:
                for row in cur.execute('SELECT %s FROM runs WHERE %s;' % (select, requirements), values):
                    d = {'name' : row['name'],
                        'events' : int(row['events']),
                        'source' : row['source'],
                        }
                    process_daemon.DoOneRun(d)
                    if process_daemon.dry:
                        break
                if process_daemon.dry:
                    num_runs = 0
                else:
                    for row in cur.execute('SELECT count(*) FROM runs WHERE %s;' % requirements, values):
                        num_runs = int(row[0])
    del process_daemon

    cur.close()
    db.close()

if __name__ == '__main__':
    main()