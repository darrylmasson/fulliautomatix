from subprocess import Popen, TimeoutExpired
import os
import logging
import re
import time
from numpy import clip

import config
import SubFile


class Daemon:
    '''Daemon base class
    '''
    ssh_command = 'ssh {username}@{host} {command}'
    def __init__(self, db, dry=False):
        self.logger = logging.getLogger(__name__)
        self.dry = dry
        self.db = db
        self.cur = self.db.cursor()

    def __del__(self):
        #self.db.close()  # we don't open the databse here, so we shouldn't close it here
        self.cur.close()

    def DoOneRun(self, run):
        raise NotImplementedError()

    def CheckIfDoing(self, run):
        raise NotImplementedError()

    def MakeCall(self, command, timeout=15):
        proc = Popen(command, **config.popen_args)
        try:
            out, err = proc.communicate(timeout=timeout)
        except TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
        return out, err

class TransferDaemon(Daemon):
    '''Daemon to move files from daq to cluster. If it finds a "transferring" run it checks to see how long it's actually been running
    '''
    rsync_command = 'rsync --quiet --remove-source-files --archive --compress --whole-file {source} {dest}'

    def DoOneRun(self, run):
        if self.IsDAQLive():
            return

        name = run
        transfer_command = self.ssh_command.format(
            username = config.whoami,
            host = config.daq_fqdn,
            command = self.rsync_command.format(source=os.path.join(config.raw_data_daq, name),
                                                dest=config.raw_data_cluster))
        rm_command = self.ssh_command.format(
            username = config.whoami,
            host = config.daq_fqdn,
            command = 'rm -rf %s' % os.path.join(config.raw_data_daq, name))
        if self.dry:
            print(transfer_command)
            print(rm_command)
        else:
            self.logger.info('transferring %s' % name)
            self.cur.execute('UPDATE runs SET raw_status=? WHERE name==?;', ('transferring', name))
            self.cur.execute('INSERT INTO logs (name,log) VALUES (?,?);', (name, 'transfer at %i | ' % time.time()))
            self.db.commit()
            self.logger.debug(transfer_command)
            _, err = self.MakeCall(transfer_command, timeout=config.max_transfer_time)
            if len(err):
                self.logger.error('rsync error on %s: %s' % (name, err.decode()))
                self.cur.execute('UPDATE runs SET raw_status=? WHERE name==?;', ('acquired', name))
                self.db.commit()
                return -1
            else:
                self.logger.info('%s tranfser completed, removing' % name)
                self.cur.execute('UPDATE runs SET raw_status=?,raw_location=? WHERE name==?;', ('ondeck', 'depot', name))
                self.db.commit()
                self.logger.debug(rm_command)
                _, err = self.MakeCall(rm_command)
                if len(err):
                    self.logger.error('Error removing %s: %s' % (name, err.decode()))

        return 0

    def IsDAQLive(self):
        command = self.ssh_command.format(username=config.whoami,
                                          host=config.daq_fqdn,
                                          command = 'top -b -n 1 | grep {daq}'.format(daq=config.daq_software))
        out, err = self.MakeCall(command)
        if len(err):
            self.logger.error('Could not check daq status: %s' % err.decode())
            return -1
        elif len(out):
            self.logger.info('DAQ is currently live, not transferring anything')
            return -1
        return 0

    def CheckIfDoing(self, run):
        name = run
        pattern = r'transfer at (?P<then>[0-9]{10})'
        for row in self.cur.execute('SELECT log FROM logs WHERE name==?;', (name,)):
            m = re.search(pattern, row['log'])
            if m is None:
                self.logger.error('%s isn\'t transferring?? Log: %s' % (name,row['log']))
                return -1
            if time.time() - int(m.group('then')) >= config.max_transfer_time:
                return 1  # redo
        return 0

class ProcessDaemon(Daemon):
    '''Daemon to drop jobs into the cluster for processing
    '''
    qsub_command = 'qsub {subfile}'

    def ProcessTime(self, events, source):
        fudge_factor = 1.2
        rate = 360*60 if source == 'LED' else 40*60 # ev/min
        minutes_min, minutes_max = 3, 60*336
        minutes = clip(events/rate*fudge_factor, minutes_min, minutes_max)
        return '%02i:%02i:00' % (minutes/60, minutes % 60)

    def DoOneRun(self, run):
        info = run
        filename = os.path.join(config.sub_directory, info['name'] + '.sub')
        walltime_str = self.ProcessTime(info['events'], info['source'])
        queue_command = self.ssh_command.format(
            username = config.whoami,
            host = config.cluster_fqdn.format(cluster=config.cluster),
            command = self.qsub_command.format(subfile=filename)
        )
        sub_file = SubFile.ProcessJob.format(
                    name = info['name'],
                    walltime = walltime_str,
                    queue = 'darkmatter',
                    nodecount = 'nodes=1:ppn=1',
                    nodeaccess = 'shared',
                    config = 'ASTERIX_LED' if info['source'] == 'LED' else 'ASTERIX',
                    raw_data = '%s' % os.path.join(config.raw_data_cluster, info['name']),
                    processed = '%s' % os.path.join(config.processed_directory, info['name']),
                )
        if self.dry:
            print(sub_file)
            print(queue_command)
        else:
            self.logger.debug('Making subfile %s' % filename)
            with open(filename, 'w') as f:
                f.write(sub_file)
            self.logger.info('queueing %s' % info['name'])
            out, err = self.MakeCall(queue_command)
            if len(err):
                self.logger.error('Error queueing %s: %s' % (info['name'], err.decode()))
                return -1
            else:
                self.cur.execute('UPDATE runs SET processed_status=? WHERE name==?;', ('queueing', info['name']))
                self.db.commit()

        return 0

    def CheckIfDoing(self, run):
        pattern = r'processing at (?P<when>[0-9]{10}) on (?P<where>[a-z]+) by (?P<who>[a-z]+)'
        for row in self.cur.execute('SELECT log FROM logs WHERE name==?;', (run['name'],)):
            m = re.search(pattern, row['log'])
            if m is None:
                self.logger.error('%s isn\'t processing?? Log: %s' % (run['name'],row['log']))
                return -1
            command = self.ssh_command.format(
                username = config.whoami,
                host = m.group('where'),
                command = 'qstat -u %s | grep %s' % (m.group('who'), run['name']))
            self.log.debug(command)
            out, err = self.MakeCall(command)
            if len(err):
                self.logger.error('Error checking %s: %s' % (run['name'], err.decode()))
                return -1
            if len(out):
                self.logger.info('%s still processing' % run['name'])
                return 1
            else:
                return 0
        return 0
