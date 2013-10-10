import logger
from datetime import datetime


logger = logging.getLogger('kraken-etl-util')
interval_hierarchies = {
    'hourly':  {
        'depth': 4,
        'directory_format': '%Y/%m/%d/%H',
        'hive_partition_format': 'year=%Y/month=%m/day=%d/hour=%H'
    },
    'daily':   {
        'depth': 3,
        'directory_format': '%Y/%m/%d',
        'hive_partition_format': 'year=%Y/month=%m/day=%d'
    },
    'monthly': {
        'depth': 2,
        'directory_format': '%Y/%m'
        'hive_partition_format': 'year=%Y/month=%m'
    },
    'yearly':  {
        'depth': 1,
        'directory_format': '%Y'
        'hive_partition_format': 'year=%Y'
    },
}


def diffDatewise(left, right, leftParse=None, rightParse=None):
    """
    Parameters
        left        : a list of datetime strings or objects
        right       : a list of datetime strings or objects
        leftParse   : None if left contains datetimes, or strptime format
        rightParse  : None if right contains datetimes, or strptime format

    Returns
        A tuple of two sets:
        [0] : the datetime objects in left but not right
        [1] : the datetime objects in right but not left
    """
    
    if leftParse:
        leftSet = set([
            datetime.strptime(l.strip(), leftParse)
            for l in left if len(l.strip())
        ])
    else:
        leftSet = set(left)
    
    if rightParse:
        rightSet = set([
            datetime.strptime(r.strip(), rightParse)
            for r in right if len(r.strip())
        ])
    else:
        rightSet = set(right)
    
    return (leftSet - rightSet, rightSet - leftSet)


def timestampsToNow(start, increment):
    """
    Generates timestamps from @start to datetime.now(), by @increment
    
    Parameters
        start       : the first generated timestamp
        increment   : the timedelta between the generated timestamps
    
    Returns
        A generator that goes from @start to datetime.now() - x,
        where x <= @increment
    """
    now = datetime.now()
    while start < now:
        yield start
        start += increment


def sh(command):
    """
    Execute a shell command and return the result
    """
    logger.debug('Running: {0}'.format(command))
    return os.popen(command).read().strip()


def call(command, check_return_code=True):
    """
    Execute a shell command and return the result
    """
    logger.debug('Running: {0}'.format(' '.join(command)))
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    if check_return_code and p.returncode != 0:
        raise RuntimeError("Command: {0} failed with error code: {1}".format(" ".join(command), p.returncode),
                               stdout, stderr)
    return stdout.strip()


class HiveUtils(object):
    def __init__(self, database='default', options=''):
        self.database   = database
        self.options    = options.split()
        self.hivecmd    = None
        # list of partitions for table, keyed by table
        # self.table_partitions = {}
        self.hivecmd = ['hive'] + self.options + ['cli', '--database', self.database]
        # initialize tables dict with table names
        self.tables  = {}
        # cache results for later
        for t in self.tables_get():
            self.tables[t] = {}

    def partitions(self, table):
        """Returns a list of partitions for the given Hive table."""

        # cache results for later
        # if we don't know the partitions yet, get them now
        if not 'partitions' in self.tables[table].keys():
            stdout     = self.query("SHOW PARTITIONS %s;" % table)
            partitions = stdout.split('\n')
            partitions.remove('partition')
            self.tables[table]['partitions'] = partitions

        return self.tables[table]['partitions']

    def table_exists(self, table): # ,force=False
        return table in self.tables.keys()

    def tables_get(self):
        t = self.query('SHOW TABLES').split('\n')
        t.remove('tab_name')
        return t

    def partition_interval(self, table):
        intervals = {
            4: 'hourly',
            3: 'daily',
            2: 'monhtly',
            1: 'yearly',
        }

        # cache results for later
        if not 'interval' in self.tables[table].keys():
            # counts the number of partition keys this table has
            # and returns a string time interval based in this number        
            cmd = ' '.join(self.hivecmd) + ' -e \'SHOW CREATE TABLE ' + table + ';\' | sed -n \'/PARTITIONED BY (/,/)/p\' | grep -v \'PARTITIONED BY\' | wc -l'
            logger.debug('Running: %s' % cmd)
            # using check_output directly here so that we can pass shell=True and use pipes.
            partition_depth = int(subprocess.check_output(cmd, stderr=subprocess.PIPE, shell=True).strip())
            self.tables[table]['depth']    = partition_depth
            self.tables[table]['interval'] = intervals[partition_depth]

        return self.tables[table]['interval']

    def query(self, query, check_return_code=True):
        """Runs the given hive query and returns stdout"""
        return self.command(['-e', query], check_return_code)

    def script(self, script, check_return_code=True):
        """Runs the contents of the given script in hive and returns stdout"""
        if not os.path.isfile(script):
            raise RuntimeError("Hive script: {0} does not exist.".format(script))
        return self.command( ['-f', script], check_return_code)

    def command(self, args, check_return_code=True):
        """Runs the `hive` from the command line, passing in the given args, and
           returning stdout.
        """
        cmd = self.hivecmd + args
        return call(cmd, check_return_code)


class CamusUtils(object):
    """
    Deal with topics and partitions created by Camus
    """
    def __init__(self, destination_path):
        self.destination_path = destination_path

    def topics(self):
        """Reads topic names out of destination_path"""
        return sh(
            'hdfs dfs -ls {0}/ | '\
            'grep -v "Found .* items" | '\
            'awk -F "/" \'{{print $NF}}\''.format(
                self.destination_path
            )
        ).split('\n')

    def basedir(self, topic, interval='hourly'):
        return self.destination_path + '/' + topic + '/' + interval

    # TODO configure interval partition paths automatically instead of hardcoding
    def partitions(self, topic, interval='hourly'):
        """
        Returns a list of time bucketd 'partitions' created by Camus import.
        These are inferred from directories in hdfs.
        """

        basedir = self.basedir(topic, interval)

        depth = interval_hierarchies[interval]['depth']
        # number of  wildcard time bucket directories, e.g. depth_stars == 4 -> '/*/*/*/*'
        depth_stars = '/*' * depth
        directories = sh(
            'hdfs dfs -ls -d {0}{1} | '\
            'grep -v \'Found .* items\' | '\
            'awk \'{{print $NF}}\''.format(basedir, depth_stars)
        ).split('\n')
        # return list of time bucket directories, e.g. ['2013/10/09/15', '2013/10/09/16', ... ]
        return [directory.replace(basedir + '/', '') for directory in directories]

        # return sh('hdfs dfs -ls -R {0}/{1}/{2} | sed "s@.*{0}/{1}/{2}/\([0-9]*\)/\([0-9]*\)/\([0-9]*\)/\([0-9]*\)/.*@year=\1, month=\2, day=\3, hour=\4@" | grep "year.*" | sort | uniq'.format(self.destination_path, topic, interval)).split()
        # return a list of lists of time buckets, e.g. [['2013']]
        # return [directory.split('/')[-depth:] for directory in directories]    
        # return [(int(d) for d in directory.split('/')[-depth:]) for directory in directories]
