from datetime import datetime, timedelta
from util import HiveUtils, HdfsDatasetUtils, diff_datewise, timestamps_to_now, interval_hierarchies
from unittest import TestCase


class TestUtil(TestCase):
    def test_diff_datewise(self):
        l = []
        l_just_dates = []
        r = []
        lp = 'blah%Y...%m...%d...%Hblahblah'
        rp = 'neenee%Y%m%d%Hneenee'
        
        expect0 = set([datetime(2012, 6, 14, 13), datetime(2012, 11, 9, 3)])
        expect1 = set([datetime(2012, 6, 14, 14), datetime(2013, 11, 10, 22)])
        
        for y in range(2012, 2014):
            for m in range(1, 13):
                # we're just diffing so we don't care about getting all days
                for d in range(1, 28):
                    for h in range(0, 24):
                        x = datetime(y, m, d, h)
                        if not x in expect1:
                            l.append(datetime.strftime(x, lp))
                            l_just_dates.append(x)
                        if not x in expect0:
                            r.append(datetime.strftime(x, rp))
        
        result = diff_datewise(l, r, left_parse=lp, right_parse=rp)
        self.assertEqual(result[0], expect0)
        self.assertEqual(result[1], expect1)
        
        result = diff_datewise(l_just_dates, r, right_parse=rp)
        self.assertEqual(result[0], expect0)
        self.assertEqual(result[1], expect1)
    
    def test_timestamps_to_now(self):
        now = datetime.now()
        start = now - timedelta(hours=2)
        expect = [
            start,
            start + timedelta(hours=1),
            start + timedelta(hours=2),
        ]
        timestamps = timestamps_to_now(start, timedelta(hours=1))
        self.assertEqual(expect, list(timestamps))


class TestHiveUtil(TestCase):
    def setUp(self):
        self.hive = HiveUtils()
        self.hive.tables = {
            'table1': {
                'partitions': ['year=2013/month=10/day=01/hour=01', 'year=2013/month=10/day=01/hour=02'],
                'interval':   'hourly',
                'depth':      4,
                'location':   '/path/to/data/table1/hourly',
            },
            'table2': {
                'partitions': ['year=2013/month=10/day=01'],
                'interval':   'daily',
                'depth':      3,
                'location':   '/path/to/data/table2/daily',
            },
        }

    def test_table_exists(self):
        self.assertTrue(self.hive.table_exists('table1'))
        self.assertFalse(self.hive.table_exists('nonya'))

    def test_partition_ddl(self):
        d = datetime(2013,10,15,10)
        for table in self.hive.tables.keys():

            interval = self.hive.tables[table]['interval']
            expect = 'PARTITION (%s) LOCATION \'%s/%s\'' % (
                d.strftime(interval_hierarchies[interval]['hive_partition_ddl_format']),
                self.hive.tables[table]['location'],
                d.strftime(interval_hierarchies[interval]['directory_format'])
            )
            ddl = self.hive.partition_ddl(table, d)
            self.assertEqual(ddl, expect)

    def test_add_partitions_ddl(self):
        table = 'table1'
        interval = 'hourly'
        ddl_format = interval_hierarchies[interval]['hive_partition_ddl_format']
        ds = [datetime(2013,10,15,10), datetime(2013,10,15,11)]
        expect = "ALTER TABLE table1 ADD\nPARTITION (%s),\nPARTITION (%s);" % (ds[0].strftime(ddl_format), ds[1].strftime(ddl_format))

        expect_ddls = ['PARTITION (%s) LOCATION \'%s/%s\'' % (
            d.strftime(interval_hierarchies[interval]['hive_partition_ddl_format']),
            self.hive.tables[table]['location'],
            d.strftime(interval_hierarchies[interval]['directory_format'])
        ) for d in ds]

        expect = 'ALTER TABLE ' + table + ' ADD\n' + '\n'.join(expect_ddls) + ';'

        add_statement = self.hive.add_partitions_ddl(table, ds)
        self.assertEqual(add_statement, expect)

    def test_create_partitions(self):
        table = 'table1'
        ddl_format = interval_hierarchies['hourly']['hive_partition_ddl_format']
        ds = [datetime(2013,10,15,10), datetime(2013,10,15,11)]
        expect = "ALTER TABLE table1 ADD\nPARTITION (%s)\nPARTITION (%s);" % (ds[0].strftime(ddl_format), ds[1].strftime(ddl_format))

    def test_partition_interval(self):
        for t in self.hive.tables.keys():
            expect = self.hive.tables[t]['interval']
            self.assertEqual(self.hive.partition_interval(t), expect)

    def table_location(self):
        self.assertEqual(self.hive.table_location('table1'), self.hive.tables['table1']['location'])

    def test_partition_location(self):
        interval = 'hourly'
        d = datetime(2013,10,15,10)
        expect = self.hive.table_location('table1') + '/' + d.strftime(interval_hierarchies[interval]['directory_format'])
        self.assertEqual(self.hive.partition_location('table1', d), expect)


class TestHdfsDatasetUtils(TestCase):
    def setUp(self):
        self.hdfs = HdfsDatasetUtils('/path/to/data')

    def test_dataset_dir(self):
        self.assertEqual(self.hdfs.dataset_dir('nonya',   'hourly'), '/path/to/data/nonya/hourly')
        self.assertEqual(self.hdfs.dataset_dir('boogers', 'yearly'), '/path/to/data/boogers/yearly')

