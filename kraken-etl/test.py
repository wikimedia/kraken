from datetime import datetime, timedelta
from util import diffDatewise, timestampsToNow
from unittest import TestCase


class TestUtil(TestCase):
    def testDiffDatewise(self):
        l = []
        lJustDates = []
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
                            lJustDates.append(x)
                        if not x in expect0:
                            r.append(datetime.strftime(x, rp))
        
        result = diffDatewise(l, r, leftParse=lp, rightParse=rp)
        self.assertEqual(result[0], expect0)
        self.assertEqual(result[1], expect1)
        
        result = diffDatewise(lJustDates, r, rightParse=rp)
        self.assertEqual(result[0], expect0)
        self.assertEqual(result[1], expect1)
    
    def testTimestampsToNow(self):
        now = datetime.now()
        start = now - timedelta(hours=2)
        expect = [
            start,
            start + timedelta(hours=1),
            start + timedelta(hours=2),
        ]
        timestamps = timestampsToNow(start, timedelta(hours=1))
        self.assertEqual(expect, list(timestamps))
