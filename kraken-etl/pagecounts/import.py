from datetime import datetime, timedelta
from util import (
    HdfsFileCollection, diffDatewise, timestampsToNow, shell
)

target = CamusUtils('hdfs://wmf/')
topic = 'pagecounts'
hdfsFormat = target.basedir(topic) + '%Y/%m/%d/%H'

imported = target.partitions(topic) or [datetime.today().strftime(hdfsFormat)]
firstHour = datetime.strptime(min(imported), hdfsFormat)
available = timestampsToNow(firstHour, timedelta(hours=1))

hoursMissing = diffDatewise(
    available,
    imported,
    rightParse=hdfsFormat,
)

for missing in hoursMissing:
    print('*************************')
    print('Importing {0} '.format(missing))
    
    try:
        result, err = shell([
            'import-one-hour',
            missing.year,
            missing.month,
            missing.day,
            missing.hour,
        ])
        if err:
            print(err)
    except Exception as e:
        print(e)
    
    print('Done Importing {0} '.format(missing))
    print('*************************')
