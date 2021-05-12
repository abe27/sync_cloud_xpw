import datetime
from yazaki_packages.logs import Logging
t = Logging("title", "description", "STATUS")

etd = str((datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y%m%d'))
print(etd)

