class Logging():
    def __str__(self):
        return self # TODO
    
    
    def __init__(self, *args, **kwargs):
        import datetime
        import os
        import pathlib

        if os.path.exists(f'{pathlib.Path().absolute()}/GEDI/LOGS') is False:
            os.makedirs(f'{pathlib.Path().absolute()}/GEDI/LOGS')

        logfilename = f"{pathlib.Path().absolute()}/GEDI/LOGS/{datetime.datetime.now().strftime('%Y%m%d')}.log"
        l = 1
        if os.path.exists(logfilename):
            lines = open(logfilename, mode='r')
            l = len(lines.readlines()) + 1

        f = open(logfilename, mode='a+')
        f.writelines(f"{(str(l)+'.').ljust(5)}{str(datetime.datetime.now().strftime('%Y-%m-%d %X')).ljust(25)}{str(args[0]).ljust(10)}{str(args[1]).ljust(90)}{(str(args[2]).lower()).ljust(5)}\n")
        f.close()