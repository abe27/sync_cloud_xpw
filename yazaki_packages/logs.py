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
        if os.path.exists(logfilename) is False:
            f = open(logfilename, mode='a+')
            f.writelines(f"{'0.'.ljust(5)} {str(datetime.datetime.now().strftime('%Y-%m-%d %X')).ljust(10)} {str(args[0]).ljust(5)}  {str(args[1]).ljust(70)} {(str(args[2]).lower()).ljust(5)}\n")
            f.close()

        else:
            lines = open(logfilename, mode='r')
            f = open(logfilename, mode='a+')
            f.writelines(f"{(str(len(lines.readlines()))+'.').ljust(5)} {str(datetime.datetime.now().strftime('%Y-%m-%d %X')).ljust(10)} {str(args[0]).ljust(5)}  {str(args[1]).ljust(70)} {(str(args[2]).lower()).ljust(5)}\n")
            f.close()