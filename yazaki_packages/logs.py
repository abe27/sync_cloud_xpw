class Logging():
    def __str__(self):
        return self # TODO
    
    
    def __init__(self, *args, **kwargs):
        import datetime
        import os
        import pathlib

        if os.path.exists(f'{pathlib.Path().absolute()}/logs') is False:
            os.makedirs(f'{pathlib.Path().absolute()}/logs')

        logfilename = f"{pathlib.Path().absolute()}/logs/{datetime.datetime.now().strftime('%Y-%m-%d')}.log"
        f = open(logfilename, mode='a+')
        f.writelines(f"{datetime.datetime.now().strftime('%Y-%m-%d %X')} {args[0]} {args[1]} {args[2]}\n")
        f.close()