import sys
import os
from yazaki_packages.cloud import SplCloud

def main():
    fname = os.listdir("./pdf")
    print(fname)

if __name__ == '__main__':
    main()
    sys.exit(0)