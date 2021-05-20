import sys
import os
from yazaki_packages.cloud import SplCloud

def main():
    fname = os.listdir("./pdf")
    for i in fname:
        if i != ".gitkeep":
            print(i)

if __name__ == '__main__':
    main()
    sys.exit(0)