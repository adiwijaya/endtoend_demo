import argparse
parser = argparse.ArgumentParser(description='This is script by Adi')
parser.add_argument('-e','--env',choices=['DEV','PROD'], help='Environment',required=True)
args = parser.parse_args()

environment = "DEV"
environment = args.env

if environment=="PROD":
    from .prod import *  # or .dev if you want dev
else:
    from .dev import *  # or .dev if you want dev
