import argparse, os, csv, pprint, zipfile, math, sys, zlib

parser = argparse.ArgumentParser()

log_level = parser.add_mutually_exclusive_group(required = False)
log_level.add_argument("-verbose", "-v", action = "store_true",
                        help = "Log verbose")
log_level.add_argument("-quiet", "-q", action = "store_true",
                        help = "Suppress all output")

parser.add_argument("-t", '-type', dest = 'batch_type',
                    choices = ['size', 'count'], default = 'size',
                    help = "Split by size or ")

parser.add_argument('size', type = int, 
                    help = "Batch Size (count or size in MB)")

parser.add_argument("input", help="Input folder")
parser.add_argument("output", help="Output folder")

args = parser.parse_args()

_path = os.path.dirname(os.path.abspath(__file__))

tmp_dir = os.path.join(_path, "temp/")
complete_dir = os.path.join(_path, "completed/")
input_dir = os.path.join(_path, args.input)
output_dir = os.path.join(_path, args.output)

if (args.batch_type == "size"):
    batch_size = args.size * 1000000 # Convert MB to bytes

def setup():
    if not os.path.exists(tmp_dir):
        if args.verbose: 
            print("Creating temporary folder.")
        os.makedirs(tmp_dir)
        
    if not os.path.exists(complete_dir):
        if args.verbose: 
            print("Creating completed folder.")
        os.makedirs(complete_dir)

    sanitize(input_dir)
    os.chdir(tmp_dir) 

""" Removes .DS_STORE and __MACOSX folders.
    dir: The directory to search and sanitize"""
def sanitize(dir):
    if os.path.exists(dir + "/.DS_STORE"):
        if args.verbose: 
            print("removing .DS_STORE")
        os.system("rm -r {0}/.DS_STORE".format(dir))

    if os.path.exists(dir + "/__MACOSX"):
        if args.verbose: 
            print("removing __MACOSX")
        os.system("rm -r {0}/__MACOSX".format(dir))

""" Unzips archive to temp folder and sanitizes
    archive: path to zip file"""
def stage(archive):
    sanitize(tmp_dir)
    sanitize(input_dir)

    flags = '' if args.verbose else '-q' # Don't display unzip output unless verbose
    os.system("unzip {0} '{1}' -d '{2}'".format(flags, archive, tmp_dir))
  
""" Deletes contents of temp folder"""
def clean_up():
    if args.verbose: print("Cleaning tmp folder")
    os.system("rm -r {0}*".format(tmp_dir))

""" Move archive to completed dir """
def complete(archive):
    os.system("mv '{0}/{1}' '{2}{1}'".format(input_dir, archive, complete_dir))

""" Calculate the size of a given file """
def calc_size(name, compressed=True):
    if os.path.exists(name):
        if compressed:
            return os.path.getsize(name)
    else:
        return 0

""" Takes staged file and chunks
    the files within into smaller archives"""
def chunk(archive, limit, limit_type):
    files = os.listdir('.')[::-1] # in reverse since we pop off end (should just pop(0)...)

    chunk_counter = 1
    while files:
        name = archive.split(".")[0] + "-" + str(chunk_counter).zfill(4) + ".zip"
        if limit_type == "size":
            # While files remain, and the size of the current zip file plus the next file is smaller than the limit
            while files and calc_size(name) + calc_size(files[-1]) < limit:
                # flush zip file each time so we can get a proper calculation of the current and future file size
                with zipfile.ZipFile(name, "a") as chunk:
                    chunk.write(files.pop(), compress_type=zipfile.ZIP_DEFLATED)
                chunk.close()

        elif limit_type == "count":
            file_counter = 0
            with zipfile.ZipFile(name, "a") as chunk:
                while file_counter < limit and files:
                    chunk.write(files.pop(), compress_type=zipfile.ZIP_DEFLATED)
                    file_counter += 1
            chunk.close()

        chunk_counter += 1

        if not args.quiet:

            print("\tCompleted {0}\t{1:.2f} MB".format(name, calc_size(name)/1000000.0))

        cmd = "mv '{0}{1}' '{2}/{1}'".format(tmp_dir, name, output_dir)
        os.system(cmd)

def process(folder, limit_type):
    setup()
    for archive in os.listdir(folder):
        try:
            if not args.quiet:
                print("Unpacking {}".format(archive))
            stage(os.path.join(folder, archive))
            chunk(archive, batch_size, limit_type)
        except Exception as e:
            if args.verbose:
                print(e)
            if not args.quiet:
                print("Failed on {0}, skipping".format(archive))
            continue
        finally:
            clean_up()
        complete(archive)
    if not args.quiet:
        print("Finished.")

process(input_dir, args.batch_type)
