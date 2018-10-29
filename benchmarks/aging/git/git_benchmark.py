#!/usr/bin/env python

################################################################################
# git_benchmark.py
#
# Usage:
# python git_benchmark.py src_repo dest output_file total_pulls pulls_per_test
#                test_script script_params         
#
# Parameters:
# src_repo: repository from which the benchmark pulls
# dest: this is the root of the filesystem to be aged
#       (generally this filesystem should be freshly initialized -- ie unaged)
# output_file: file to write results to
# total_pulls: number of pulls before the test stops
# pulls_per_test: how many pulls inbetween runs of test_script
# test_script: script to be performed every pulls_per_test pulls
# script_params: parameters to be passed to the test script

import subprocess
import shlex
import sys
import os
import argparse
from distutils.version import LooseVersion

####################
# globals

devnull = open(os.devnull, 'w')

####################
# initialization

# require git version 2.5+ for uploadpack.allowReachableSHA1InWant
git_version_cmd = "git --version | awk '{print $3}'"
git_version = subprocess.check_output(git_version_cmd, shell=True)
if (LooseVersion(git_version) < LooseVersion('2.5')):
    print("Git version must be 2.5+, currently {}".format(git_version))
    sys.exit(1)
else:
    print("Git version {} OK".format(git_version.strip()))

# parse the arguments
parser = argparse.ArgumentParser()
parser.add_argument("src_repo", help="source repository")
parser.add_argument("dest", help="destination location to be aged")
parser.add_argument("output_file", help="file to which results are written")
parser.add_argument("total_pulls", help="total number of pulls in the test", type=int)
parser.add_argument("pulls_per_test", help="run the test_script every this many pulls", type=int)
parser.add_argument("test_script", help="test script to be run")
parser.add_argument("script_params", help="parameters to pass to the test_script", nargs=argparse.REMAINDER)
args = parser.parse_args()

src_repo = os.path.abspath(args.src_repo)
dest = args.dest
test_script = "{} {}".format(args.test_script, " ".join(args.script_params))
output_file = open(args.output_file, 'w')
total_pulls = args.total_pulls
pulls_per_test = args.pulls_per_test

# prep output_file
output_file.write("pulls output\n")

# prep dest repo
repo_name = os.path.basename(os.path.normpath(src_repo))
dest_repo = os.path.abspath("{}/{}".format(dest, repo_name))
dest_mkdir_cmd = "mkdir -p {}".format(dest_repo)
git_init_cmd = "git init"
print("Initializing destination repository")
subprocess.check_call(shlex.split(dest_mkdir_cmd))
subprocess.check_call(shlex.split(git_init_cmd), cwd = dest_repo, stdout = devnull, stderr = devnull)

# configure git
git_sha_in_want_cmd = "git config uploadpack.allowReachableSHA1InWant True"
git_gc_off_cmd = "git config gc.auto 0"
git_gc_autodetach_cmd = "git config gc.autodetach False"
print("Configuring git")
subprocess.check_call(shlex.split(git_sha_in_want_cmd), cwd = src_repo)
subprocess.check_call(shlex.split(git_gc_autodetach_cmd), cwd = dest_repo)
subprocess.check_call(shlex.split(git_gc_off_cmd), cwd = dest_repo)

# generate list of commits
git_rev_list_cmd = "git rev-list --reverse HEAD"
rev_list = subprocess.check_output(shlex.split(git_rev_list_cmd), cwd = src_repo).split('\n')
if (len(rev_list) < total_pulls):
    print("Source repository does not have enough commits.")
    print("Have {}, test requires {}".format(len(rev_list), total_pulls))
    sys.exit(1)

print('--------------------------------------------------------------------------------')
print('Git-aging {} from local repository {}'.format(dest_repo, src_repo))
print('Running {} every {} pulls for {} total pulls'.format(test_script, pulls_per_test, total_pulls))
print('--------------------------------------------------------------------------------')

####################
# main loop

for pull in range(0, total_pulls + 1):
    # print progress bar
    overall_progress = 20 * pull / total_pulls
    current_progress = 20 * (pull % pulls_per_test) / pulls_per_test 
    progress = "\r Overall: |{0}{1}| {2: >3}%   Next test: |{3}{4}| {5: >3}%".format('#' * overall_progress, '-' * (20 - overall_progress), 100 * pull / total_pulls, '#' * current_progress, '-' * (20 - current_progress), 100 * (pull % pulls_per_test) / pulls_per_test)
    sys.stdout.write(progress)
    sys.stdout.flush()

    # perform the next git pull
    git_pull_cmd = "git pull --no-edit -q -s recursive -X theirs {} {}".format(src_repo, rev_list[pull].strip())
    subprocess.check_call(shlex.split(git_pull_cmd), cwd = dest_repo, stderr = devnull, stdout = devnull)

    # run the test_script
    if pull % pulls_per_test == 0:
        output = subprocess.check_output(shlex.split(test_script)).strip()
        output_line = "{} {}\n".format(pull, output)
        output_file.write(output_line)
        sys.stdout.write("\r{}".format(' ' * 80))
        sys.stdout.write("\r{}".format(output_line))
        sys.stdout.flush()

sys.stdout.write("\r{}".format(' ' * 80))
sys.stdout.flush()

