# Mail Server Experiments

## Setup
**WARNING**: this step could take HOURS to run. Run in tmux, so you can detach,
and come back later. At the time of writing, it takes 40 minutes to run. The
run time increases based on the number of mailboxes to be generated (among
other variables). See imap-punish.sh for variables.

Just run,
```bash
./setup.sh
```
to:

1. install dovecot dependencies,
1. create the user needed for the experiments,
1. create the mailboxes for the experiment, and back them up to the user directory.

The last step is the one that could take hours.


## IMAP Test

To run experiments for all filesystems,
```bash
./collect-all-fs.sh
```

To run experiments for a particular filesystem,
```bash
cd imap-test
./run-test.sh <FS>
```
where FS is the filesystem to test.

Read the script to understand the params if you do not want to use default
values.


## Delivery Test

A supposedly straightforward single client delivery throughput test. Check out
delivery\_throughput.sh to understand the params if you don't want to use
default values to test.
