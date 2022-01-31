#!/bin/bash

#make sure you have the following users in your local system:
#"dovecot" and "dovenull" used by dovecot and "ftfstest" for 
#single client whoses maildir is tested.
#make sure you have run ./setup-*.sh
#setting the default values:
# m - mbox number default 10
# n - nest layers default 1
# nmsg - message numbers, default 3000
# testmail - the sample msg

set -x
FT_HOMEDIR=/home/betrfs/ft-index
. $FT_HOMEDIR/benchmarks/fs-info.sh

#prepare the maildir on the mounted fs
([ -d $mntpnt/ftfstest ] || sudo mkdir $mntpnt/ftfstest; sudo chown -R ftfstest:ftfstest $mntpnt/ftfstest)
m=10
n=1
nmsg=1000
succ=0
testmail=testmail
while [ "$1" != "" ]; do
    case $1 in
    -f )    shift
            testmail=$1
            ;;
    -m )    shift
            m=$1
            ;;
    -n )     shift
            n=$1
            ;;
    -nmsg )  shift
            nmsg=$1
            ;;
    * )      exit 1
    esac
    shift
done

RANDOM=$(date +%s)

get_rand_mbox()
{
    if [ $1 -lt 0 ] || [ $2 -lt 0 ]; then
      return 1;
    fi
    i=$2
    mailbox=mbox"$((RANDOM%m))"
    while [ $i -gt 1 ]; do
        idx=$((RANDOM%m))
        mailbox="$mailbox/mbox$idx"
        ((i--))
    done
    return 0
}
echo "mbox number:$m; mbox nest layers:$n;message number:$nmsg"

pushd .
cd $FT_HOMEDIR/benchmarks/support-files/dovecot-2.2.13/src/lda
echo "I love to go a-wondering, along with the moutain tracks..." > $testmail

accumulator=0
while [ $((nmsg)) -gt 0 ]; do
    get_rand_mbox $m $n
    if [ $? != 0 ]; then
        exit 1
    fi 
    begin_time=$(date +%s%3N)
    sudo ./dovecot-lda -d ftfstest -m $mailbox<$testmail
    end_time=$(date +%s%3N)
    if [ $? = 0 ]; then
        ((succ++))
        accumulator=$((end_time-begin_time+accumulator))
    fi
    ((nmsg--))
done
popd
ratio=`bc -l <<<"scale=3;$succ*1000/$accumulator"`
echo "delivered $succ messages, taking $accumulator ms, throughput=$ratio msg/sec"
exit 0
