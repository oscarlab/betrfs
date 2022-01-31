#!/bin/bash
# Setup the mailserver. This only needs to be run once on a given machine.
# WARNING: this script can potentially take HOURS to run. At the time of
# writing, it takes about 40 minutes. It can take hours when creating a larger
# number of mailboxes. See imap-punish.py to change number of mailboxes.

set -eux

# helper functions
errcho() {
	[ $# -eq 1 ] || errcho 'Usage: errcho message'
	echo $1 >&2
	return 1
}

# 0. install dependencies
sudo apt-get install -y libssl-dev libpam-dev autoconf libtool python


# 1. build and install dovecot
wget https://dovecot.org/releases/2.3/dovecot-2.3.16.tar.gz
tar xf dovecot-2.3.16.tar.gz
cd dovecot-2.3.16/
./configure --with-pam --with-ssl
make -j$(nproc --all)
sudo make install
cd -
rm -rf dovecot-2.3.16.tar.gz dovecot-2.3.16/

wget https://github.com/dovecot/pigeonhole/archive/refs/tags/0.5.16.tar.gz
tar xf 0.5.16.tar.gz
cd pigeonhole-0.5.16/
./autogen.sh
./configure
make -j$(nproc --all)
sudo make install
cd -
rm -rf 0.5.16.tar.gz pigeonhole-0.5.16/


# 2. create users for experiment
create_user() {
	[ $# -eq 2 ] || [ $# -eq 3 ] || errcho 'Usage: create_user name pass [flags]'
	local user=$1 pass=$2 flags=${3:-}
	if ! id -u $user &>/dev/null; then
		echo -e "$pass\n$pass\n" | sudo adduser $user --gecos $user $flags
	else
		echo $user already exists.
	fi
}
# minimum password length on Ubuntu is 6 characters
# prevent dovecot and dovenull from logging in for security purposes
# TODO: can we disable login for ftfstest as well?
create_user dovecot asdasd '--shell /usr/sbin/nologin'
create_user dovenull asdasd '--shell /usr/sbin/nologin'
USER1=ftfstest
create_user $USER1 'ftfstest@2009'


# 3. extract dovecot config
sudo mkdir -p /usr/local/etc
sudo tar xf dovecot-conf.tar.gz -C /usr/local/etc


# 4. run mkcert
if [ ! -e /etc/ssl/certs/dovecot.pem ]; then
	cd /usr/local/share/doc/dovecot
	sudo chmod +x mkcert.sh
	sudo ./mkcert.sh
	cd -
fi


# 5. run dovecot
sudo dovecot


# Create mailboxes. This can take a very, very long time. Like HOURS long
# depending on the variables set in imap-punish.py.
cd imap-test
sudo rm -rf /mnt/benchmark
sudo mkdir -p /mnt/benchmark
sudo chmod 777 /mnt/benchmark
./imap-punish.py -s

# backup the files we just created
sudo cp -r /mnt/benchmark/$USER1 /home/$USER1
