#!/bin/bash

set -e

VERBOSE='-v'
USERS="kraken cass etl zk cdh4 cdh3 bundler job"

NORMAL='\033[00m'
WHITE='\033[01;37m'


### Utilities

log    () { [ "$VERBOSE" ] && echo -e "$*" >&2; :; }
logs   () { [ "$VERBOSE" ] && printf "%s" "$*"; :; }
whiten () { [ -z "$VERBOSE" ] && return; local NL='\n'; [ "x$1" = "x-n" ] && { NL=''; shift; }; printf "${NORMAL}${WHITE}$*${NORMAL}${NL}"; }
fail   () { echo >&2; echo "[ERROR] $*" >&2; exit 1; }
count  () { echo $#; }
nth    () { local n="$1"; shift; [ -z "$n" ] && return 1; [[ "$n" < 0 ]] && n=$(( 1 + $# + $n )); echo "${!n}"; }
join   () { local sep="$1" old="$IFS"; export IFS=\n; read -t1 out; while read -t1 line; do out="$out$sep$line"; done; echo "$out"; export IFS=$old; }
sjoin  () { local sep="$1"; shift; [ -z "$*" ] && return 1; printf "$1"; shift; for a in $*; do printf "$sep$a"; done; echo; }

# Run a command on all the machines in the kraken cluster.
kraken () {
    dsh --file etc/kraken.machines --show-machine-names --concurrent-shell -- $* 2>&1 | fgrep -v 'Killed by signal 1.'
}


### Create Users

whiten "Creating Users..."
for u in $USERS; do
    whiten "[$u] ..."
    
    logs "  [$u] Creating user...  "
    grps='--groups kraken'
    [ "$u" = 'kraken' ] && grps=''
    kraken sudo useradd --create-home --user-group $grps $u
    log 'ok'
    
    logs "  [$u] Generating keys...  "
    # kraken sudo -u $u mkdir -p /home/$u/.ssh
    kraken sudo -u $u ssh-keygen -t rsa -N \'\' -f /home/$u/.ssh/id_rsa
    log 'ok'
    
    log "  [$u] Aggregating keys...  "
    kraken "sudo -u $u mkdir -v -p /tmp/kraken-keys/\$HOSTNAME/$u"
    kraken "sudo cp -v /home/$u/.ssh/id_rsa /home/$u/.ssh/id_rsa.pub /tmp/kraken-keys/\$HOSTNAME/$u/"
    log 'ok'
    
    whiten "[$u] woo\n"
done
whiten '\ndone\n'



