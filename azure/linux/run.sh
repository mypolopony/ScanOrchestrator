#!/bin/bash
# Custom Script for Linux (Ubuntu 16)

# Don't exit on errors (git throws errors that aren't really errors)
set +e

#$1 == ScanOrchestrator brach name
#$2 == deepLearning brach name
#$3 == session_name

RUNAS=agridata

git_sync_core() {
    git_user_name=agkgeorge
    git_password=Panch56!
    git_organization_name=motioniq
    
    git_repo_name=$1
    branch_name=$2
     
    git config --global user.email "bot@agridata.ai"
    git config --global user.name "Windows Instance Bot"
    git remote rm origin
    git remote add origin "https://${git_user_name}:${git_password}@github.com/${git_organization_name}/${git_repo_name}"
    git fetch --all
    git checkout $branch_name --force
    git branch --set-upstream-to=origin/$branch_name $branch_name
    git pull
}

export -f git_sync_core

write_config() {
    session_name=$1
    data_dir="./data"
    cd /home/agridata/code/projects/ScanOrchestrator
    mkdir -p  ${data_dir}
    echo -e "[args]\nsession_name=${session_name}\n" > ${data_dir}/overriding.conf
    cd $HOME
}

export -f write_config

git_sync() {
    #syncing ScanOrchestrator
    git_repo_name=ScanOrchestrator
    cd /home/agridata/code/projects/$git_repo_name
    branch_name=$1
    git_sync_core $git_repo_name $branch_name




    #syncing deepLearning
    git_repo_name=deepLearning
    cd /home/agridata/code/projects/$git_repo_name
    branch_name=$2
    git_sync_core $git_repo_name $branch_name




    cd $HOME
}
export -f git_sync

# Stop old service
sudo systemctl stop myservice


pip install psutil
pip install redis

# Update as agridata
su -p -c "git_sync $1 $2" - $RUNAS
su -p -c "write_config $3" - $RUNAS


# Restart service
sudo systemctl start myservice

exit
