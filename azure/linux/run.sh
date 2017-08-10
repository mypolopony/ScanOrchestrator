#!/bin/bash
# Custom Script for Linux (Ubuntu 16)

# Don't exit on errors (git throws errors that aren't really errors)
set +e

RUNAS=agridata

git_sync_core() {
    git_user_name=agkgeorge
    git_password=Panch56!
    git_organization_name=motioniq
    
    git_repo_name=$1
    branch_name=$2
     
    git remote rm origin
    git remote add  origin "https://${git_user_name}:${git_password}@github.com/${git_organization_name}/${git_repo_name}"
    git fetch --all
    git checkout $branch_name
    git config --global user.email "bot@agridata.ai"
    git config --global user.name "Windows Instance Bot"
    git merge origin/$branch_name --no-edit
    git checkout --theirs .
    git add -u
    git commit -m "merge" --no-edit
    git checkout $branch_name
}

export -f git_sync_core
    
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

# Update as agridata
su -p -c "git_sync $1 $2" - $RUNAS


# Restart service
sudo systemctl start myservice

exit
