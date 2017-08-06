#!/bin/bash
# Custom Script for Linux (Ubuntu 16)

# Don't exit on errors (git throws errors that aren't really errors)
set +e

git_sync() {

    git_user_name=agkgeorge
    git_password=Panch56!
    git_organization_name=motioniq




    cd /home/agridata/code/projects/ScanOrchestrator
    git_repo_name=ScanOrchestrator

    branch_name_scanOrchestrator=$1

    git remote rm origin
    git remote add  origin "https://${git_user_name}:${git_password}@github.com/${git_organization_name}/${git_repo_name}"
    git fetch --all
    git checkout $branch_name_scanOrchestrator
    git config --global user.email "bot@agridata.ai"
    git config --global user.name "Windows Instance Bot"
    git merge origin/$branch_name_scanOrchestrator --no-edit
    git checkout --theirs .
    git add -u
    git commit -m "merge" --no-edit
    git checkout $branch_name_scanOrchestrator



    cd /home/agridata/code/projects/deepLearning
    git_repo_name=deepLearning
    branch_name_deepLearning=$2

    git remote rm origin
    git remote add  origin "https://${git_user_name}:${git_password}@github.com/${git_organization_name}/${git_repo_name}"
    git fetch --all
    git checkout $branch_name_deepLearning
    git config --global user.email "bot@agridata.ai"
    git config --global user.name "Windows Instance Bot"
    git merge origin/$branch_name_deepLearning --no-edit
    git checkout --theirs .
    git add -u
    git commit -m "merge" --no-edit
    git checkout $branch_name_deepLearning




    cd $HOME
}

# Stop old service
sudo systemctl stop myservice

# Update
git_sync $1 $2

# Run
sudo systemctl start myservice

exit
