#!/bin/bash
# Custom Script for Linux (Ubuntu 16)

# Don't exit on errors (git throws errors that aren't really errors)
set +e

git_sync() {
    cd /home/agridata/code/projects/deepLearning

    git_user_name=agkgeorge
    git_password=Panch56!
    git_organization_name=motioniq
    git_repo_name=deepLearning
    branch_name=dev

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
    cd $HOME
}

# Stop old service
sudo systemctl stop myservice

# Update
git_sync

# Run
python /home/agridata/code/projects/deepLearning/infra/ag_orchestrator.py &

exit