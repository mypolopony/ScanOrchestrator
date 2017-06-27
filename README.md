#### Scan Orchestrator . 
_AgriData, Inc._

#### Purpose . 
A high-level wrapper for Koshy G.'s `infra` package, with added bells and whistles which handle communication and code execution for incoming AgriData scans.

#### Points of note . 
- Scan data is automatically uploaded to the `agridatadepot` bucket
- An SNS message is emitted by the client's box upon upload completion (topic: `arn:aws:sns:us-west-2:090780547013:new-upload`)
- An SQS queue (arn: `arn:aws:sqs:us-west-2:090780547013:new-upload-queue`) subsubscribes to this topic, and a task is enqueued
- A long poller (stereotype: `poller.py`, implementation: `orchestrator.py`) waits for tasks
- Upon receiving the task, the orchestrator, via `infra`, begins the processing pipeline
- Each step is dependent on the previous step entirely completing successfully. To monitor this, sub-tasks are placed into a "Task Results Queue" and are marked as either _successful_ or _failed_
- Before moving to the next step, the orchestrator monitors the Task Results Queue and continues only when it sees all sub-tasks succeed
- If a sub-task is placed in the Task Results Queue and marked as failed, the orchestrator is responsible for restarting that task on that specific instance as indicated by the newly enqueued task

#### Session Folders . 
- `/sunworld_file_transfer` hold the ephemeral files for a particular processing run. The format is `[prefix]_[instance_num]_[attempt]`. Given a prefix and the number of instances desired, the orchestrator looks to S3 and creates folders / session names that increment the most recent attempt.
