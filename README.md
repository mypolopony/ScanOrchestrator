## Redis-Symphony-Orchestrator

#### General
- The new symphony uses a redis queue instead of a rabbit queue. This is beneficial because I was having a hard time maintaining persistent Rabbit connections (it requires heartbeats and it's just a little too full-featured and unwieldy)
- The redis queue is very simple, it is just a FIFO queue. It is hosted on the same server, and if you need to access the console, do so via: `redis-cli -h master -p 9376` (where `master` is an alias to the main server, 52.191.141.93). A common command is `keys *` to see the keys (which I use as queues)
- Redis is commonly used as a key-pair store, but it has additional functionality in the sence that keys can point to lists, which are essentually queues (https://redislabs.com/ebook/part-1-getting-started/chapter-1-getting-to-know-redis/1-2-what-redis-data-structures-look-like/)
- Namespaces are created by naming queues in the format: `[role]:[session_name]`


#### First
- Switch to `redis` branch on ScanOrchestrator
- Switch to `symphony` branch on MatlabCore


#### Deploying
- I only use one resource group with two scale sets, one windows and one linux. To deploy the Linux boxes, I use Koshy's `azure/linux/deploy.sh` and only need to change the resource group name
- To deploy the windows machines, I use the portal. I don't know why. I just like it. I add a template deployment and copy/paste `utils/vmss-windows.json` into the template entry box, then give names to one or two of the resources, and deploy
- The linux scale set comes with a jumpbox. The Windows scale set doesn't but is created (again portal) by creating a VM off of `MyCustomImage`, which is deployed by the Windows ARM.
- This only needs to be done once!


#### Starting
- One script, `initialize.py`, sends tasks to a queue, usually the RVM queue when starting out. Tasks are defined by YAML files and placed in the `tasks` directory. When the initialization script is run (without any arguments), the tasks defined by the YAML files in `tasks` will all be sent. Before being sent, however, they go through a verification step which checks to make sure that the required data is available, like some of the DB data (vines per row), and also makes sure that the objects inthe database point to the correct resources, and in general catches some of the errors that would otherwise be encountered down the line
- One script, `utils/repair.py` can be used to automatically inject missing tasks in the proper queues in the case of a failure. It will take any YAML file in the tasks directory and analyze the 1) RVM, 2) Preprocess diretory, 3) Detection directory, and 4) Process directory and then create / send the missing tasks to the appropriate queues. For example, if a file (part of a row) goes through RVM and preprocess, but there is no corresponding detection file, the repair script will create the detection task and enqueue it. That file will go through detection and then on to processing as normal.


#### Operation
- The windows machines will look at tasks in the namespaces `rvm`, `preproc` and `preprocess`. The linux machines will look at queues with namespace `detection`.
- Windows machines each deploy four processes to maximize core usage. Each process is individually responsible for grabbing tasks from the queues, so no processing time is wasted and CPU usage should be near ~95% at all times. This is viewable using the Azure metics on the VMSS.


#### Assistant Scripts
There are a lot of scripts, usually in `utils/` that I use to do various tasks. One useful script is `status.py`, which creates a very simple, console-based, auto refreshed list of the current queues and their size