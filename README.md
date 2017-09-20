## Symphony-Orchestrator

#### General
- The new symphony uses redis instead of a rabbit queue. This is beneficial because I was having a hard time maintaining persistent Rabbit connections (it requires heartbeats and it's just a little too full-featured and unwieldy)
- The redis queue is very simple, it is just a FIFO queue. It is hosted on the same server, and if you need to access the console, do so via: `redis-cli -h master -p 9376` (where `master` is an alias to the main server, 52.191.141.93). A common command is `keys *` to see the keys (which I use as queues)
- Redis is commonly used as a key-pair store, but it has additional functionality in the sence that keys can point to lists, which are essentually queues
- Namespaces are created by naming queues in the format: `[role]:[session_name]`

#### First
- Switch to `symphony` branch on ScanOrchestrator
- Switch to `redis` branch on MatlabCore

#### Running
- One script, `initialize.py`, sends tasks to a queue, usually the RVM queue when starting out. Tasks are defined by YAML files and placed in the `tasks` directory. When the initialization script is run (without any arguments), the tasks defined by the YAML files in `tasks` will all be sent. Before being sent, however, they go through a verification step which checks to make sure that the required data is available, like some of the DB data (vines per row), and also makes sure that the objects inthe database point to the correct resources, and in general catches some of the errors that would otherwise be encountered down the line
- One script, `utils/repair.py` can be used to automatically inject missing tasks in the proper queues in the case of a failure. It will take any YAML file in the tasks directory and analyze the 1) RVM, 2) Preprocess diretory, 3) Detection directory, and 4) Process directory and then create / send the missing tasks. If a file goes through RVM and preprocess, for example, but there is no corresponding detection file, the repair script will create the detection task and queue it. That file will go through detection and then processing as normal.

#### Assistant Scripts
There are a lot of scripts, usually in `utils/` that I use to do various tasks. One useful script is `status.py`, which creates a very simple, console-based, auto refreshed list of the current queues and their size