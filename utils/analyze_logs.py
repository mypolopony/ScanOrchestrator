import pandas as pd
import subprocess

tasktypes = {
    'Successfully':        'green',
    'Preprocessing':       'purple',
    'Un-tarring':          'red',
    'Downloading':         'pink',
    'Task':                'blue',
    'Received':            'yellow',
    'All':                 'purple',
    'RVM':                 'orange',
    'Calculating':         'brown',
    'I\'m':                'black'
}

# Export the logs with conditions
# mongoexport -h boringmachine --db status --collection orchestrator --fields ip,hostname,message,timestamp,session_name --query '{"session_name":"08.09.22.11", hostname: /preproc0/i }' --sort '{_id: -1}' --limit 3000 --type=csv > /tmp/rhetoric.csv

session_name = '08.10.13.52'
filename = '/tmp/rhetoric.csv'

print(' '.join(['mongoexport',
                 '--host', 'boringmachine',
                 '--db', 'status',
                 '--collection', 'orchestrator',
                 '--fields', 'ip,hostname,message,timestamp,session_name',
                 '--query', '\'{"session_name":"08.10.13.52", hostname: /preproc0/i }\'',
                 '--sort', '\'{_id: -1}\'',
                 '--limit', '5000',
                 '--type', 'csv',
                 '--out', filename]))

subprocess.call(['mongoexport',
                 '--host', 'boringmachine',
                 '--db', 'status',
                 '--collection', 'orchestrator',
                 '--fields', 'ip,hostname,message,timestamp,session_name',
                 '--query', '\'{"session_name":"08.10.13.52", hostname: /preproc0/i }\'',
                 '--sort', '\'{_id: -1}\'',
                 '--limit', '5000',
                 '--type', 'csv',
                 '--out', filename])

data = pd.read_csv(filename)

data['hostname'] = [hostname.replace('-matlab','') for hostname in data['hostname']]
data['tasktype'] = [msg.split(' ')[0] for msg in data['message']]
data['color'] = [tasktypes[tasktype] for tasktype in data['tasktype']]

data.to_csv(filename.replace('.csv','-plotly.csv'))