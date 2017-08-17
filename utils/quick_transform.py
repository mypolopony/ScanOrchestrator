from orchestrator import transformScan

class dotdict(dict):
    '''
    dot.notation access to dictionary attributes
    '''
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

scanids = ['2017-07-20_08-57', '2017-07-20_09-10', '2017-07-20_09-28', '2017-07-20_09-55', '2017-07-20_12-25', '2017-07-20_12-47', '2017-07-20_13-14', '2017-07-20_13-42', '2017-07-20_14-09', '2017-07-20_14-25', '2017-07-20_15-44', '2017-07-20_16-00', '2017-07-20_16-41', '2017-07-20_17-46']
client = '594ce94f1fb3590bef00c927'

for s in scanids:
    # scan = dotdict({'scanid':s, 'client':client})
    # transformScan(scan)

    