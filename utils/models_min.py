import jwt
import datetime
import json
import numpy as np
import ConfigParser
import boto3
from bson.objectid import ObjectId
from pymongo import MongoClient
from flask_mongoengine import MongoEngine, DoesNotExist
from flask_login import UserMixin

db = MongoEngine()


class Role(db.Document):
    '''
    Different users can have different roles
    '''
    name = db.StringField(nullable=False, server_default='', unique=True)  # for @roles_accepted()
    label = db.StringField(server_default='')  # for display purposes


class User(db.Document, UserMixin):
    '''
    For individual logins and accounts
    '''

    first_name = db.StringField(max_length=255)
    last_name = db.StringField(max_length=255)
    email = db.StringField(max_length=255, unique_with=['client'])
    active = db.BooleanField(default=True)
    confirmed_at = db.DateTimeField()
    username = db.StringField()
    password_hash = db.StringField(max_length=255)
    client = db.ObjectIdField(unique_with=['email'], required=True)

    @property
    def password(self):
        '''
        Prevent pasword from being accessed
        '''
        raise AttributeError('password is not a readable attribute.')


    @password.setter
    def password(self, password):
        '''
        Set password to a hashed password
        '''
        self.password_hash = generate_password_hash(password)


    def verify_password(self, password):
        '''
        Check if hashed password matches actual password
        '''
        return check_password_hash(self.password_hash, password)


    def encode_auth_token(self):
        '''
        Generates the Auth Token
        :return: string
        '''
        try:
            payload = {
                'exp': datetime.datetime.utcnow() + datetime.timedelta(days=365),
                'iat': datetime.datetime.utcnow(),
                'sub': str(self.id)
            }
            return jwt.encode(
                payload,
                '',
                algorithm='HS256'
            )
        except Exception as e:
            return jsonify(e)


    @staticmethod
    def decode_auth_token(auth_token):
        """
        Decodes the auth token
        :param auth_token:
        :return: integer|string
        """
        try:
            payload = jwt.decode(auth_token, '')
            return payload['sub']
        except jwt.ExpiredSignatureError:
            return 'Signature expired. Please log in again.'
        except jwt.InvalidTokenError:
            return 'Invalid token. Please log in again.'


    def __str__(self):
        return "User(id='%s')" % self.id

class Client(db.Document):
    '''
    For client accounts
    '''

    class ContactInfo(db.EmbeddedDocument):
        contact_name = db.StringField(max_length=255)
        contact_address = db.StringField(max_length=512)
        contact_phone = db.StringField(max_length=16)
        contact_email = db.StringField(max_length=32)

    name = db.StringField(unique=True)
    contact_info = db.EmbeddedDocumentField(ContactInfo)
    farms = db.ListField(db.ObjectIdField())
    scans = db.ListField(db.ObjectIdField())
    users = db.ListField(db.ObjectIdField())
    hw_season_start = db.DateTimeField()
    hw_season_end = db.DateTimeField()
    hw_units = db.IntField()
    notes = db.StringField()
    client_key = db.StringField()


class Observation(db.Document):
    '''
    For recordings and measurements
    '''

    userId = db.ObjectIdField()
    clientId = db.ObjectIdField()
    analysis = db.ObjectIdField()
    lastUpdatedAt = db.DateTimeField()
    type = db.StringField(max_length=255)
    subType = db.StringField(max_length=255)
    customType = db.StringField(max_length=255)
    context = db.ObjectIdField()
    source = db.StringField(max_length=255)
    data = db.DynamicField()
    value = db.DynamicField()
    subValue = db.DynamicField()
    state = db.IntField()
    notes = db.StringField()
    createdAt = db.DateTimeField()
    date = db.DateTimeField()
    timestamp = db.DateTimeField()
    vintage = db.IntField()
    accuracy = db.FloatField()              # GPS accuracy metric at time of observation (meters?)


class Farm(db.Document):
    '''
    A farm is a collection of blocks
    '''

    name = db.StringField(max_length=255, unique_with=['client'])
    client = db.ObjectIdField(unique_with=['name'])
    location = db.PointField()
    acerage = db.FloatField()
    notes = db.StringField()
    blocks = db.ListField(db.ObjectIdField())
    observations = db.ListField(db.ObjectIdField())
    thumbnail = db.StringField()
        
class Vine(db.Document):
    '''
    A "vine" is a plant in a row
    '''
    number = db.IntField()
    plantName = db.StringField()
    row = db.ObjectIdField(unique_with=['location'])
    location = db.PointField(unique_with=['row'])
    descriptors = db.DictField()
    root_stock = db.StringField()
    variety = db.StringField()
    plant_date = db.StringField()
    observations = db.ListField(db.ObjectIdField())


class Row(db.Document):
    '''
    A Row is a collection of plants
    '''

    name = db.StringField(unique_with=['block'])
    block = db.ObjectIdField(unique_with=['name'])
    clusters = db.ListField(db.DictField())
    num_plants = db.IntField()
    location = db.DictField()
    descriptors = db.DictField()
    vines = db.ListField(db.ObjectIdField())
    observations = db.ListField(db.ObjectIdField())


class Cluster(db.Document):
    '''
    Some plants are special enough to have their own object!
    '''

    name = db.StringField(max_length=32)
    row = db.ObjectIdField()
    observations = db.ListField(db.ObjectIdField())


class Block(db.Document):
    '''
    A Block is a collection of rows with, presumably, homogenous plants
    '''

    name = db.StringField(max_length=255, unique_with=['farm'])
    farm = db.ObjectIdField(unique_with=['name'])
    rows = db.ListField(db.ObjectIdField())
    location = db.PolygonField()
    descriptors = db.DictField()
    acreage = db.FloatField()
    num_plants = db.IntField()
    num_rows = db.IntField()
    notes = db.StringField()
    observations = db.ListField(db.ObjectIdField())


class Scan(db.Document):
    '''
    A Scan is one outing with the ATV
    '''

    client = db.ObjectIdField()
    scanid = db.StringField()
    start = db.DateTimeField()
    end = db.DateTimeField()
    farm = db.ObjectIdField()
    blocks = db.ListField(db.ObjectIdField())
    cameras = db.DictField()
    filenames = db.ListField()
    rvm = db.ObjectIdField()
    notes = db.ListField(db.StringField())
    startleft = db.StringField()
    startright = db.StringField()
    missed = db.ListField()
    direction = db.IntField(choices=(-1, 1), default=1)


class Video(db.Document):
    '''
    One Video and its metadata
    '''

    video_file = db.StringField(max_length=255)
    scan_id = db.ObjectIdField()
    logfile = db.StringField(max_length=255)
    notes = db.StringField()


class Frame(db.Document):
    '''
    One imageo in a video
    '''

    scan_id = db.ObjectIdField()
    video_id = db.ObjectIdField()
    frame_no = db.IntField()
    frame_file = db.StringField(max_length=255)
    height = db.IntField()
    width = db.IntField()
    rotation = db.IntField()
    rowtype = db.StringField(max_length=6, choices=('row', 'nonrow'))
    notes = db.StringField()


class rvm(db.Document):
    '''
    A Row-video-map defines the relationship between the layout of the block(s) and the associated imagery
    '''

    video_file = db.StringField(max_length=255)
    rvmap = db.ListField(db.DictField())


class dotdict(dict):
    '''
    dot.notation access to dictionary attributes
    '''
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId) or isinstance(o, datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)


class Task():

    '''
    Storage class used to standardize tasks. For the moment, task structure follows the previous
    implementation (from initiate.py) -- it can be cleaned up but is kept stable for the purposes
    of compatability
    '''

    def __init__(self, 
                 client_name,
                 farm_name,
                 block_name,
                 session_name = None,
                 caffemodel_s3_url_cluster='s3://deeplearning_data/models/best/cluster_june_15_288000.caffemodel', 
                 caffemodel_s3_url_trunk='s3://deeplearning_data/models/best/trunk_june_10_400000.caffemodel',
                 test=True, 
                 exclude_scans=None, 
                 include_scans=None,
                 role='rvm',
                 clientid=None):

        self.task = dotdict()

        # Session name (defaults to block + timestamp)
        if session_name:
            self.task.session_name = session_name
        else:
            self.task.session_name = block_name + datetime.fromtimestamp(time.time()).strftime('-%m_%d_%H_%M')
        
        # Models (this is *not* a dot dict)
        self.task.detection_params = dict()
        self.task.detection_params['caffemodel_s3_url_cluster'] = caffemodel_s3_url_cluster
        self.task.detection_params['caffemodel_s3_url_trunk'] = caffemodel_s3_url_trunk

        # Test
        self.task.test = test

        # Populate the main task structure
        try:
            print('Creating task')
            # Client
            self.task.client_name = client_name
            if not clientid:
                self.task.clientid =  Client.objects.get(name=self.task.client_name).id
            else:
                self.task.clientid = clientid

            # Farm
            self.task.farm_name = farm_name
            self.task.farmid = Farm.objects.get(name=self.task.farm_name, client=Client.objects.get(name=self.task.client_name).id).id

            # Block
            self.task.block_name = block_name
            self.task.blockid = Block.objects.get(name=self.task.block_name, farm=self.task.farmid).id


        except DoesNotExist:
            print('Sorry, that Client / Farm / Block could not be found')
            pass

        # Scan IDs
        if include_scans:
            # Selected scans
            if type(include_scans) is not list:
                include_scans = [include_scans]
            self.task.scanids = include_scans
        else:
            # All Scans
            self.task.scanids = [scan.scanid for scan in Scan.objects(blocks=self.task.blockid)]
            # Filter out scans
            if exclude_scans:
                if type(exclude_scans) is not list:
                    exclude_scans = [exclude_scans]
                self.task.scanids = list(set(self.task.scanids) - set(exclude_scans))

        # Role (defaults to rvm)
        self.task.role = role

        # Validation
        self.validate()

        
    def validate(self):
        '''
        Sanity checks to be passed before the task is accepted
        '''
        print('Validating scans')

        # Scans exist and are of the right block
        try:
            for scanid in self.task.scanids:
                assert(ObjectId(self.task.blockid) in Scan.objects.get(scanid=scanid).blocks)
        except AssertionError:
            print('\n\t!!! Error. Can not continue! Scans do not match block.\n\n')
            print('ScanID: {}\tBlocks: {}'.format(scanid, Scan.objects.get(scanid=scanid).blocks))
        except DoesNotExist:
            print('\n\t!!! Error. Does this scan exist? ({})\n\n'.format(scan.scanid))

        # Check cameras
        try:
            for scanid in self.task.scanids:
                assert(Scan.objects.get(scanid=scanid).cameras)
        except AssertionError:
            print('\n\t!!! Error. Can not continue! Camera definition not find in scan: {}\n\n'.format(scanid))

        # Check start rows
        try:
            for scanid in self.task.scanids:
                scan = Scan.objects.get(scanid=scanid)
                startleft = scan.startleft
                startright = scan.startright
                assert((int(startleft) or startleft == '0') and (int(startleft) or startleft == '0'))
        except (AssertionError, ValueError):
            print('\n\t!!! Error. Can not continue! Startleft and/or startright are invalid: {}\n\n'.format(scanid))
        
        # Block has rows
        print('Validating rows')
        block = Block.objects.get(id=self.task.blockid)  # Redundant grabbing of block

        try:
            assert(block.rows)
        except AssertionError:
            print('\n\t!!! This block has no rows!\n\n')
        try:
            # If num_rows is empty, maybe we can capture this from the row array
            if not block.num_rows and block.rows:
                block.update(num_rows=len(block.rows))
                block.save()

                assert(block.num_rows != 0)
        except AssertionError:
            print('\n\t!!! # of block.rows and num_rows is unequal or zero!\n\n')

        # Block has spacing
        print('Validating vine/row spacing')
        try:
            block = Block.objects.get(id=self.task.blockid)          # Redundant grabbing of block

            assert(block.descriptors['plant_space'] and block.descriptors['row_space'])
        except AssertionError:
            print('\n\t!!! Either row space or plant space is missing from the DB\n\n')


        # Row array matches row query
        try:
            rows = Row.objects(block=self.task.blockid)
            assert(len((set([r.id for r in rows])-set(block.rows))) == 0)
        except AssertionError:
            print('\n\t!!! Rows: {}'.format([r.id for r in rows]))
            print('\n\t!!! Blockrows: {}'.format(block.rows))
            print('\n\t!!! Error. Can not continue! The block and the rows do not match.\n\n')

        # Rows have num_plants
        print('Validating vines')
        try:
            for row in Row.objects(block=self.task.blockid):
                # assert(row.vines)
                if not row.num_plants:
                    row.update(num_plants=len(row.vines))
                # assert(row.num_plants == len(row.vines))
                # assert(len(set([v for v in row.vines])-set([v.id for v in Vine.objects(row=row.id)])) == 0)
        except AssertionError:
            print('\n\t!!! Error. Can not conotinue! The number of plants per row does not seem correct\n\n')

    def to_json(self):
        '''
        Return a dict representation
        '''
        return json.loads(JSONEncoder().encode(self.task))