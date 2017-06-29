import jwt
from datetime import datetime
from flask_mongoengine import MongoEngine
from flask_login import UserMixin


db = MongoEngine()


class Role(db.Document):
    '''
    Different users can have different roles
    '''
    name = db.StringField(nullable=False, server_default='', unique=True)  # for @roles_accepted()
    label = db.StringField(server_default='')  # for display purposes


class ProcessStatus(db.Document):
    '''
    This one atomic unit of a status report from an instance, hopefully in the midst of processing
    '''

    # This class breaks our general style because it is an effort to conform to Koshyframework+AWS-SSM
    instanceId = db.StringField(),
    tagIndex = db.IntField(),
    tagName = db.StringField(),
    progress = db.FloatField(),
    status = db.StringField(),
    startTime = db.DateTimeField(),
    lastUpdatedAt = db.DateTimeField(),
    lastStateChange = db.DateTimeField(),
    task = db.StringField(),
    subtask = db.StringField(),
    currentCommandId = db.StringField()



class User(db.Document, UserMixin):
    '''
    For individual logins and accounts
    '''

    first_name = db.StringField(max_length=255)
    last_name = db.StringField(max_length=255)
    email = db.StringField(max_length=255)
    active = db.BooleanField(default=True)
    confirmed_at = db.DateTimeField()
    username = db.StringField()
    password_hash = db.StringField(max_length=255)
    client = db.ObjectIdField()

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
                'exp': datetime.utcnow() + datetime.timedelta(days=365),
                'iat': datetime.utcnow(),
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
    start = db.DateTimeField()
    end = db.DateTimeField()
    video_files = db.ListField(db.ObjectIdField())
    rvm = db.ObjectIdField()
    notes = db.StringField()


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
