import redis
from datetime import datetime
import jsonschema
from .queue import LinkMessenger
import uuid
import strict_rfc3339

from .wrapper import ServiceWrapper, NotFound


schema = {
    "type": "object",
    "properties": {
        "page_title": { "type": "string" },
        "desc_text": { "type": "string" },
        "url_address": { "type": "string" },
        "author": { "type": "string" },
        "created": { "type": "string", "format": "date-time" }
    }
}

add_schema = schema.copy()
add_schema["required"] = ["page_title", "desc_text", "url_address", "author"]

class URLExists(Exception):
    """
    Raised when URL exists in redis.
    """
    
class AuthorNotFound(Exception):
    """
    Raised when the provided author is not a user in the system.
    """

class LinkManager:
    
    def __init__(self, redis_url="redis://localhost:6379/0", rabbit_url="amqp://localhost"):
        
        self.connection = redis.StrictRedis.from_url(redis_url, decode_responses=True)
        self.link_messenger = LinkMessenger(rabbit_url)
    
    
    def link_key(self, link_id):
        """
        key for redis
        """
        return "link:{}".format(link_id)
    
    
    def link_id(self):
        """
        Generating a random id.
        """
        return uuid.uuid4().hex
        
    def index_key(self):
        return "links:by-date"
        
    def url_key(self):
        return "links:urls"
    
    def url_exists(self, url_address):
        exists =  bool(self.connection.sismember(self.url_key(), url_address))
        return exists
    
    def add(self, **kwargs):
        jsonschema.validate(kwargs, add_schema)
        
        if self.url_exists(kwargs['url_address']):
            raise URLExists()
        
        if not kwargs.get("created", False):
            kwargs["created"] = strict_rfc3339.now_to_rfc3339_utcoffset()
            
        link_id = self.link_id()
        key = self.link_key(link_id)
        
        kwargs["link_id"] = link_id
        
        with self.connection.pipeline() as pipe:
            pipe.hmset(key, kwargs)
            pipe.sadd(self.url_key(), kwargs['url_address'])
            
            score = float(strict_rfc3339.rfc3339_to_timestamp(kwargs["created"]))
            
            pipe.zadd(self.index_key(), score, link_id)
            
            pipe.execute()
            
        
        self.link_messenger.added(link_id)
        
        return link_id
    
    
    def modify(self, link_id, **kwargs):
        print(kwargs)
        jsonschema.validate(kwargs, schema)
        
        key = self.link_key(link_id)
        
        if "url_address" in kwargs:
            existing_url = self.get_field(link_id, 'url_address')
            
            if kwargs['url_address'] != existing_url:
                if self.url_exists(kwargs['url_address']):
                    raise URLExists()
                
                with self.connection.pipeline() as pipe:
                    pipe.srem(self.url_key(), existing_url)
                    pipe.sadd(self.url_key(), kwargs['url_address'])
                    
                    pipe.execute()
                
        self.connection.hmset(key, kwargs)
        
        self.link_messenger.modified(link_id, **kwargs)
        
        
    
    def delete(self, link_id):
        key = self.link_key(link_id)
        
        existing_url = self.get_field(link_id, 'url_address')
        
        with self.connection.pipeline() as pipe:
            pipe.srem(self.url_key(), existing_url)
            pipe.zrem(self.index_key(), link_id)
            pipe.delete(key)
            
            pipe.execute()
        
        self.link_messenger.deleted(link_id)
    
    
    def get(self, link_id):
        key = self.link_key(link_id)
        
        result = self.connection.hgetall(key)
        
        self.link_messenger.viewed_link(link_id)
        
        return result
    
    def get_field(self, link_id, field):
        """
        Get a single field.
        
        Returns None if the link does not exist or the field does not exist.
        """
        key = self.link_key(link_id)
        
        result = self.connection.hget(key, field)
        
        self.link_messenger.viewed_field(link_id, field)
        
        return result
    
    def exists(self, link_id):
        """
        Returns true if link_id exists, returns false if it does not exist.
        """
        key = self.link_key(link_id)
        exists =  bool(self.connection.exists(key))
        
        self.link_messenger.link_exists(link_id, exists)
        
        return exists
        
    def count_links(self):
        result = self.connection.zcard(self.index_key())
        
        return result
        
    def list_links(self, start=0, stop=-1):
        result = self.connection.zrevrange(self.index_key(), start, stop)
        
        self.link_messenger.viewed_listing()
        
        return result