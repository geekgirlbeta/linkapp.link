import configparser
from webob import Response, Request

from .manager import LinkManager, schema, URLExists, AuthorNotFound
from .queue import LinkMessenger
import jsonschema
import math

pagination_schema = {
    "type": "object",
    "properties": {
        "page": { "type": "integer" }
    }
}

import re

def bad_request(environ, start_response, msg="Bad Request", status=400):
    res = Response(msg, status=status)
    
    return res(environ, start_response)

class BadRequest(Exception):
    """
    Raised when something bad happened in a request
    """
    
class NotFound(Exception):
    """
    Raised when something is not found.
    """
    
class UnsupportedMediaType(Exception):
    """
    Raised when a bad content type is specified by the client.
    """



class LinkMicroservice:
    
    def __init__(self, config):
        self.config = config
        self.link_manager = LinkManager(self.config.redis_url, self.config.rabbit_url)
            
        self.link_messenger = LinkMessenger(self.config.rabbit_url)
    
    
    def __call__(self, environ, start_response):
        req = Request(environ)
        
        # /xxxxx/field1
        parts = req.path.split("/")
        print(parts)
        try:
            if req.content_type != "application/json":
                raise UnsupportedMediaType()
            
            if parts == ['', '']:
                if req.method == 'POST':
                    result = self.add(req)
                elif req.method == 'GET':
                    result = self.list_links(req)
                else:
                    raise BadRequest()
            elif len(parts) == 2:
                if req.method == 'GET':
                    result = self.get_one(req, parts[1])
                elif req.method == 'DELETE':
                    result = self.delete(req, parts[1])
                elif req.method == 'PUT':
                    result = self.modify(req, parts[1])
                else:
                    raise BadRequest()
            elif len(parts) == 3:
                if req.method == 'GET':
                    result = self.get_one_field(req, parts[1], parts[2])
                elif req.method == 'PUT':
                    result = self.modify_one_field(req, parts[1], parts[2])
                else:
                    raise BadRequest()
            else: 
                raise BadRequest()
        except BadRequest:
            return bad_request(environ, start_response)
        except URLExists:
            return bad_request(environ, start_response, "URL already exists.")
        except UnsupportedMediaType:
            return bad_request(environ, start_response, "Unsupported media type", 415)
        except NotFound:
            return bad_request(environ, start_response, "Not Found", 404)
        except AuthorNotFound:
            return bad_request(environ, start_response, "Author Not Found", 400)
        except jsonschema.ValidationError as e:
           return bad_request(environ, start_response, e.message)
        except ValueError as e:
            return bad_request(environ, start_response, str(e))
            
        res = Response()
        res.json = result
        return res(environ, start_response)
    
    
    def get_one(self, req, link_id):
        if not self.link_manager.exists(link_id):
            raise NotFound()
        return self.link_manager.get(link_id)
    
    
    def get_one_field(self, req, link_id, field):
        if not field in schema["properties"]:
            raise NotFound()
            
        result = self.link_manager.get_field(link_id, field)
        
        if result is None:
            raise NotFound()
            
        return result
    
    
    def add(self, req):
        new_link_id = self.link_manager.add(**req.json)
        return new_link_id
        
    
    
    def modify(self, req, link_id):
        return self.link_manager.modify(link_id, **req.json)
    
    
    def modify_one_field(self, req, link_id, field):
        if not field in schema["properties"]:
            raise NotFound()
        
        data = {field: req.json}
        
        return self.link_manager.modify(link_id, **data)
    
    def delete(self, req, link_id):
        if not self.link_manager.exists(link_id):
            raise NotFound()
        return self.link_manager.delete(link_id)
    
    def list_links(self, req):
        data = req.GET.mixed()
        
        try:
            data['page'] = int(data.get('page', 1))
        except ValueError:
            raise BadRequest("Invalid page number")
        
        jsonschema.validate(data, pagination_schema)
        
        page = data.get("page", 1)
        
        per_page = self.config.listing_per_page
        count = self.link_manager.count_links()
        last = int(math.ceil(count/per_page))
        
        if page > last:
            page = last
        
        if page < 1:
            page = 1
        
        stop = page*per_page-1
        start = page*per_page-per_page
        
        next = page+1
        previous = page-1
        
        links = self.link_manager.list_links(start=start, stop=stop)
        
        if next > last:
            next = None
            
        if previous < 1:
            previous = None
        
        return {
            'links': links,
            'pagination': {
                'next': next,
                'previous': previous,
                'count': count,
                'last': last
            }
        }