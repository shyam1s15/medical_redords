from flask import jsonify
import flask

headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods":"*",
		    "Access-Control-Allow-Headers":"*",
		    "Access-Control-Allow-Credentials":"true",
		    "Access-Control-Max-Age":"3600"
        }

class APIResponse:
    def __init__(self):
        self.response = {'error': 0, 'message': 'Success'}
        self.content = None

    def serialize(self):
        api_content = {}
        if self.content is not None:
            api_content['content'] = self.content
        api_response = {
            'response': self.response
        }
        api_content.update(api_response)
        resp = jsonify(api_content)
        resp.headers.add('Access-Control-Allow-Origin', '*')
        resp.headers.add('Access-Control-Allow-Methods', '*')
        resp.headers.add('Access-Control-Allow-Headers', "Origin, X-Requested-With, Content-Type, Accept")  
        resp.headers.add("Access-Control-Max-Age", 3600)
        resp.headers.add('X-Content-Type-Options', 'nosniff')
        return resp


    @classmethod
    def ok(cls):
        # when sending only ok response with empty body
        return cls().serialize()
    
    @classmethod
    def ok_with_data(cls, object, message="Success"):
        obj = cls()
        obj.content = object
        obj.response['message'] = message
        return obj.serialize()

    @classmethod
    def error_with_message(cls, message="Something went wrong"):
        obj = cls()
        obj.response['message'] = message
        obj.response['error'] = 1
        return obj.serialize()

    @classmethod
    def error_with_code_message(cls, code=1, message="Something went wrong"):
        obj = cls()
        obj.response['message'] = message
        obj.response['error'] = code
        return obj.serialize()

    @classmethod
    def error_with_data_message(cls, object=None, message="Something went wrong"):
        obj = cls()
        obj.content = object
        obj.response['message'] = message
        obj.response['error'] = 1
        return obj.serialize()

    @classmethod
    def error_with_data_code_message(cls, object=None, code=1, message="Something went wrong"):
        obj = cls()
        obj.content = object
        obj.response['message'] = message
        obj.response['error'] = code
        return obj.serialize()
