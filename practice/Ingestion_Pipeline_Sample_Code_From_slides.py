import singer
import json

columns = ("id", "name", "age", "has_children")

users = {(1, "Adrian", 32, False)
        ,(2, "Ruanne", 28, False)
        ,(3, "Hillary", 29, True)}
            
json_schema = {
    "properties": {"age": {"maximum": 130
                            ,"minimum": 1
                            ,"type": "integer"}
                    ,"has_children": {"type": "boolean"}
                    ,"id": {"type": "integer"}
                    ,"name": {"type": "string"}}
    ,"$id": "http://yourdomain.com/schemas/my_user_schema.json"
    ,"$schema": "http://json-schema.org/draft-07/schema#"}


# I haven't been able to make this method work
singer.write_schema(schema=json_schema
                    ,stream_name='DC_employees'
                    ,key_properties=["id"])


# I am not sure I understand what's happening here. Are we just creating a json file with the schema information? What is the point?
# writes the json-serialized object to the open file handle
with open("foo.json", mode="w") as fh:
    json.dump(obj=json_schema, fp=fh)


# Lines 36 as well as 39-40 seem to produce the same outcome with different ways (Maybe?)
# There is a difference that I don't seem to comprehend. The fixed_dict is replacing stream_name...
# How can we write multiple records? The presenter suggests the write_records method, but I am not sure how to use the syntax
singer.write_record(stream_name="DC_employees", record=dict(zip(columns, users.pop())))

# Did we write anything here?
fixed_dict = {"type": "RECORD", "stream": "DC_employees"}
record_msg = {**fixed_dict, "record": dict(zip(columns, users.pop()))}

print(json.dumps(record_msg))


