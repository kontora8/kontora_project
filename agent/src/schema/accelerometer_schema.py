from marshmallow import Schema, fields


class AccelerometerSchema(Schema):
    x = fields.Int()
    y = fields.Int()
    z = fields.Int()
    air = fields.Int()
    noise = fields.Int()
