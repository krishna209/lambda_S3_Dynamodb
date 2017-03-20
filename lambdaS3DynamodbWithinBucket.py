from __future__ import print_function
import boto3
import json
import decimal
import re
import traceback
import urllib
from boto3.dynamodb.conditions import Key, Attr
from cStringIO import StringIO

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o%1 > 0:
                return float(o)
            else:
                return int(o)
            return super(DecimalEncoder, self).default(o)
def lambda_handler(event, context):
    
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix='uploads/original'):            
        print("object : ",obj)
        key = obj.key
        bucket_nm = obj.bucket_name
        if not key.endswith('/'):
            try:
                attributes = []
                validator = []
                masking = []
                indx = []
                body = obj.get()['Body'].read()
                schema = body.split('\n', 1)[0]
                data = body.split('\n')[1:][:-1]
                schema_list = schema.split(',')
                split_key = key.split('/')
                file_name = split_key[-1]
                table_name_attr = file_name.split('_')[0]
                print("Received event: " + json.dumps(event))           
                dynamodb = boto3.resource('dynamodb', region_name='us-east-2', endpoint_url="https://dynamodb.us-east-2.amazonaws.com")
                table = dynamodb.Table('DataLake_Mask')
                response = table.query(
                    KeyConditionExpression=Key('Table_Name').eq(table_name_attr)
                )

                for i in response['Items']:
                    attributes.append(i['Attribute_Name'])
                    validator.append(i['RegEx_Validator'])
                    masking.append(i['RegEx_Masking'])
                    
                for att in attributes:
                    indx.append(schema_list.index(att))
                print(key)   
                masked_data = schema
                for line in data or []:
                    masked_line = ""
                    line_split = line.split(',')
                    for j, val in enumerate(indx):
                        if attributes[j] == "email":
                            match = re.match(validator[j], line_split[val])
                            if match == None:
                                masked_line = line.replace("%s"%line_split[val],"default")
                            else:
                                result = re.sub(r'%s'%masking[j],r'*',line_split[val])
                                masked_line = line.replace("%s"%line_split[val],"%s"%result)
                        elif attributes[j] == "ssn":
                            match = re.match(validator[j],line_split[val])
                            if match == None:
                                masked_line = line.replace("%s"%line_split[val],"*****0000")
                            else:
                                masked_line = line.replace("%s"%line_split[val],"*****%s"%line_split[val][-4:])
                                """result = re.sub(r"%s"%masking[j],r'***-**-$1',line_split[val])
                                masked_line = line.replace("%s"%line_split[val],"%s"%result)"""
                        else:
                            print("none")
                        line = masked_line
                    masked_data=masked_data+"\n"+masked_line

                """write masked data to a bucket"""
                s3_client = boto3.client('s3')
                fake_handle = StringIO(masked_data)
                s3_client.put_object(Bucket=bucket_nm, Key='uploads/masked/%s_masked'%file_name, Body=fake_handle.read())
                
                """move original files to backup filder"""
                s3_resource = boto3.resource('s3')
                s3.Object(bucket_nm,'uploads/unmasked/%s_unmasked'%file_name).copy_from(CopySource='%s/uploads/original/%s'%(bucket_nm,file_name))
                s3.Object('%s'%bucket_nm,'uploads/original/%s'%file_name).delete()

            except Exception as e:
                print(traceback.format_exc())
