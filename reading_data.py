import boto3
import json


class S3_data:
    s3 = ""
    bucketName = ""
    PrefixFolder = ""
    s3ObjKey = ""

    """Constructor for S3_data"""

    def __init__(self, bucket, prefix, objKey, partition_field, device, ip_address, impressions, ad_blocking_rate):
        self.bucketName = bucket
        self.PrefixFolder = prefix
        self.s3ObjKey = objKey
        self.s3 = boto3.client('s3')

        self.event_ts = partition_field
        self.device = device
        self.ip_address = ip_address
        self.impressions = impressions
        self.ad_blocking_rate = ad_blocking_rate

    def __str__(self):
        return 'partition_field = ' + str(self.event_ts) + '\n' + 'List of all instances: ' + \
               str(self.device + self.ip_address + self.impressions + self.ad_blocking_rate)

    def reading_list_objects(self):
        objects = self.s3.list_objects_v2(Bucket=self.bucketName, Prefix=self.PrefixFolder)
        list_keys = []
        for obj in objects['Contents']:
            directoryName = obj['Key'].split('/')
            list_keys.append(directoryName[1])
        list_prefix = []
        list_prefix_value = []
        for pref in list_keys:
            split_partition_prefix = pref.split('=')
            list_prefix.append(split_partition_prefix[0])
            list_prefix_value.append(split_partition_prefix[1])
        print('Extracted partition attribute value separately from the respective partition prefix:')
        for i in range(len(list_prefix)):
            print(f'Partition prefix = "{list_prefix[i]}", Partition value = {list_prefix_value[i]}')
        self.event_ts = list_prefix_value[-1]
        print('\n')

    def reading_content(self):
        resp = self.s3.select_object_content(Bucket=self.bucketName, Key=self.s3ObjKey,
                                             ExpressionType='SQL',
                                             Expression="SELECT * FROM s3object s LIMIT 10",
                                             InputSerialization={'Parquet': {}},
                                             OutputSerialization={'JSON': {}},
                                             )
        lst = []
        print("Data from S3:")
        for event in resp['Payload']:
            if 'Records' in event:
                records = event['Records']['Payload'].decode('utf-8')
                print(records)  # print records from S3
                lst = records.split('\n')
        lst.pop(-1)  # delete blank record in lst
        values_list = []
        for item in lst:
            normalize_records = json.loads(item)  # normalizing records to dictionary representation
            values_records = list(normalize_records.values())
            values_list.append(values_records)
        list_all_info = [item for i in values_list for item in i]
        # print(list_all_info)  # list all values in one list
        self.device = list_all_info[0:len(list_all_info):4]
        self.ip_address = list_all_info[1:len(list_all_info):4]
        self.impressions = list_all_info[2:len(list_all_info):4]
        self.ad_blocking_rate = list_all_info[3:len(list_all_info):4]


if __name__ == '__main__':
    s3Obj = S3_data('bdc21-roman-korsun-user-bucket', 'kinesis-data-firehose-partitioned/event_ts_parquet_data_format',
                    'kinesis-data-firehose-partitioned/event_ts_parquet_data_format=1645281045/'
                    'bdc21-roman-korsun-delivery-stream-partitioned-3-2022-02-19-14-30-45-115be782-01d7-3c5d-bf1e'
                    '-eb213a947f32.parquet',
                    '', '', '', '', '')
    s3Obj.reading_list_objects()
    s3Obj.reading_content()
    print(s3Obj.__str__())
