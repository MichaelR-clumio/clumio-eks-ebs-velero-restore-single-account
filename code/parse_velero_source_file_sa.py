# Copyright 2024, Clumio Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from botocore.exceptions import ClientError
from smart_open import open
from datetime import datetime
import json
import boto3


def lambda_handler(events, context):
    target_region = events.get('target_region', None)
    velero_file_s3_uri = events.get('velero_file_s3_uri', None)
    velero_file_segment_size = events.get('velero_file_segment_size', 40)
    debug = events.get('debug', None)

    inputs = {'records': [],
              'day_offset': None}
    s3_client = boto3.client('s3', region_name=target_region)
    source_uri = velero_file_s3_uri
    date_string = source_uri.split('-')[-2]
    if debug > 5: print(f"parse_velero_source_file: datestring {date_string}")
    format = '%Y%m%d%H%M%S'
    file_datetime = datetime.strptime(date_string, format)
    today = datetime.today()
    days_diff = (today - file_datetime).days
    if debug > 5: print(f"parse_velero_source_file: day offset {days_diff}")

    try:
        with open(source_uri, transport_params={"client": s3_client}) as fin:
            file_content = json.load(fin)
            if debug > 20: print(f"parse_velero_source_file: file content {file_content}")
        if len(file_content) > 0:
            big_list = file_content
            bigger_list = []
            num_items = len(big_list)
            num_sets = 0
            if num_items > velero_file_segment_size and num_items < (velero_file_segment_size * 39):
                _temp = num_items // velero_file_segment_size
                if num_items % velero_file_segment_size > 0:
                    num_sets = _temp + 1
                else:
                    num_sets = _temp

                print(f"num_sets: {num_sets}")
                for count in range(1, velero_file_segment_size):
                    print(f"count: {count}")
                    start = velero_file_segment_size * (count - 1)
                    stop = count * velero_file_segment_size
                    if stop > num_items:
                        stop = num_items
                    partial_list = big_list[start:stop]
                    print(partial_list)
                    if partial_list:
                        bigger_list.append(partial_list)
                    else:
                        break
                    start = start + 1
            else:
                bigger_list = [[big_list]]
            inputs = {'records': bigger_list,
                      'day_offset': days_diff}
            return {"status": 200, "inputs": inputs, "msg": "parsed velero file"}
        else:
            inputs = {'records': file_content,
                      'day_offset': None}
            return {"status": 207, "inputs": inputs, "msg": "no records"}

    except Exception as e:
        msg = f"could not read file {e}"
        return {"status": 401, "inputs": inputs, "msg": msg}