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
import random
import string
from clumio_sdk_v12 import DynamoDBBackupList, RestoreDDN, ClumioConnectAccount, AWSOrgAccount, ListEC2Instance, \
    EnvironmentId, RestoreEC2, EC2BackupList, EBSBackupList, RestoreEBS, OnDemandBackupEC2, RetrieveTask


def lambda_handler(events, context):

    record = events.get('inputs').get("record", [])
    bear = events.get('bear', None)
    target_account = events.get('target_account', None)
    target_region = events.get('target_region', None)
    debug_input = events.get('debug', None)
    target_az = events.get("target_aws_az", None)
    target_iops = events.get("target_iops", None)
    target_volume_type = events.get("target_volume_type", None)
    target_kms_key_native_id = events.get("target_kms_key_native_id", None)
    velero_manifest_dict = events.get('inputs').get("velero_manifest", None)

    inputs = {
        'task': None,
        "source_backup_id": None,
        "velero_manifest": velero_manifest_dict
    }

    # Validate inputs
    try:
        debug = int(debug_input)
    except ValueError as e:
        error = f"invalid debug: {e}"
        return {"status": 401, "task": None,"msg": f"failed {error}",
                "inputs": inputs}
    if debug > 20 : print(f"events {events}")
    if len(record) == 0:
        return {"status": 205, "msg": "no records",
                "inputs": inputs}

    # Initiate API and configure
    ebs_restore_api = RestoreEBS()
    ebs_restore_api.set_token(bear)
    ebs_restore_api.set_debug(debug)

    if record:
        source_backup_id = record[0].get("backup_record", {}).get('source_backup_id', None)
    else:
        error = f"invalid backup record {record}"
        return {"status": 402, "msg": f"failed {error}",
                "inputs": inputs}
    # Set restore target information

    ebs_restore_target = {
        "account": target_account,
        "region": target_region,
        "aws_az": target_az,
        "iops": target_iops,
        "kms_key_native_id": target_kms_key_native_id,
        "volume_type": target_volume_type
    }
    if debug > 5: print(f"ebs_restore_target {ebs_restore_target}")
    result_target = ebs_restore_api.set_target_for_ebs_restore(ebs_restore_target)
    if debug > 5: print(f"target set status {result_target}")
    # Run restore
    ebs_restore_api.save_restore_task()
    result_run = ebs_restore_api.ebs_restore_from_record(record, "add_source_volume_tag")
    if debug > 5: print(result_run)

    if result_run:
        # Get a task id
        task_list = ebs_restore_api.get_restore_task_list()
        task = task_list[0].get("task",None)
        if task:
            inputs = {
                'task': task,
                "source_backup_id": source_backup_id,
                "velero_manifest": velero_manifest_dict
            }
            return {"status": 200, "msg": "restore started", "inputs": inputs}
        else:
            return {"status": 404, "msg": f"restore task did not initiate {result_run}", "inputs": inputs}
    else:
        return {"status": 403, "msg": "failed", "inputs": inputs}