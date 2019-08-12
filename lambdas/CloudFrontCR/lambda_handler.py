import sys
import os
import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sys.path.append(f"{os.environ['LAMBDA_TASK_ROOT']}/lib")
sys.path.append(os.path.dirname(os.path.realpath(__file__)))

import json
import cr_response
import uuid

"""
TODO: figure out a way to poll the distribution status using Lambda
We'll have to submit a rule to CloudWatch with the following parameters
(the rule will make the API call on behalf of the Lambda):
- a Lambda
- the CloudFormation resource to signal
- the CloudFront distribution ID
- the desired state of the distribution



TODO: don't respond with anything when a waiter is submitted.



"""

def handler(event, context):
    event = jsonify(event)
    logger.info(f"Received event:{json.dumps(event, indent=2)}")

    if 'PhysicalResourceId' not in event:
        # Set a default ID to prevent errors about invalid physical resource IDs
        event['PhysicalResourceId'] = str(uuid.uuid4())

    try:
        if 'UpdateConfig' not in event['ResourceProperties']:
          event['ResourceProperties']['UpdateConfig'] = {}

        lambda_response = cr_response.CustomResourceResponse(event)
        client = boto3.client('cloudfront')

        stack_arn = event['StackId']
        properties = event['ResourceProperties']
        physical_resource_id = event['PhysicalResourceId']

        properties = preprocess_structure(properties)
        properties = convert_structure(properties)

        if event['RequestType'] == 'Create':
            outputs = _create_distribution(client, event, properties, stack_arn)
            lambda_response.payload['PhysicalResourceId'] = outputs['Id']
            lambda_response.respond(data=outputs, NoEcho=False)

        elif event['RequestType'] == 'Update':
            outputs = _update_distribution(client, event, properties, physical_resource_id)
            lambda_response.respond(data=outputs, NoEcho=False)

        elif event['RequestType'] == 'Delete':
            _delete_distribution(client, event, properties, physical_resource_id)
            lambda_response.respond(data={}, NoEcho=False)

    except Exception as e:
        message = str(e)
        lambda_response.respond_error(message)
    return 'OK'


def _create_distribution(client, event, properties, stack_arn):
    """
    Creates a CloudFront distribution.
    """
    tags = {'Items': [
        {'Key': 'Status', 'Value': 'Created'},
        {'Key': 'StackARN', 'Value': stack_arn}
    ]}

    if 'Tags' in properties:
        tags['Items'] += properties['Tags']

    distribution_config = {
        'DistributionConfig': properties['DistributionConfig'],
        'Tags': tags
    }

    logger.info("Creating CloudFront distribution with tags...")
    response = client.create_distribution_with_tags(DistributionConfigWithTags=distribution_config)
    logger.info("Success.")

    distribution = response['Distribution']

    outputs = {
        'Id': distribution['Id'],
        'ARN': distribution['ARN'],
        'DomainName': distribution['DomainName']
    }

    logger.info("Outputs:\n%s" % json.dumps(outputs))

    # TODO: wait for the distribution to be created
    wait_for_status = properties['UpdateConfig'].get('WaitForCreation', True)
    if (wait_for_status) {
        update_polling_rule(
            rule_name=properties['DistributionHelpers']['PollDistributionsRule'],
            function_arn=properties['DistributionHelpers']['PollDistributionsFunctionArn'],
            distribution_id=physical_resource_id,
            distribution_arn=distribution_arn,
            desired_state='Deployed',
            enabled=distribution_config['Enabled'],
            resource_to_signal=physical_resource_id,
            event=event
        )
        exit(0)
    }

    return outputs


def _update_distribution(client, properties, physical_resource_id):
    """
    Updates a CloudFront distribution.
    """
    response = client.get_distribution_config(Id=physical_resource_id)

    etag = response['ETag']
    distribution_config = properties['DistributionConfig']

    logger.info("Updating CloudFront distribution: %s ..." % (physical_resource_id))
    response = client.update_distribution(Id=physical_resource_id, DistributionConfig=distribution_config, IfMatch=etag)
    logger.info("Success.")

    distribution = response['Distribution']

    outputs = {
        'Id': distribution['Id'],
        'ARN': distribution['ARN'],
        'DomainName': distribution['DomainName']
    }

    # TODO: wait for the distribution to be created
    wait_for_status = properties['UpdateConfig'].get('WaitForUpdate', True)
    if (wait_for_status) {
      update_polling_rule(
            rule_name=properties['DistributionHelpers']['PollDistributionsRule'],
            function_arn=properties['DistributionHelpers']['PollDistributionsFunctionArn'],
            distribution_id=physical_resource_id,
            distribution_arn=distribution_arn,
            desired_state='Deployed',
            enabled=distribution_config['Enabled'],
            resource_to_signal=physical_resource_id
        )
        exit(0)
    }

    return outputs


def _delete_distribution(client, properties, physical_resource_id):
    """
    Deletes a CloudFront distribution.

    If the distribution's status is Deployed, this function disables it and
    then sets its tag to be Status=Deleting, where a schedule Lambda function
    waits for it to be disabled and then removes it.
    """

    # 1. Get the distribution's config
    # 2. Set Enabled=False, Aliases={}
    # 3. Send update-distribution with the updated config
    # 4. Tag the distribution as Status=Deleting
    # 5. Enable the CloudWatch rule to trigger the CleanUp Lambda function

    response = client.get_distribution(Id=physical_resource_id)

    if response['Distribution']['Status'] == 'Deployed' and response['Distribution']['DistributionConfig']['Enabled'] == False:
        logger.info("Deleting CloudFront distribution: %s ..." % (physical_resource_id))
        try:
            client.delete_distribution(Id=physical_resource_id, IfMatch=response['ETag'])
            logger.info("Success.")
        except Exception as e:
            logger.info("Ignoring error:")
            print(str(e))

    else:
        logger.info("Distribution '%s' is currently set as 'Deployed' and must be disabled before deletion." % (physical_resource_id))
        logger.info("Getting config for distribution...")
        response = client.get_distribution_config(Id=physical_resource_id)

        # An ETag must be provided when updating a distribution
        etag = response['ETag']
        config = response['DistributionConfig']

        config['Enabled'] = False
        config['Comment'] = 'Scheduled for deletion - ' + config['Comment']
        config['Aliases'] = {'Quantity': 0}

        logger.info("Removing aliases and disabling distribution...")
        response = client.update_distribution(Id=physical_resource_id, DistributionConfig=config, IfMatch=etag)

        # Mark the distribution as deleting
        logger.info("Tagging the distribution with 'Status = Deleting' ...")
        distribution_arn = response['Distribution']['ARN']
        tags = {'Items': [{'Key': 'Status', 'Value': 'Deleting'}]}
        client.tag_resource(Resource=distribution_arn, Tags=tags)

        wait_for_status = properties['UpdateConfig'].get('WaitForDeletion', True)
        if (wait_for_status) {
            update_polling_rule(
                rule_name=properties['DistributionHelpers']['PollDistributionsRule'],
                function_arn=properties['DistributionHelpers']['PollDistributionsFunctionArn'],
                distribution_id=physical_resource_id,
                distribution_arn=distribution_arn,
                desired_state='Deployed',
                enabled=False,
                resource_to_signal=physical_resource_id
            )
            exit(0)
        }
        else {
            update_disable_rule(
                rule_name=properties['DistributionHelpers']['CleanUpDistributionsRule'],
                function_arn=properties['DistributionHelpers']['CleanUpDistributionsFunctionArn'],
                distribution_id=physical_resource_id,
                distribution_arn=distribution_arn
            )
        }

        logger.info("Success.")


def update_disable_rule(rule_name, function_arn, distribution_id, distribution_arn):
    # 1. Get the current target config
    # 2. Update the target config to contain a payload with this distribution's ID
    # 3. Call `put_target` on the rule to update the target
    # 4. Call `put_rule` to enable the rule
    events_client = boto3.client('events')
    logger.info("Adding distribution '%s' as a new target to CloudWatch rule '%s' to trigger the Lambda cleanup function..." % (distribution_id, rule_name))

    target = {
        'Id': distribution_id,
        'Arn': function_arn,
        'Input': json.dumps(
            {
                'RuleName': rule_name,
                'DistributionId': distribution_id,
                'DistributionARN': distribution_arn
            }
        )
    }

    events_client.put_targets(Rule=rule_name, Targets=[target])

    logger.info("Enabling rule...")
    events_client.put_rule(Name=rule_name, State='ENABLED', ScheduleExpression='rate(5 minutes)')


def update_polling_rule(rule_name, function_arn, distribution_id, distribution_arn, desired_state, event):
    events_client = boto3.client('events')

    logger.info("The polling for the distribution state will be offloaded to Lambda function: %s" % function_arn)
    logger.info("Adding distribution '%s' as a new target to CloudWatch rule '%s' to trigger the polling Lambda function..." % (distribution_id, rule_name))

    target = {
        'Id': distribution_id,
        'Arn': function_arn,
        'Input': json.dumps(
            {
                'RuleName': rule_name,
                'DesiredState': desired_state,
                'DistributionId': distribution_id,
                'DistributionARN': distribution_arn,
                'ResourceToSignal': {
                  'StackId': event['StackId'],      # TODO: verify this
                  'RequestId': event['RequestId'],
                  'LogicalResourceId': event['LogicalResourceId']
                }
            }
        )
    }

    events_client.put_targets(Rule=rule_name, Targets=[target])

    logger.info("Enabling rule...")
    events_client.put_rule(Name=rule_name, State='ENABLED', ScheduleExpression='rate(5 minutes)')


def jsonify(obj):
    """
    Convert any strings to their boolean and integer equivalents.
    This is required to transform invalid JSON into valid JSON.
    """
    if type(obj) == dict:
        for k,v in obj.items():
            obj[k] = jsonify(v)
        return obj

    elif type(obj) == list:
        for i, item in enumerate(obj):
            obj[i] = jsonify(item)
        return obj

    elif type(obj) == str:
        if obj.lower() == 'true':
            return True
        elif obj.lower() == 'false':
            return False
        elif obj.isnumeric():
            return int(obj)
        else:
            return obj

def preprocess_structure(properties):
  """Apply any transformations to the provided structure before we process them
  against the API structure.
  """
  config = properties['DistributionConfig']

  try:
    config['IsIPV6Enabled'] = config['IPV6Enabled']
    properties['DistributionConfig'] = config
  except KeyError:
    pass

  return properties


def convert_structure(properties):
  api_structure = json.loads(open('ApiStructure.json').read())
  new_structure = {}

  for name, properties in api_structure.items():
    print("")
    print("Processing key:", name)
    provided_item = provided_structure.get(name, None)

    print("Provided item:", provided_item)
    print("Required:", properties['Required'])
    print("Type:", properties['Type'])

    if provided_item:
      # Each property type tells us how to process the provided structure.
      if properties['Type'] == 'Random':
        new_structure[name] = generate_random_string()

      elif properties['Type'] in ['Boolean', 'String', 'Integer']:
        new_structure[name] = provided_item

      elif properties['Type'] in ['BooleanList', 'StringList', 'IntegerList']:
        new_structure[name] = {
          'Quantity': len(provided_structure[name]),
          'Items': provided_structure[name]
        }

      elif properties['Type'] == 'Object':
        new_structure[name] = convert_structure(provided_item, properties['ObjectFields'])

      elif properties['Type'] == 'ObjectList':
        object_list = []

        # An ObjectList corresponds to a provided structure of [{}, {}, ...]
        for item in provided_item:
          print("YES", item)
          api_substructure = dict(properties['ObjectFields'].items())
          new_item = convert_structure(item, api_substructure)
          object_list.append(new_item)

        new_structure[name] = {
          'Quantity': len(object_list),
          'Items': object_list
        }

    elif properties['Required']:
      # The item was not provided in CloudFormation by the user but
      # is required by the API. Generate defaults.
      new_structure[name] = generate_defaults(properties)

  return new_structure


def generate_defaults(properties):
  print("Generating default.")

  if properties['Type'] == 'Random':
    return generate_random_string()


def generate_random_string():
  return str(datetime.datetime.now())

