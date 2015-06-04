#!/usr/bin/env python2.7
import boto.dynamodb2.layer1, json, sys, time, shutil, os, argparse, logging, datetime, threading, re
from BackupConfig import BackupConfig

configFile = "/data/dynamodump/dynamodump.conf"
THREAD_START_DELAY = 1 #seconds

CURRENT_WORKING_DIR = os.getcwd()

class DynamoDbBackup:
  """
  Class for backing up and restoring DynamoDb Tables.
  """
  def __init__(self, configfile = configFile, log_level = None, region = None, prefix_separator = None):
    config      = BackupConfig(configfile)
    self.dump_path       = config.get('settings', 'dump_path')
    self.json_indent     = int(config.get('settings', 'json_indent'))
    self.max_retry       = config.get('settings', 'max_retry')
    self.schema_file     = config.get('settings', 'schema_file')
    self.data_dir        = config.get('settings', 'data_dir')
    self.restore_write_capacity = int(config.get('settings', 'restore_write_capacity'))
    self.max_batch_write = int(config.get('settings', 'max_batch_write'))
    self.sleep_interval  = float(config.get('settings', 'aws_sleep_interval'))
    self.conn = None
    self.sleep_interval = 10.0

    # set log level
    if not log_level:
      log_level = config.get('settings', 'log_level')
    logging.basicConfig(level=getattr(logging, log_level))

    # instantiate connection
    if not region:
      region = config.get('settings', 'region')
    self.conn = boto.dynamodb2.connect_to_region(region)

    # set prefix separator
    if not prefix_separator:
      prefix_separator = config.get('settings', 'default_prefix_separator')

    self.whitelist = config.get('filters','whitelist').strip()
    self.blacklist = config.get('filters','blacklist').strip()

  def get_table_list(self):
    last_evaluated_table_name = None
    table_names = []
    while True:
      table_list = self.conn.list_tables(exclusive_start_table_name=last_evaluated_table_name)
      table_names.extend(table_list['TableNames'])
      if table_list.has_key('LastEvaluatedTableName'):
        last_evaluated_table_name = table_list['LastEvaluatedTableName']
      else:
        break
    return table_names

  def get_table_name_config(self):
    matched_tables = []
    table_names = self.get_table_list()
    for table in table_names:
      if len(self.whitelist) > 0:
        '''
        If there is a whitelist, use it, and ignore the blacklist.
        '''
        for filter in self.whitelist.split('\n'):
          if re.search(filter,table):
            matched_tables.append(table)
      elif len(self.blacklist) > 0:
        include = True
        for filter in self.blacklist.split('\n'):
          if re.search(filter,table):
            include = False
        if include:
            matched_tables.append(table)
      else:
        # Return all tables
        print 'Woah! why am I here????'
        matched_tables.append(table)
    return matched_tables

  def get_table_name_matches(self, table_name_wildcard):
    all_tables = []
    last_evaluated_table_name = None

    while True:
      table_list = self.conn.list_tables(exclusive_start_table_name=last_evaluated_table_name)
      try:
        last_evaluated_table_name = table_list['LastEvaluatedTableName']
      except KeyError:
        break

    matching_tables = []
    for table_name in all_tables:
      match = re.match(table_name_wildcard, table_name)
      if match:
        matching_tables.append(table_name)

    return matching_tables

  def get_restore_table_matches(self, table_name_wildcard):
    matching_tables = []
    try:
      dir_list = os.listdir('./' + self.dump_path)
    except OSError:
      logging.info('Cannot find "./%s", Now trying current working directory..' % self.dump_path)
      dump_data_path = CURRENT_WORKING_DIR
      try:
        dir_list = os.listdir(dump_data_path)
      except OSError:
        logging.info('Cannot find "%s" directory containing dump files!' % dump_data_path)
        sys.exit(1)

    for dir_name in dir_list:
      if dir_name.split(self.prefix_separator, 1)[0] == table_name_wildcard.split('*', 1)[0]:
        matching_tables.append(dir_name)

    return matching_tables

  def change_prefix(self, source_table_name, source_wildcard, destination_wildcard, separator):
    source_prefix = source_wildcard.split('*', 1)[0]
    destination_prefix = destination_wildcard.split('*', 1)[0]
    if source_table_name.split(separator, 1)[0] == source_prefix:
      return destination_prefix + separator + source_table_name.split(separator, 1)[1]

  def delete_table(self, table_name):
    while True:
      # delete table if exists
      table_exist = True
      try:
        self.conn.delete_table(table_name)
      except boto.exception.JSONResponseError, e:
        if e.body['__type'] == 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException':
          table_exist = False
          logging.info('%s table deleted!' % table_name)
          break
        elif e.body['__type'] == 'com.amazonaws.dynamodb.v20120810#LimitExceededException':
          logging.info('Limit exceeded, retrying deletion of ' + table_name + '..')
          time.sleep(self.sleep_interval)
        elif e.body['__type'] == 'com.amazon.coral.availability#ThrottlingException':
          logging.info('Control plane limit exceeded, retrying deletion of ' + table_name + '..')
          time.sleep(self.sleep_interval)
        elif e.body['__type'] == 'com.amazonaws.dynamodb.v20120810#ResourceInUseException':
          logging.info(table_name + ' table is being deleted..')
          time.sleep(self.sleep_interval)
        else:
          logging.exception(e)
          sys.exit(1)

    # if table exists, wait till deleted
    if table_exist:
      try:
        while True:
          logging.info('Waiting for %s table to be deleted.. [%s]' % (table_name, self.conn.describe_table(table_name)['Table']['TableStatus']))
          time.sleep(self.sleep_interval)
      except boto.exception.JSONResponseError, e:
        if e.body['__type'] == 'com.amazonaws.dynamodb.v20120810#ResourceNotFoundException':
          logging.info('%s table deleted.' % table_name)
          pass
        else:
          logging.exception(e)
          sys.exit(1)

  def mkdir_p(self, path):
    try:
      os.makedirs(path)
    except OSError as exc:
      if exc.errno == errno.EEXIST and os.path.isdir(path):
        pass
      else: raise

  def batch_write(self, table_name, put_requests):
    request_items = {table_name: put_requests}
    i = 1
    while True:
      response = self.conn.batch_write_item(request_items)
      unprocessed_items = response['UnprocessedItems']

      if len(unprocessed_items) == 0:
        break

      if len(unprocessed_items) > 0 and i <= self.max_retry:
        logging.debug('%s unprocessed items, retrying.. [%s]' % (len(unprocessed_items), i))
        request_items = unprocessed_items
        i += 1
      else:
        logging.info('Max retries reached, failed to processed batch write: %s ' % json.dumps(unprocessed_items, indent=self.json_indent))
        logging.info('Ignoring and continuing..')
        break

  def wait_for_active_table(self, table_name, verb):
    while True:
      if self.conn.describe_table(table_name)['Table']['TableStatus'] != 'ACTIVE':
        logging.info('Waiting for %s table to be %s.. [%s]' % (table_name, verb, self.conn.describe_table(table_name)['Table']['TableStatus']))
        time.sleep(self.sleep_interval)
      else:
        logging.info('%s %s.' % (table_name, verb))
        break

  def update_provisioned_throughput(self, table_name, read_capacity, write_capacity, wait=True):
    logging.info('Updating %s table read capacity to: %s, write capacity to: %s' % (table_name, read_capacity, write_capacity))
    while True:
      try:
        self.conn.update_table(table_name, {'ReadCapacityUnits': int(read_capacity), 'WriteCapacityUnits': int(write_capacity)})
        break
      except boto.exception.JSONResponseError, e:
        if e.body['__type'] == 'com.amazonaws.dynamodb.v20120810#LimitExceededException':
          logging.info('Limit exceeded, retrying updating throughput of %s..' % table_name)
          time.sleep(self.sleep_interval)
        elif e.body['__type'] == 'com.amazon.coral.availability#ThrottlingException':
          logging.info('Control plane limit exceeded, retrying updating throughput of %s..' % table_name)
          time.sleep(self.sleep_interval)

    # wait for provisioned throughput update completion
    if wait:
      self.wait_for_active_table(table_name, 'updated')

  def do_backup(self, table_name, read_capacity):
    logging.info('Starting backup for %s..' % table_name)

    # trash data, re-create subdir
    if os.path.exists('%s/%s' % (self.dump_path, table_name)):
      shutil.rmtree('%s/%s' % (self.dump_path, table_name))
    self.mkdir_p('%s/%s' % (self.dump_path, table_name))

    # get table schema
    logging.info('Dumping table schema for %s' % table_name)
    f = open('%s/%s/%s' % (self.dump_path, table_name, self.schema_file), 'w+')
    table_desc = self.conn.describe_table(table_name)
    f.write(json.dumps(table_desc, indent=self.json_indent))
    f.close()

    original_read_capacity = table_desc['Table']['ProvisionedThroughput']['ReadCapacityUnits']
    original_write_capacity = table_desc['Table']['ProvisionedThroughput']['WriteCapacityUnits']

    # override table read capacity if specified
    if read_capacity != None and read_capacity != original_read_capacity:
      self.update_provisioned_throughput(self.conn, table_name, read_capacity, original_write_capacity)

    # get table data
    logging.info('Dumping table items for %s' % table_name)
    self.mkdir_p('%s/%s/%s' % (self.dump_path, table_name, self.data_dir))

    i = 1
    last_evaluated_key = None

    while True:
      scanned_table = self.conn.scan(table_name, exclusive_start_key=last_evaluated_key)

      f = open('%s/%s/%s/%s.json' % (self.dump_path, table_name, self.data_dir, str(i).zfill(4)), 'w+')
      f.write(json.dumps(scanned_table, indent=self.json_indent))
      f.close()

      i += 1

      try:
        last_evaluated_key = scanned_table['LastEvaluatedKey']
      except KeyError:
        break

    # revert back to original table read capacity if specified
    if read_capacity != None and read_capacity != original_read_capacity:
      self.update_provisioned_throughput(table_name, original_read_capacity, original_write_capacity, False)

    logging.info('Backup for %s table completed. Time taken: %s' % (table_name, datetime.datetime.now().replace(microsecond=0) - start_time))

  def do_restore(self, source_table, destination_table, write_capacity):
    logging.info('Starting restore for %s to %s..' % (source_table, destination_table))

    # create table using schema
    # restore source_table from dump directory if it exists else try current working directory
    if os.path.exists('%s/%s' % (self.dump_path, source_table)):
      dump_data_path = self.dump_path
    else:
      logging.info('Cannot find "./%s/%s", Now trying current working directory..' % (self.dump_path, source_table))
      if os.path.exists('%s/%s' % (CURRENT_WORKING_DIR, source_table)):
        dump_data_path = CURRENT_WORKING_DIR
      else:
        logging.info('Cannot find "%s/%s" directory containing dump files!' % (CURRENT_WORKING_DIR, source_table))
        sys.exit(1)
    table_data = json.load(open('%s/%s/%s' % (dump_data_path, source_table, self.schema_file)))
    table = table_data['Table']
    table_attribute_definitions = table['AttributeDefinitions']
    table_table_name = destination_table
    table_key_schema = table['KeySchema']
    original_read_capacity = table['ProvisionedThroughput']['ReadCapacityUnits']
    original_write_capacity = table['ProvisionedThroughput']['WriteCapacityUnits']
    table_local_secondary_indexes = table.get('LocalSecondaryIndexes')
    table_global_secondary_indexes = table.get('GlobalSecondaryIndexes')

    # override table write capacity if specified, else use self.restore_write_capacity if original write capacity is lower
    if write_capacity == None:
      if original_write_capacity < self.restore_write_capacity:
        write_capacity = self.restore_write_capacity
      else:
        write_capacity = original_write_capacity

    # override GSI write capacities if specified, else use self.restore_write_capacity if original write capacity is lower
    original_gsi_write_capacities = []
    if table_global_secondary_indexes is not None:
      for gsi in table_global_secondary_indexes:
        original_gsi_write_capacities.append(gsi['ProvisionedThroughput']['WriteCapacityUnits'])

        if gsi['ProvisionedThroughput']['WriteCapacityUnits'] < self.restore_write_capacity:
          gsi['ProvisionedThroughput']['WriteCapacityUnits'] = self.restore_write_capacity

    # temp provisioned throughput for restore
    table_provisioned_throughput = {'ReadCapacityUnits': int(original_read_capacity), 'WriteCapacityUnits': int(write_capacity)}

    logging.info('Creating %s table with temp write capacity of %s' % (destination_table, write_capacity))

    while True:
      try:
        self.conn.create_table(table_attribute_definitions, table_table_name, table_key_schema, table_provisioned_throughput, table_local_secondary_indexes, table_global_secondary_indexes)
        break
      except boto.exception.JSONResponseError, e:
        if e.body['__type'] == 'com.amazonaws.dynamodb.v20120810#LimitExceededException':
          logging.info('Limit exceeded, retrying creation of %s..' % destination_table)
          time.sleep(self.sleep_interval)
        elif e.body['__type'] == 'com.amazon.coral.availability#ThrottlingException':
          logging.info('Control plane limit exceeded, retrying creation of %s..' % destination_table)
          time.sleep(self.sleep_interval)
        else:
          logging.exception(e)
          sys.exit(1)

    # wait for table creation completion
    self.wait_for_active_table(destination_table, 'created')

    # read data files
    logging.info('Restoring data for %s table..' % destination_table)
    data_file_list = os.listdir('%s/%s/%s' % (dump_data_path, source_table, self.data_dir))
    data_file_list.sort()

    for data_file in data_file_list:
      logging.info('Processing %s of %s' % (data_file, destination_table))
      items = []
      item_data = json.load(open('%s/%s/%s/%s' % (dump_data_path, source_table, self.data_dir, data_file)))
      items.extend(item_data['Items'])

      # batch write data
      put_requests = []
      while len(items) > 0:
        put_requests.append({'PutRequest': {'Item': items.pop(0)}})

        # flush every self.max_batch_write
        if len(put_requests) == self.max_batch_write:
          logging.debug('Writing next ' + str(self.max_batch_write) + ' items to ' + destination_table + '..')
          self.batch_write(destination_table, put_requests)
          del put_requests[:]

      # flush remainder
      if len(put_requests) > 0:
        self.batch_write(destination_table, put_requests)

    # revert to original table write capacity if it has been modified
    if write_capacity != original_write_capacity:
      self.update_provisioned_throughput(destination_table, original_read_capacity, original_write_capacity, False)

    # loop through each GSI to check if it has changed and update if necessary
    if table_global_secondary_indexes is not None:
      gsi_data = []
      for gsi in table_global_secondary_indexes:
        original_gsi_write_capacity = original_gsi_write_capacities.pop(0)
        if original_gsi_write_capacity != gsi['ProvisionedThroughput']['WriteCapacityUnits']:
          gsi_data.append({'Update': { 'IndexName' : gsi['IndexName'], 'ProvisionedThroughput': { 'ReadCapacityUnits': int(gsi['ProvisionedThroughput']['ReadCapacityUnits']), 'WriteCapacityUnits': int(original_gsi_write_capacity),},},})

      logging.info('Updating %s global secondary indexes write capacities as necessary..' % destination_table)
      while True:
        try:
          self.conn.update_table(destination_table, global_secondary_index_updates=gsi_data)
          break
        except boto.exception.JSONResponseError, e:
          if e.body['__type'] == 'com.amazonaws.dynamodb.v20120810#LimitExceededException':
            logging.info('Limit exceeded, retrying updating throughput of GlobalSecondaryIndexes in %s..' % destination_table)
            time.sleep(self.sleep_interval)
          elif e.body['__type'] == 'com.amazon.coral.availability#ThrottlingException':
            logging.info('Control plane limit exceeded, retrying updating throughput of GlobalSecondaryIndexes in %s..' % destination_table)
            time.sleep(self.sleep_interval)

    logging.info('Restore for %s to %s table completed. Time taken: %s' % (source_table, destination_table, datetime.datetime.now().replace(microsecond=0) - start_time))

if __name__ == '__main__':
    # parse args
    parser = argparse.ArgumentParser(description='Simple DynamoDB backup/restore.')
    parser.add_argument('-m', '--mode', help='"backup" or "restore"')
    parser.add_argument('-r', '--region', help='AWS region to use, e.g. "us-west-1". Uses what is in the config file %s by default' % configFile)
    parser.add_argument('-s', '--srcTable', help='Source DynamoDB table name to backup or restore from, use "tablename*" for wildcard prefix selection')
    parser.add_argument('-d', '--destTable', help='Destination DynamoDB table name to backup or restore to, use "tablename*" for wildcard prefix selection (defaults to use "-" separator) [optional, defaults to source]')
    parser.add_argument('--prefixSeparator', help='Specify a different prefix separator, e.g. "." [optional]')
    parser.add_argument('--readCapacity', help='Change the temp read capacity of the DynamoDB table to backup from [optional]')
    parser.add_argument('--writeCapacity', help='Change the temp write capacity of the DynamoDB table to restore to [defaults to config file %s' % configFile)
    parser.add_argument('--host', help='Host of local DynamoDB [required only for local]')
    parser.add_argument('--port', help='Port of local DynamoDB [required only for local]')
    parser.add_argument('--accessKey', help='Access key of local DynamoDB [required only for local]')
    parser.add_argument('--secretKey', help='Secret key of local DynamoDB [required only for local]')
    parser.add_argument('--log', help='Logging level - DEBUG|INFO|WARNING|ERROR|CRITICAL [optional]')
    parser.add_argument('-F','--config',help='Specify config file for matching tables.')
    args = parser.parse_args()

    log_level = None
    region = None
    prefix_separator = None

    if args.log != None:
      log_level = args.log.upper()
    if args.region != None:
      region = args.region
    if args.prefixSeparator != None:
      prefix_separator = args.prefixSeparator

    dynamo = DynamoDbBackup(configFile, log_level, region, prefix_separator)
    # do backup/restore
    start_time = datetime.datetime.now().replace(microsecond=0)
    if args.mode == 'backup':
      if args.srcTable and args.srcTable.find('*') != -1:
        matching_backup_tables = dynamo.get_table_name_matches(args.srcTable, prefix_separator)
        logging.info('Found %s table(s) in DynamoDB host to backup: %s' % (len(matching_backup_tables), ', '.join(matching_backup_tables)))

        threads = []
        for table_name in matching_backup_tables:
          t = threading.Thread(target=dynamo.do_backup, args=(table_name, args.readCapacity,))
          threads.append(t)
          t.start()
          time.sleep(THREAD_START_DELAY)

        for thread in threads:
          thread.join()

        logging.info('Backup of table(s) ' + args.srcTable + ' completed!')
      elif len(args.config) > 0:
        matching_backup_tables = dynamo.get_table_name_config()
        logging.info('Found %s table(s) in DynamoDB host to backup: %s' % (len(matching_backup_tables), ', '.join(matching_backup_tables)))

        threads = []
        for table_name in matching_backup_tables:
          t = threading.Thread(target=dynamo.do_backup, args=(table_name, args.readCapacity,))
          threads.append(t)
          t.start()
          time.sleep(THREAD_START_DELAY)

        for thread in threads:
          thread.join()

        logging.info('Backup of table(s) %s completed!' % args.srcTable)
      else:
        dynamo.do_backup(args.srcTable, args.readCapacity)
    elif args.mode == 'restore':
      if args.destTable != None:
        dest_table = args.destTable
      else:
        dest_table = args.srcTable

      if dest_table.find('*') != -1:
        matching_destination_tables = dynamo.get_table_name_matches(dest_table, prefix_separator)
        logging.info('Found %s table(s) in DynamoDB host to be deleted: %s' % (len(matching_destination_tables), ', '.join(matching_destination_tables)))

        threads = []
        for table_name in matching_destination_tables:
          t = threading.Thread(target=dynamo.delete_table, args=(table_name,))
          threads.append(t)
          t.start()
          time.sleep(THREAD_START_DELAY)

        for thread in threads:
          thread.join()

        matching_restore_tables = dynamo.get_restore_table_matches(args.srcTable, prefix_separator)
        logging.info('Found %s table(s) to restore: ' % (len(matching_restore_tables), ', '.join(matching_restore_tables)))

        threads = []
        for source_table in matching_restore_tables:
          t = threading.Thread(target=dynamo.do_restore, args=(source_table, dynamo.change_prefix(source_table, args.srcTable, dest_table), args.writeCapacity,))
          threads.append(t)
          t.start()
          time.sleep(THREAD_START_DELAY)

        for thread in threads:
          thread.join()

        logging.info('Restore of table(s) %s to %s completed!' % (args.srcTable, dest_table))
      else:
        dynamo.delete_table(dest_table)
        dynamo.do_restore(args.srcTable, dest_table, args.writeCapacity)
