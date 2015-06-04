#!/usr/bin/env python
configFile = "/data/dynamodump/dynamodump.conf"

from ConfigParser import ConfigParser

class BackupConfig(ConfigParser):
  def __init__(self, file=None):
    if not file:
      file = configFile
    ConfigParser.__init__(self)
    self.read(file)
