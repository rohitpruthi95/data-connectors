#!/usr/bin/python

from connectors import *

myConnectionInfo = ConnectionInfo('sftp','localhost',22,'dev',attributes={'privatekeyfile':'~/.ssh/id_rsa'})
print("Trying to establish connection")
myConnection = myConnectionInfo.getConnection()
myConnection.connect()
print("Connection established")
# myReader = Reader(myConnection)
# myPartitionReader = myReader.createPartitionReader()
# myPartitionReader.read('./Desktop')
myWriter = Writer(myConnection)
myPartitionWriter = myWriter.createPartitionWriter()
myPartitionWriter.write('s3://rohitp/tmp/2014-12-04/2688/3546862.err','./Desktop')
myConnection.close()