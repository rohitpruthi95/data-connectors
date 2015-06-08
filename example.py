#!/usr/bin/python

from connectors import *

myConnectionInfo = ConnectionInfo('sftp','localhost',22,'dev',attributes={'privatekeyfile':'~/.ssh/id_rsa'})
print("Trying to establish connection")
myConnection = myConnectionInfo.getConnection()
myConnection.connect()
print("Connection established")
myReader = Reader(myConnection)
myPartitionReader = myReader.createPartitionReader()
myPartitionReader.read('./Desktop')
myConnection.close()
# myWriter = SFTPWriter(myConnection)
# myPartitionWriter = myWriter.createPartitionWriter()
# myPartitionWriter.write('holla','./Desktop')