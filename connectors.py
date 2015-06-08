#!/usr/bin/python

import paramiko
import time
import datetime
import os
import subprocess
import optparse
import stat
import errno
from multiprocessing.pool import ThreadPool


#Factory classes and Base Classes

class ConnectionInfo:
	attributes = dict()

	def __init__(self, connectiontype, host, port, username, password=None, attributes=None):
		self.connectiontype = connectiontype
		self.host = host
		self.port = port
		self.username = username
		self.password = password
		self.attributes = attributes

	def getConnection(self):
		if (self.connectiontype == 'sftp'):
			return SFTPConnection(self)

class Connection:
	def __init__(self, obj):
		self.connectionInfo = obj

	def connect(self):
		raise NotImplementedError("connect method is not defined")

	def close(self):
		pass
		#method to close connection

class Reader:
	def __init__(self, obj):
		self.connectionObj = obj
		self.connectionInfo = obj.connectionInfo

	def createPartitionReader(self):
		if (self.connectionInfo.connectiontype == 'sftp'):
			sftpPartitionReader = SFTPPartitionReader()
			sftpPartitionReader.transport = self.connectionObj.transport
			sftpPartitionReader.connectionInfo = self.connectionInfo
			return sftpPartitionReader

class PartitionReader:
	def read(self, datablock):
		raise NotImplementedError("read method is not definded")

class Writer:
	def __init__(self, obj):
		self.connectionObj = obj
		self.connectionInfo = obj.connectionInfo

	def createPartitionWriter(self):
		if (self.connectionInfo.connectiontype == 'sftp'):
			sftpPartitionWriter = SFTPPartitionWriter()
			sftpPartitionWriter.transport = self.connectionObj.transport
			sftpPartitionWriter.connectionInfo = self.connectionInfo
			return sftpPartitionWriter

class PartitionWriter:
	def write(self, datablock):
		raise NotImplementedError("write method is not defined")


#SFTP Classes

class SFTPConnection(Connection):
	def connect(self):
		self.transport = paramiko.Transport((self.connectionInfo.host, self.connectionInfo.port))
		if self.connectionInfo.password is not None:
			self.transport.connect(username = self.connectionInfo.username, password = self.connectionInfo.password)
		else:
			if self.connectionInfo.attributes['privatekeyfile'] is None:
				key_path = os.path.expanduser('~/.ssh/id_rsa')
			else:
				key_path = os.path.expanduser(self.connectionInfo.attributes['privatekeyfile'])
			mykey = paramiko.RSAKey.from_private_key_file(key_path)
			self.transport.connect(username = self.connectionInfo.username, pkey = mykey)
						
	def close(self):
		self.transport.close()

class SFTPPartitionReader(PartitionReader):
	def read(self, sftppath, localPath = None, numParallelConnections = 1):
		if localPath is None:
			localPath = os.getcwd() # local path - can be changed later
		sftp = paramiko.SFTPClient.from_transport(self.transport)
		if (numParallelConnections > 1):
			pool = ThreadPool(numParallelConnections)

		def getFile(sftppath, localpath):
			pconnection = SFTPConnection(self.connectionInfo)
			pconnection.connect()
			psftp = paramiko.SFTPClient.from_transport(pconnection.transport)
			psftp.get(sftppath, localpath)
			psftp.close()
			pconnection.close()

		def recursiveRead(sftp, sftppath, localPath):
			fileattr = sftp.lstat(sftppath)
			if not stat.S_ISDIR(fileattr.st_mode): #it is a file
				if (numParallelConnections > 1):
					pool.apply_async(getFile, args= (sftppath, os.path.join(localPath, os.path.basename(sftppath))))
				else:
					sftp.get(sftppath, os.path.join(localPath, os.path.basename(sftppath)))
			else: #it is a directory
				try: #creating local directory, using try-catch to handle race conditions
					os.makedirs(os.path.join(localPath, os.path.basename(sftppath)))
				except OSError as exception:
					if exception.errno != errno.EEXIST:
						raise
				for file in sftp.listdir_attr(sftppath):
					recursiveRead(sftp, os.path.join(sftppath, file.filename), os.path.join(localPath, os.path.basename(sftppath)))
		recursiveRead(sftp, sftppath, localPath)
		sftp.close()
		if (numParallelConnections > 1):
			pool.close()
			pool.join()

class SFTPPartitionWriter(PartitionWriter):
	def write(self, localPath, sftppath = None, numParallelConnections = 1):
		localPath = os.path.expanduser(localPath)
		print 'Copying contents from ' + localPath + ' to ' + sftppath 
		if sftppath is None:
			sftppath = './'
		sftp = paramiko.SFTPClient.from_transport(self.transport)
		if (numParallelConnections > 1):
			pool = ThreadPool(numParallelConnections)

		def putFile(localpath, sftppath):
			pconnection = SFTPConnection(self.connectionInfo)
			pconnection.connect()
			psftp = paramiko.SFTPClient.from_transport(pconnection.transport)
			psftp.put(localpath, sftppath)
			psftp.close()
			pconnection.close()

		def recursiveWrite(sftp, localPath, sftppath):
			if os.path.isfile(localPath): # if given path is a file
				if (numParallelConnections > 1):
					pool.apply_async(putFile, args=(localPath, os.path.join(sftppath, os.path.basename(localPath))))
				else:
					sftp.put(localPath, os.path.join(sftppath, os.path.basename(localPath)))
			else:
				print sftppath
				try:
					sftp.listdir(os.path.join(sftppath, os.path.basename(localPath)))  # Test if remote_path exists
				except IOError:
					sftp.mkdir(os.path.join(sftppath, os.path.basename(localPath)))  # Create remote_path
				for file in os.listdir(localPath):
					recursiveWrite(sftp, os.path.join(localPath, file), os.path.join(sftppath, os.path.basename(localPath)))
		recursiveWrite(sftp, localPath, sftppath)
		sftp.close()
		if (numParallelConnections > 1):
			pool.close()
			pool.join()