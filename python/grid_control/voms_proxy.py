import os, popen2
from grid_control import *

class VomsProxy(Proxy):
	def __init__(self):
		proc = popen2.Popen3('voms-proxy-info', True)
		lines = proc.fromchild.readlines()
		retCode = proc.wait()

		if retCode != 0:
			raise InstallationError("voms-proxy-info failed")

		self.data = {}
		for line in lines:
			try:
				# split at first occurence of ':'
				# and strip spaces around
				key, value = map(lambda x: x.strip(), 
				                 line.split(':', 1))
			except:
				# in case no ':' was found
				continue

			self.data[key.lower()] = value


	def get(self, key):
		return self.data['key']


	def timeleft(self):
		# split ##:##:## into [##, ##, ##] and convert to integers
		timeleft = map(int, self.data['timeleft'].split(':'))
		# multiply from left with 60 and add right component
		# result is in seconds
		return reduce(lambda x, y: x * 60 + y, timeleft)
