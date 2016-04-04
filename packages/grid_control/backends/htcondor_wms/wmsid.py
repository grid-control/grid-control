# | Copyright 2014-2016 Karlsruhe Institute of Technology
# |
# | Licensed under the Apache License, Version 2.0 (the "License");
# | you may not use this file except in compliance with the License.
# | You may obtain a copy of the License at
# |
# |     http://www.apache.org/licenses/LICENSE-2.0
# |
# | Unless required by applicable law or agreed to in writing, software
# | distributed under the License is distributed on an "AS IS" BASIS,
# | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# | See the License for the specific language governing permissions and
# | limitations under the License.

# -*- coding: utf-8 -*-

class HTCJobID(object):
	"""
	HTCondorWMS unique job identifier
	
	Links GC and HTC jobs via the respective pseudo-unique identifiers
	  gcJobNum@gcTaskID
	and
	  clusterID.procID@scheddURI
	"""
	__slots__ = ['_infoBlob']
	def __init__(self, gcJobNum = -1, gcTaskID = '', clusterID = -1, procID = -1, scheddURI = '', rawID = '', typed = True):
		if rawID:
			scheddURI, gcTaskID, gcJobNum, clusterID, procID = rawID.rsplit('.',4)
		if typed:
			self._infoBlob = ( int(gcJobNum), str(gcTaskID), str(scheddURI), int(clusterID), int(procID) )
		else:
			self._infoBlob = ( gcJobNum, gcTaskID, scheddURI, clusterID, procID )
		self.gcJobNum = self._infoBlob[0]
		self.gcTaskID = self._infoBlob[1]
		self.scheddURI = self._infoBlob[2]
		self.clusterID = self._infoBlob[3]
		self.procID = self._infoBlob[4]
		self.rawID = '%s.%s.%s.%s.%s' % ( self.scheddURI, self.gcTaskID, self.gcJobNum, self.clusterID, self.procID)
	def __len__(self):
		return 5
	def __getitem__(self, key):
		return self._infoBlob[key]
	def __eq__(self, other):
		return ( isinstance(other, type(self)) and self._infoBlob == other._infoBlob )
	def __ne__(self, other):
		return not self.__eq__(other)
