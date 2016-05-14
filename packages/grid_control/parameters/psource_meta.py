# | Copyright 2012-2016 Karlsruhe Institute of Technology
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

from grid_control import utils
from grid_control.parameters.psource_base import ParameterSource
from grid_control.utils.gc_itertools import ichain
from hpfwk import AbstractError
from python_compat import imap, irange, izip, lfilter, lmap, md5_hex, reduce

def combineSyncResult(a, b, sc_fun = lambda x, y: x or y):
	if a is None:
		return b
	(redo_a, disable_a, sizeChange_a) = a
	(redo_b, disable_b, sizeChange_b) = b
	redo_a.update(redo_b)
	disable_a.update(disable_b)
	return (redo_a, disable_a, sc_fun(sizeChange_a, sizeChange_b))

class ForwardingParameterSource(ParameterSource):
	def __init__(self, psource):
		ParameterSource.__init__(self)
		self._psource = psource

	def getMaxParameters(self):
		return self._psource.getMaxParameters()

	def fillParameterKeys(self, result):
		self._psource.fillParameterKeys(result)

	def fillParameterInfo(self, pNum, result):
		self._psource.fillParameterInfo(pNum, result)

	def resync(self):
		return self._psource.resync()

	def show(self):
		return ParameterSource.show(self) + lmap(lambda x: '\t' + x, self._psource.show())

	def getHash(self):
		return self._psource.getHash()


class RangeParameterSource(ForwardingParameterSource):
	def __init__(self, psource, posStart = None, posEnd = None):
		ForwardingParameterSource.__init__(self, psource)
		self._posStart = utils.QM(posStart is None, 0, posStart)
		self._posEndUser = posEnd
		self._posEnd = utils.QM(self._posEndUser is None, self._psource.getMaxParameters() - 1, self._posEndUser)

	def getMaxParameters(self):
		return self._posEnd - self._posStart + 1

	def getHash(self):
		return md5_hex(self._psource.getHash() + str([self._posStart, self._posEnd]))

	def fillParameterInfo(self, pNum, result):
		self._psource.fillParameterInfo(pNum + self._posStart, result)

	def resync(self):
		(result_redo, result_disable, result_sizeChange) = ParameterSource.resync(self)
		(psource_redo, psource_disable, _) = self._psource.resync() # size change is irrelevant if outside of range
		for pNum in psource_redo:
			if (pNum >= self._posStart) and (pNum <= self._posEnd):
				result_redo.add(pNum - self._posStart)
		for pNum in psource_disable:
			if (pNum >= self._posStart) and (pNum <= self._posEnd):
				result_disable.add(pNum - self._posStart)
		oldPosEnd = self._posEnd
		self._posEnd = utils.QM(self._posEndUser is None, self._psource.getMaxParameters() - 1, self._posEndUser)
		return (result_redo, result_disable, result_sizeChange or (oldPosEnd != self._posEnd))

	def show(self):
		result = ForwardingParameterSource.show()
		result[0] += ' range = (%s, %s)' % (self._posStart, self._posEnd)
		return result
ParameterSource.managerMap['range'] = 'RangeParameterSource'


# Meta processing of parameter psources
class MultiParameterSource(ParameterSource):
	def __init__(self, *psources):
		ParameterSource.__init__(self)
		self._psourceList = psources
		self._psourceMaxList = lmap(lambda p: p.getMaxParameters(), self._psourceList)
		self._maxParameters = self.initMaxParameters()

	# Get local parameter numbers (result) from psource index (pIdx) and subpsource parameter number (pNum)
	def _translateNum(self, pIdx, pNum):
		raise AbstractError

	def getMaxParameters(self):
		return self._maxParameters

	def initMaxParameters(self):
		raise AbstractError

	def fillParameterKeys(self, result):
		for psource in self._psourceList:
			psource.fillParameterKeys(result)

	def resync(self):
		oldMaxParameters = self._maxParameters
		# Perform resync of subsources
		psourceResyncList = lmap(lambda p: p.resync(), self._psourceList)
		# Update max for _translateNum
		self._psourceMaxList = lmap(lambda p: p.getMaxParameters(), self._psourceList)
		self._maxParameters = self.initMaxParameters()
		# translate affected pNums from subsources
		(result_redo, result_disable, dummy) = ParameterSource.resync(self)
		for (idx, psource_resync) in enumerate(psourceResyncList):
			(psource_redo, psource_disable, dummy) = psource_resync
			for pNum in psource_redo:
				result_redo.update(self._translateNum(idx, pNum))
			for pNum in psource_disable:
				result_disable.update(self._translateNum(idx, pNum))
		return (result_redo, result_disable, oldMaxParameters != self._maxParameters)

	def show(self):
		result = ParameterSource.show(self)
		result.extend(imap(lambda x: '\t' + x, ichain(imap(lambda ps: ps.show(), self._psourceList))))
		return result

	def getHash(self):
		return md5_hex(str(lmap(lambda p: str(p.getMaxParameters()) + p.getHash(), self._psourceList)))


# Base class for psources invoking their sub-psources in parallel
class BaseZipParameterSource(MultiParameterSource):
	def fillParameterInfo(self, pNum, result):
		for (psource, maxN) in izip(self._psourceList, self._psourceMaxList):
			if maxN is not None:
				if pNum < maxN:
					psource.fillParameterInfo(pNum, result)
			else:
				psource.fillParameterInfo(pNum, result)

	def resync(self): # Quicker version than the general purpose implementation
		result = ParameterSource.resync(self)
		for psource in self._psourceList:
			result = combineSyncResult(result, psource.resync())
		oldMaxParameters = self._maxParameters
		self._maxParameters = self.initMaxParameters()
		return (result[0], result[1], oldMaxParameters != self._maxParameters)


class ZipShortParameterSource(BaseZipParameterSource):
	def initMaxParameters(self):
		maxN = lfilter(lambda n: n is not None, self._psourceMaxList)
		if len(maxN):
			return min(maxN)

class ZipLongParameterSource(BaseZipParameterSource):
	def initMaxParameters(self):
		maxN = lfilter(lambda n: n is not None, self._psourceMaxList)
		if len(maxN):
			return max(maxN)

	def __repr__(self):
		return 'zip(%s)' % str.join(', ', imap(repr, self._psourceList))
ParameterSource.managerMap['zip'] = 'ZipLongParameterSource'


class ChainParameterSource(MultiParameterSource):
	def initMaxParameters(self):
		self.offsetList = lmap(lambda pIdx: sum(self._psourceMaxList[:pIdx]), irange(len(self._psourceList)))
		return sum(self._psourceMaxList)

	def _translateNum(self, pIdx, pNum):
		return [pNum + self.offsetList[pIdx]]

	def fillParameterInfo(self, pNum, result):
		limit = 0
		for (psource, maxN) in izip(self._psourceList, self._psourceMaxList):
			if pNum < limit + maxN:
				return psource.fillParameterInfo(pNum - limit, result)
			limit += maxN

	def __repr__(self):
		return 'chain(%s)' % str.join(', ', imap(repr, self._psourceList))
ParameterSource.managerMap['chain'] = 'ChainParameterSource'


class RepeatParameterSource(MultiParameterSource):
	def __init__(self, psource, times):
		self._psource = psource
		self.times = times
		MultiParameterSource.__init__(self, psource)

	def initMaxParameters(self):
		self.maxN = self._psource.getMaxParameters()
		if self.maxN is not None:
			return self.times * self.maxN
		return self.times

	def _translateNum(self, pIdx, pNum):
		return lmap(lambda i: pNum + i * self.maxN, irange(self.times))

	def fillParameterInfo(self, pNum, result):
		self._psource.fillParameterInfo(pNum % self.maxN, result)

	def show(self):
		return ParameterSource.show(self) + lmap(lambda x: '\t' + x, self._psource.show())

	def getHash(self):
		return md5_hex(self._psource.getHash() + str(self.times))

	def __repr__(self):
		return 'repeat(%s, %d)' % (repr(self._psource), self.times)
ParameterSource.managerMap['repeat'] = 'RepeatParameterSource'


class CrossParameterSource(MultiParameterSource):
	def initMaxParameters(self):
		self.quickFill = []
		prev = 1
		for (psource, maxN) in izip(self._psourceList, self._psourceMaxList):
			self.quickFill.append((psource, maxN, prev))
			if maxN:
				prev *= maxN
		maxList = lfilter(lambda n: n is not None, self._psourceMaxList)
		if maxList:
			return reduce(lambda a, b: a * b, maxList)

	def _translateNum(self, pIdx, pNum):
		(_, maxN, prev) = self.quickFill[pIdx] # psource irrelevant for pnum translation
		return lfilter(lambda x: int(x / prev) % maxN == pNum, irange(self.getMaxParameters()))

	def fillParameterInfo(self, pNum, result):
		for (psource, maxN, prev) in self.quickFill:
			if maxN:
				psource.fillParameterInfo(int(pNum / prev) % maxN, result)
			else:
				psource.fillParameterInfo(pNum, result)

	def __repr__(self):
		return 'cross(%s)' % str.join(', ', imap(repr, self._psourceList))
ParameterSource.managerMap['cross'] = 'CrossParameterSource'


class ErrorParameterSource(ChainParameterSource):
	def __init__(self, *psources):
		self.rawpsources = psources
		central = lmap(lambda p: RangeParameterSource(p, 0, 0), psources)
		chain = [ZipLongParameterSource(*central)]
		for pidx, p in enumerate(psources):
			if p.getMaxParameters() is not None:
				tmp = list(central)
				tmp[pidx] = RangeParameterSource(psources[pidx], 1, None)
				chain.append(CrossParameterSource(*tmp))
		ChainParameterSource.__init__(self, *chain)

	def fillParameterKeys(self, result):
		for psource in self.rawpsources:
			psource.fillParameterKeys(result)
ParameterSource.managerMap['variation'] = 'ErrorParameterSource'


class CombineParameterSource(ZipLongParameterSource):
	# TODO: combine according to common parameter value
	pass
ParameterSource.managerMap['combine'] = 'CombineParameterSource'
