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

from grid_control.parameters.psource_base import NullParameterSource, ParameterError, ParameterSource
from hpfwk import AbstractError, Plugin
from python_compat import all, ichain, imap, irange, izip, lchain, lfilter, lmap, md5_hex, reduce

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

	def canFinish(self):
		return self._psource.canFinish()

	def resync(self):
		return self._psource.resync()

	def show(self):
		return ParameterSource.show(self) + lmap(lambda x: '\t' + x, self._psource.show())

	def getUsedSources(self):
		return [self] + self._psource.getUsedSources()

	def getHash(self):
		return self._psource.getHash()


class SubSpaceParameterSource(ForwardingParameterSource):
	alias = ['pspace']

	def __init__(self, name, factory):
		(self._name, self._factory) = (name, factory)
		ForwardingParameterSource.__init__(self, factory.getSource())

	def __repr__(self):
		if self._factory.__class__.__name__ == 'SimpleParameterFactory':
			return 'pspace(%r)' % self._name
		return 'pspace(%r, %r)' % (self._name, self._factory.__class__.__name__)

	def show(self):
		return ['%s: name = %s, factory = %s' % (self.__class__.__name__, self._name, self._factory.__class__.__name__)] +\
			lmap(lambda x: '\t' + x, self._psource.show())

	def create(cls, pconfig, repository, name = 'subspace', factory = 'SimpleParameterFactory'): # pylint:disable=arguments-differ
		try:
			ParameterFactory = Plugin.getClass('ParameterFactory')
			config = pconfig.getConfig(viewClass = 'SimpleConfigView', addSections = [name])
			return SubSpaceParameterSource(name, ParameterFactory.createInstance(factory, config, repository))
		except:
			raise ParameterError('Unable to create subspace %r using factory %r' % (name, factory))
	create = classmethod(create)


class RangeParameterSource(ForwardingParameterSource):
	alias = ['range']

	def __init__(self, psource, posStart = None, posEnd = None):
		ForwardingParameterSource.__init__(self, psource)
		(self._posStart, self._posEndUser) = (posStart or 0, posEnd)
		self._posEnd = self._getPosEnd()

	def _getPosEnd(self):
		if self._posEndUser is None:
			return self._psource.getMaxParameters() - 1
		return self._posEndUser

	def getMaxParameters(self):
		return self._posEnd - self._posStart + 1

	def getHash(self):
		return md5_hex(self._psource.getHash() + str([self._posStart, self._posEnd]))

	def fillParameterInfo(self, pNum, result):
		self._psource.fillParameterInfo(pNum + self._posStart, result)

	def resync(self):
		(result_redo, result_disable, _) = ParameterSource.EmptyResyncResult() # empty resync result
		(psource_redo, psource_disable, _) = self._psource.resync() # size change is irrelevant if outside of range
		def translate(source, target):
			for pNum in source:
				if (pNum >= self._posStart) and (pNum <= self._posEnd):
					target.add(pNum - self._posStart)
		translate(psource_redo, result_redo)
		translate(psource_disable, result_disable)
		oldPosEnd = self._posEnd
		self._posEnd = self._getPosEnd()
		return (result_redo, result_disable, oldPosEnd != self._posEnd)

	def show(self):
		result = ForwardingParameterSource.show(self)
		result[0] += ' range = (%s, %s)' % (self._posStart, self._posEnd)
		return result


def strip_null_sources(psources):
	return lfilter(lambda p: not isinstance(p, NullParameterSource), psources)


# Meta processing of parameter psources
class MultiParameterSource(ParameterSource):
	def __new__(cls, *psources):
		psources = strip_null_sources(psources)
		if len(psources) == 1:
			return psources[0]
		elif not psources:
			return NullParameterSource()
		return ParameterSource.__new__(cls)

	def __init__(self, *psources):
		ParameterSource.__init__(self)
		self._psourceList = strip_null_sources(psources)
		self._psourceMaxList = lmap(lambda p: p.getMaxParameters(), self._psourceList)
		self._maxParameters = self._initMaxParameters()

	def getUsedSources(self):
		return [self] + lchain(imap(lambda ps: ps.getUsedSources(), self._psourceList))

	def canFinish(self):
		return all(imap(lambda p: p.canFinish(), self._psourceList))

	def getInputSources(self):
		return list(self._psourceList)

	# Get local parameter numbers (result) from psource index (pIdx) and subpsource parameter number (pNum)
	def _translateNum(self, pIdx, pNum):
		raise AbstractError

	def getMaxParameters(self):
		return self._maxParameters

	def _initMaxParameters(self):
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
		self._maxParameters = self._initMaxParameters()
		# translate affected pNums from subsources
		(result_redo, result_disable, _) = ParameterSource.EmptyResyncResult()
		for (idx, psource_resync) in enumerate(psourceResyncList):
			(psource_redo, psource_disable, _) = psource_resync
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


def simplify_nested_sources(cls, psources):
	result = []
	for ps in psources:
		if isinstance(ps, cls):
			result.extend(ps.getInputSources())
		else:
			result.append(ps)
	return result


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
		result = ParameterSource.EmptyResyncResult()
		for psource in self._psourceList:
			result = combineSyncResult(result, psource.resync())
		oldMaxParameters = self._maxParameters
		self._psourceMaxList = lmap(lambda p: p.getMaxParameters(), self._psourceList)
		self._maxParameters = self._initMaxParameters()
		return (result[0], result[1], oldMaxParameters != self._maxParameters)


class ZipShortParameterSource(BaseZipParameterSource):
	def _initMaxParameters(self):
		maxN = lfilter(lambda n: n is not None, self._psourceMaxList)
		if len(maxN):
			return min(maxN)


class ZipLongParameterSource(BaseZipParameterSource):
	alias = ['zip']

	def __init__(self, *psources):
		BaseZipParameterSource.__init__(self, *simplify_nested_sources(ZipLongParameterSource, psources))

	def _initMaxParameters(self):
		maxN = lfilter(lambda n: n is not None, self._psourceMaxList)
		if len(maxN):
			return max(maxN)

	def __repr__(self):
		return 'zip(%s)' % str.join(', ', imap(repr, self._psourceList))


class RepeatParameterSource(MultiParameterSource):
	alias = ['repeat']

	def __new__(cls, psource, times): # pylint:disable=arguments-differ
		if times == 0:
			return NullParameterSource()
		elif times == 1:
			return psource
		return MultiParameterSource.__new__(cls, psource, psource) # suppress simplification in MultiparameterSource.__new__

	def __init__(self, psource, times):
		self._psource = psource
		self._times = times
		MultiParameterSource.__init__(self, psource)

	def _initMaxParameters(self):
		self.maxN = self._psource.getMaxParameters()
		if self.maxN is not None:
			return self._times * self.maxN
		return self._times

	def _translateNum(self, pIdx, pNum):
		return lmap(lambda i: pNum + i * self.maxN, irange(self._times))

	def fillParameterInfo(self, pNum, result):
		self._psource.fillParameterInfo(pNum % self.maxN, result)

	def show(self):
		result = ParameterSource.show(self)
		result[0] += ' count = %d' % self._times
		return result + lmap(lambda x: '\t' + x, self._psource.show())

	def getHash(self):
		return md5_hex(self._psource.getHash() + str(self._times))

	def __repr__(self):
		return 'repeat(%s, %d)' % (repr(self._psource), self._times)


class CrossParameterSource(MultiParameterSource):
	alias = ['cross']

	def __new__(cls, *psources):
		psources = strip_null_sources(psources)
		if len(lfilter(lambda p: p.getMaxParameters() is not None, psources)) < 2:
			return ZipLongParameterSource(*psources)
		return MultiParameterSource.__new__(cls, *psources)

	def __init__(self, *psources):
		MultiParameterSource.__init__(self, *simplify_nested_sources(CrossParameterSource, psources))

	def _initMaxParameters(self):
		self._quickFill = []
		prev = 1
		for (psource, maxN) in izip(self._psourceList, self._psourceMaxList):
			self._quickFill.append((psource, maxN, prev))
			if maxN:
				prev *= maxN
		maxList = lfilter(lambda n: n is not None, self._psourceMaxList)
		if maxList:
			return reduce(lambda a, b: a * b, maxList)

	def _translateNum(self, pIdx, pNum):
		(_, maxN, prev) = self._quickFill[pIdx] # psource irrelevant for pnum translation
		return lfilter(lambda x: int(x / prev) % maxN == pNum, irange(self.getMaxParameters()))

	def fillParameterInfo(self, pNum, result):
		for (psource, maxN, prev) in self._quickFill:
			if maxN:
				psource.fillParameterInfo(int(pNum / prev) % maxN, result)
			else:
				psource.fillParameterInfo(pNum, result)

	def __repr__(self):
		return 'cross(%s)' % str.join(', ', imap(repr, self._psourceList))


class ChainParameterSource(MultiParameterSource):
	alias = ['chain']

	def _initMaxParameters(self):
		if None in self._psourceMaxList:
			prob_sources = lfilter(lambda p: p.getMaxParameters() is None, self._psourceList)
			raise ParameterError('Unable to chain unlimited sources: %s' % repr(str.join(', ', imap(repr, prob_sources))))
		self._offsetList = lmap(lambda pIdx: sum(self._psourceMaxList[:pIdx]), irange(len(self._psourceList)))
		return sum(self._psourceMaxList)

	def _translateNum(self, pIdx, pNum):
		return [pNum + self._offsetList[pIdx]]

	def fillParameterInfo(self, pNum, result):
		limit = 0
		for (psource, maxN) in izip(self._psourceList, self._psourceMaxList):
			if pNum < limit + maxN:
				return psource.fillParameterInfo(pNum - limit, result)
			limit += maxN

	def __repr__(self):
		return 'chain(%s)' % str.join(', ', imap(repr, self._psourceList))


class ErrorParameterSource(ChainParameterSource):
	alias = ['variation']

	def __init__(self, *psources):
		psources = strip_null_sources(psources)
		self._rawpsources = psources
		central = lmap(lambda p: RangeParameterSource(p, 0, 0), psources)
		chain = [ZipLongParameterSource(*central)]
		for pidx, p in enumerate(psources):
			if p.getMaxParameters() is not None:
				tmp = list(central)
				tmp[pidx] = RangeParameterSource(psources[pidx], 1, None)
				chain.append(CrossParameterSource(*tmp))
		ChainParameterSource.__init__(self, *chain)

	def fillParameterKeys(self, result):
		for psource in self._rawpsources:
			psource.fillParameterKeys(result)
