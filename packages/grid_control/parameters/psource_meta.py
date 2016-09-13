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

def combine_resync_result(a, b, sc_fun = lambda x, y: x or y):
	if a is None:
		return b
	(redo_a, disable_a, size_change_a) = a
	(redo_b, disable_b, size_change_b) = b
	redo_a.update(redo_b)
	disable_a.update(disable_b)
	return (redo_a, disable_a, sc_fun(size_change_a, size_change_b))


def simplify_nested_sources(cls, psrc_list):
	result = []
	for ps in psrc_list:
		if isinstance(ps, cls):
			result.extend(ps.getInputSources())
		else:
			result.append(ps)
	return result


def strip_null_sources(psrc_list):
	return lfilter(lambda p: not isinstance(p, NullParameterSource), psrc_list)


class MultiParameterSource(ParameterSource): # Meta processing of parameter psrc_list
	def __init__(self, *psrc_list):
		ParameterSource.__init__(self)
		self._psrc_list = strip_null_sources(psrc_list)
		self._psrc_max_list = lmap(lambda p: p.get_parameter_len(), self._psrc_list)
		self._psrc_max = self._init_psrc_max()

	def __new__(cls, *psrc_list):
		psrc_list = strip_null_sources(psrc_list)
		if len(psrc_list) == 1:
			return psrc_list[0]
		elif not psrc_list:
			return NullParameterSource()
		return ParameterSource.__new__(cls)

	def __repr__(self):
		return '%s(%s)' % (self.alias[0], str.join(', ', imap(repr, self._psrc_list)))

	def can_finish(self):
		return all(imap(lambda p: p.can_finish(), self._psrc_list))

	def fill_parameter_metadata(self, result):
		for psrc in self._psrc_list:
			psrc.fill_parameter_metadata(result)

	def get_hash(self):
		return md5_hex(self.__class__.__name__ + str(lmap(lambda p: p.get_hash(), self._psrc_list)))

	def get_parameter_len(self):
		return self._psrc_max

	def get_used_psrc_list(self):
		return [self] + lchain(imap(lambda ps: ps.get_used_psrc_list(), self._psrc_list))

	def getInputSources(self):
		return list(self._psrc_list)

	def resync(self):
		oldMaxParameters = self._psrc_max
		# Perform resync of subsources
		psrcResyncList = lmap(lambda p: p.resync(), self._psrc_list)
		# Update max for _translate_pnum
		self._psrc_max_list = lmap(lambda p: p.get_parameter_len(), self._psrc_list)
		self._psrc_max = self._init_psrc_max()
		# translate affected pNums from subsources
		(result_redo, result_disable, _) = ParameterSource.EmptyResyncResult()
		for (idx, psrc_resync) in enumerate(psrcResyncList):
			(psrc_redo, psrc_disable, _) = psrc_resync
			for pNum in psrc_redo:
				result_redo.update(self._translate_pnum(idx, pNum))
			for pNum in psrc_disable:
				result_disable.update(self._translate_pnum(idx, pNum))
		return (result_redo, result_disable, oldMaxParameters != self._psrc_max)

	def show(self):
		result = ParameterSource.show(self)
		result.extend(imap(lambda x: '\t' + x, ichain(imap(lambda ps: ps.show(), self._psrc_list))))
		return result

	def _init_psrc_max(self):
		raise AbstractError

	def _translate_pnum(self, pIdx, pNum):
		raise AbstractError # Get local parameter numbers (result) from psrc index (pIdx) and subpsrc parameter number (pNum)


class ForwardingParameterSource(ParameterSource):
	def __init__(self, psrc):
		ParameterSource.__init__(self)
		self._psrc = psrc

	def can_finish(self):
		return self._psrc.can_finish()

	def fill_parameter_content(self, pNum, result):
		self._psrc.fill_parameter_content(pNum, result)

	def fill_parameter_metadata(self, result):
		self._psrc.fill_parameter_metadata(result)

	def get_hash(self):
		return self._psrc.get_hash()

	def get_parameter_len(self):
		return self._psrc.get_parameter_len()

	def get_used_psrc_list(self):
		return [self] + self._psrc.get_used_psrc_list()

	def resync(self):
		return self._psrc.resync()

	def show(self):
		return ParameterSource.show(self) + lmap(lambda x: '\t' + x, self._psrc.show())


class BaseZipParameterSource(MultiParameterSource): # Base class for psrc_list invoking their sub-psrc_list in parallel
	def fill_parameter_content(self, pNum, result):
		for (psrc, maxN) in izip(self._psrc_list, self._psrc_max_list):
			if maxN is not None:
				if pNum < maxN:
					psrc.fill_parameter_content(pNum, result)
			else:
				psrc.fill_parameter_content(pNum, result)

	def resync(self): # Quicker version than the general purpose implementation
		result = ParameterSource.EmptyResyncResult()
		for psrc in self._psrc_list:
			result = combine_resync_result(result, psrc.resync())
		oldMaxParameters = self._psrc_max
		self._psrc_max_list = lmap(lambda p: p.get_parameter_len(), self._psrc_list)
		self._psrc_max = self._init_psrc_max()
		return (result[0], result[1], oldMaxParameters != self._psrc_max)


class ChainParameterSource(MultiParameterSource):
	alias = ['chain']

	def fill_parameter_content(self, pNum, result):
		limit = 0
		for (psrc, maxN) in izip(self._psrc_list, self._psrc_max_list):
			if pNum < limit + maxN:
				return psrc.fill_parameter_content(pNum - limit, result)
			limit += maxN

	def _init_psrc_max(self):
		if None in self._psrc_max_list:
			prob_sources = lfilter(lambda p: p.get_parameter_len() is None, self._psrc_list)
			raise ParameterError('Unable to chain unlimited sources: %s' % repr(str.join(', ', imap(repr, prob_sources))))
		self._offset_list = lmap(lambda pIdx: sum(self._psrc_max_list[:pIdx]), irange(len(self._psrc_list)))
		return sum(self._psrc_max_list)

	def _translate_pnum(self, pIdx, pNum):
		return [pNum + self._offset_list[pIdx]]


class CrossParameterSource(MultiParameterSource):
	alias = ['cross']

	def __init__(self, *psrc_list):
		MultiParameterSource.__init__(self, *simplify_nested_sources(CrossParameterSource, psrc_list))

	def __new__(cls, *psrc_list):
		psrc_list = strip_null_sources(psrc_list)
		if len(lfilter(lambda p: p.get_parameter_len() is not None, psrc_list)) < 2:
			return ZipLongParameterSource(*psrc_list)
		return MultiParameterSource.__new__(cls, *psrc_list)

	def fill_parameter_content(self, pNum, result):
		for (psrc, maxN, prev) in self._psrc_info_list:
			if maxN:
				psrc.fill_parameter_content(int(pNum / prev) % maxN, result)
			else:
				psrc.fill_parameter_content(pNum, result)

	def _init_psrc_max(self):
		self._psrc_info_list = []
		prev = 1
		for (psrc, maxN) in izip(self._psrc_list, self._psrc_max_list):
			self._psrc_info_list.append((psrc, maxN, prev))
			if maxN:
				prev *= maxN
		maxList = lfilter(lambda n: n is not None, self._psrc_max_list)
		if maxList:
			return reduce(lambda a, b: a * b, maxList)

	def _translate_pnum(self, pIdx, pNum):
		(_, maxN, prev) = self._psrc_info_list[pIdx] # psrc irrelevant for pnum translation
		return lfilter(lambda x: int(x / prev) % maxN == pNum, irange(self.get_parameter_len()))


class RangeParameterSource(ForwardingParameterSource):
	alias = ['range']

	def __init__(self, psrc, posStart = None, posEnd = None):
		ForwardingParameterSource.__init__(self, psrc)
		(self._pos_start, self._pos_end_user) = (posStart or 0, posEnd)
		self._pos_end = self._getPosEnd()

	def __repr__(self):
		param_list = [str(self._pos_start)]
		if self._pos_end_user is not None:
			param_list.append(str(self._pos_end_user))
		return 'range(%r, %s)' % (self._psrc, str.join(', ', param_list))

	def fill_parameter_content(self, pNum, result):
		self._psrc.fill_parameter_content(pNum + self._pos_start, result)

	def get_hash(self):
		return md5_hex(self._psrc.get_hash() + str([self._pos_start, self._pos_end]))

	def get_parameter_len(self):
		return self._pos_end - self._pos_start + 1

	def resync(self):
		(result_redo, result_disable, _) = ParameterSource.EmptyResyncResult() # empty resync result
		(psrc_redo, psrc_disable, _) = self._psrc.resync() # size change is irrelevant if outside of range
		def translate(source, target):
			for pNum in source:
				if (pNum >= self._pos_start) and (pNum <= self._pos_end):
					target.add(pNum - self._pos_start)
		translate(psrc_redo, result_redo)
		translate(psrc_disable, result_disable)
		oldPosEnd = self._pos_end
		self._pos_end = self._getPosEnd()
		return (result_redo, result_disable, oldPosEnd != self._pos_end)

	def show(self):
		result = ForwardingParameterSource.show(self)
		result[0] += ' range = (%s, %s)' % (self._pos_start, self._pos_end)
		return result

	def _getPosEnd(self):
		if self._pos_end_user is None:
			return self._psrc.get_parameter_len() - 1
		return self._pos_end_user


class RepeatParameterSource(MultiParameterSource):
	alias = ['repeat']

	def __init__(self, psrc, times):
		self._psrc = psrc
		self._times = times
		MultiParameterSource.__init__(self, psrc)

	def __new__(cls, psrc, times): # pylint:disable=arguments-differ
		if times == 0:
			return NullParameterSource()
		elif times == 1:
			return psrc
		return MultiParameterSource.__new__(cls, psrc, psrc) # suppress simplification in MultiparameterSource.__new__

	def __repr__(self):
		return 'repeat(%s, %d)' % (repr(self._psrc), self._times)

	def fill_parameter_content(self, pNum, result):
		self._psrc.fill_parameter_content(pNum % self._psrc_child_max, result)

	def get_hash(self):
		return md5_hex(self._psrc.get_hash() + str(self._times))

	def show(self):
		result = ParameterSource.show(self)
		result[0] += ' count = %d' % self._times
		return result + lmap(lambda x: '\t' + x, self._psrc.show())

	def _init_psrc_max(self):
		self._psrc_child_max = self._psrc.get_parameter_len()
		if self._psrc_child_max is not None:
			return self._times * self._psrc_child_max
		return self._times

	def _translate_pnum(self, pIdx, pNum):
		return lmap(lambda i: pNum + i * self._psrc_child_max, irange(self._times))


class SubSpaceParameterSource(ForwardingParameterSource):
	alias = ['pspace']

	def __init__(self, name, factory, repository):
		(self._name, self._factory_name) = (name, factory.__class__.__name__)
		ForwardingParameterSource.__init__(self, factory.get_source(repository))

	def __repr__(self):
		if self._factory_name == 'SimpleParameterFactory':
			return 'pspace(%r)' % self._name
		return 'pspace(%r, %r)' % (self._name, self._factory_name)

	def create(cls, pconfig, repository, name = 'subspace', factory = 'SimpleParameterFactory'): # pylint:disable=arguments-differ
		try:
			ParameterFactory = Plugin.getClass('ParameterFactory')
			config = pconfig.get_config(viewClass = 'SimpleConfigView', addSections = [name])
			return SubSpaceParameterSource(name, ParameterFactory.createInstance(factory, config), repository)
		except:
			raise ParameterError('Unable to create subspace %r using factory %r' % (name, factory))
	create = classmethod(create)

	def show(self):
		return ['%s: name = %s, factory = %s' % (self.__class__.__name__, self._name, self._factory_name)] +\
			lmap(lambda x: '\t' + x, self._psrc.show())


class ErrorParameterSource(ChainParameterSource):
	alias = ['variation']

	def __init__(self, *psrc_list):
		psrc_list = strip_null_sources(psrc_list)
		self._psrc_list_raw = psrc_list
		central = lmap(lambda p: RangeParameterSource(p, 0, 0), psrc_list)
		chain = [ZipLongParameterSource(*central)]
		for pidx, p in enumerate(psrc_list):
			if p.get_parameter_len() is not None:
				tmp = list(central)
				tmp[pidx] = RangeParameterSource(psrc_list[pidx], 1, None)
				chain.append(CrossParameterSource(*tmp))
		ChainParameterSource.__init__(self, *chain)

	def __repr__(self):
		return 'variation(%s)' % str.join(', ', imap(repr, self._psrc_list_raw))

	def fill_parameter_metadata(self, result):
		for psrc in self._psrc_list_raw:
			psrc.fill_parameter_metadata(result)


class ZipLongParameterSource(BaseZipParameterSource):
	alias = ['zip']

	def __init__(self, *psrc_list):
		BaseZipParameterSource.__init__(self, *simplify_nested_sources(ZipLongParameterSource, psrc_list))

	def _init_psrc_max(self):
		maxN = lfilter(lambda n: n is not None, self._psrc_max_list)
		if len(maxN):
			return max(maxN)


class ZipShortParameterSource(BaseZipParameterSource):
	alias = ['szip']

	def _init_psrc_max(self):
		maxN = lfilter(lambda n: n is not None, self._psrc_max_list)
		if len(maxN):
			return min(maxN)
