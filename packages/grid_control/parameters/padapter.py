# | Copyright 2013-2016 Karlsruhe Institute of Technology
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

import os, time, logging
from grid_control import utils
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.parameters.psource_base import ParameterError, ParameterInfo, ParameterMetadata, ParameterSource
from grid_control.utils.activity import Activity
from grid_control.utils.file_objects import ZipFile
from grid_control.utils.parsing import strTimeShort
from hpfwk import APIError
from python_compat import identity, ifilter, imap, irange, ismap, itemgetter, lfilter, lmap, md5, set, sort_inplace, sorted, str2bytes

class ParameterAdapter(ConfigurablePlugin):
	def __init__(self, config, source):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('parameters.adapter')
		self._psrc = source

	def __repr__(self):
		return '%s(%r)' % (self.__class__.__name__, self._psrc)

	def can_finish(self):
		return self._psrc.can_finish()

	def can_submit(self, job_num):
		return self.get_job_content(job_num)[ParameterInfo.ACTIVE]

	def get_job_content(self, job_num, pnum = None):
		if pnum is None:
			pnum = job_num
		if job_num is None:
			raise APIError('Unable to process job number None!')
		result = {ParameterInfo.ACTIVE: True, ParameterInfo.REQS: []}
		result['GC_JOB_ID'] = job_num
		result['GC_PARAM'] = pnum
		self._psrc.fill_parameter_content(pnum, result)
		return utils.filterDict(result, vF = lambda v: v != '')

	def get_job_len(self):
		return self._psrc.get_parameter_len()

	def get_job_metadata(self):
		result = lmap(lambda k: ParameterMetadata(k, untracked=True), ['GC_JOB_ID', 'GC_PARAM'])
		self._psrc.fill_parameter_metadata(result)
		return result

	def get_used_psrc_list(self):
		return self._psrc.get_used_psrc_list()

	def iter_jobs(self):
		for job_num in irange(self.get_job_len() or 0):
			yield self.get_job_content(job_num)

	def resync(self, force = False):
		return self._psrc.resync_psrc()

	def show(self):
		return self._psrc.show_psrc()


class ResyncParameterAdapter(ParameterAdapter):
	def __init__(self, config, source):
		ParameterAdapter.__init__(self, config, source)
		self._psrc_hash = source.get_psrc_hash()
		self._resync_state = ParameterSource.EmptyResyncResult()

	def resync(self, force = False): # Do not overwrite resync results - eg. from external or init trigger
		source_hash = self._psrc.get_psrc_hash()
		if (self._resync_state == ParameterSource.EmptyResyncResult()) and ((source_hash != self._psrc_hash) or force):
			activity = Activity('Syncronizing parameter information')
			t_start = time.time()
			try:
				self._resync_state = self._resync()
			except Exception:
				raise ParameterError('Unable to resync parameters!')
			self._psrc_hash = self._psrc.get_psrc_hash()
			activity.finish()
			self._log.log(logging.INFO, 'Finished resync of parameter source (%s)', strTimeShort(time.time() - t_start))
		result = self._resync_state
		self._resync_state = ParameterSource.EmptyResyncResult()
		return result

	def _resync(self):
		return self._psrc.resync_psrc()


class BasicParameterAdapter(ResyncParameterAdapter):
	def __init__(self, config, source):
		ResyncParameterAdapter.__init__(self, config, source)
		self._can_submit_map = {}

	def can_submit(self, job_num): # Use caching to speed up job manager operations
		if job_num not in self._can_submit_map:
			self._can_submit_map[job_num] = ParameterAdapter.can_submit(self, job_num)
		return self._can_submit_map[job_num]

	def resync(self, force = False):
		result = ResyncParameterAdapter.resync(self, force)
		if result not in (None, ParameterSource.EmptyResyncResult()):
			self._can_submit_map = {} # invalidate cache on changes
		return result

# Parameter parameter adapter that tracks changes in the underlying parameter source

def create_placeholder_psrc(pa_old, pa_new, map_job_num2pnum, pspi_list_missing, result_disable):
	# Construct placeholder parameter source with missing parameter entries and intervention state
	psp_list_missing = []
	missing_pnum_start = pa_new.get_job_len()
	sort_inplace(pspi_list_missing, key = itemgetter('GC_PARAM'))
	for (idx, pspi_missing) in enumerate(pspi_list_missing):
		map_job_num2pnum[pspi_missing['GC_PARAM']] = missing_pnum_start + idx
		psp_missing = pa_old.get_job_content(missing_pnum_start + idx, pspi_missing['GC_PARAM'])
		psp_missing.pop('GC_PARAM')
		if psp_missing[ParameterInfo.ACTIVE]:
			psp_missing[ParameterInfo.ACTIVE] = False
			result_disable.add(missing_pnum_start + idx)
		psp_list_missing.append(psp_missing)
	meta_list_current = lmap(lambda key: key.value, pa_new.get_job_metadata())
	meta_list_missing = lfilter(lambda key: key.value not in meta_list_current, pa_old.get_job_metadata())
	return ParameterSource.create_instance('InternalParameterSource', psp_list_missing, meta_list_missing)


def diff_pspi_list(pa_old, pa_new, result_redo, result_disable):
	map_job_num2pnum = {}
	def handle_same_psp(pspi_list_added, pspi_list_missing, pspi_list_same, pspi_old, pspi_new):
		map_job_num2pnum[pspi_old['GC_PARAM']] = pspi_new['GC_PARAM']
		if not pspi_old[ParameterInfo.ACTIVE] and pspi_new[ParameterInfo.ACTIVE]:
			result_redo.add(pspi_new['GC_PARAM'])
		if pspi_old[ParameterInfo.ACTIVE] and not pspi_new[ParameterInfo.ACTIVE]:
			result_disable.add(pspi_new['GC_PARAM'])
	# pspi_list_changed is ignored, since it is already processed by the change handler above
	(pspi_list_added, pspi_list_missing, _) = utils.DiffLists(
		translate_pa2pspi_list(pa_old), translate_pa2pspi_list(pa_new),
		itemgetter(ParameterInfo.HASH), handle_same_psp)
	return (map_job_num2pnum, pspi_list_added, pspi_list_missing)


def extend_map_job_num2pnum(map_job_num2pnum, job_num_start, pspi_list_added):
	# assign sequential job numbers to the added parameter entries
	sort_inplace(pspi_list_added, key = itemgetter('GC_PARAM'))
	for (pspi_idx, pspi_added) in enumerate(pspi_list_added):
		if job_num_start + pspi_idx != pspi_added['GC_PARAM']:
			map_job_num2pnum[job_num_start + pspi_idx] = pspi_added['GC_PARAM']


def translate_pa2pspi_list(pa): # Reduces parameter adapter output to essential information for diff - faster than keying
	meta_list = sorted(ifilter(lambda k: not k.untracked, pa.get_job_metadata()), key = lambda k: k.value)
	def translate_psp2pspi(psp): # Translates parameter space point into hash
		hash_obj = md5()
		for key in ifilter(lambda k: k.value in psp, meta_list):
			value = str(psp[key.value])
			if value:
				hash_obj.update(str2bytes(key.value))
				hash_obj.update(str2bytes(value))
		return {ParameterInfo.HASH: hash_obj.hexdigest(), 'GC_PARAM': psp['GC_PARAM'],
			ParameterInfo.ACTIVE: psp[ParameterInfo.ACTIVE]}
	for psp in pa.iter_jobs():
		yield translate_psp2pspi(psp)


class TrackedParameterAdapter(BasicParameterAdapter):
	def __init__(self, config, source):
		self._psrc_raw = source
		BasicParameterAdapter.__init__(self, config, source)
		self._map_job_num2pnum = {}
		utils.ensureDirExists(config.getWorkPath(), 'parameter storage directory', ParameterError)
		self._path_job_num2pnum = config.getWorkPath('params.map.gz')
		self._path_params = config.getWorkPath('params.dat.gz')

		# Find out if init should be performed - overrides resync_requested!
		init_requested = config.getState('init', detail = 'parameters')
		init_needed = False
		if not (os.path.exists(self._path_params) and os.path.exists(self._path_job_num2pnum)):
			init_needed = True # Init needed if no parameter log exists
		if init_requested and not init_needed and (source.get_parameter_len() is not None):
			self._log.warning('Re-Initialization will overwrite the current mapping between jobs and parameter/dataset content! This can lead to invalid results!')
			if utils.getUserBool('Do you want to perform a syncronization between the current mapping and the new one to avoid this?', True):
				init_requested = False
		do_init = init_requested or init_needed

		# Find out if resync should be performed
		resync_requested = config.getState('resync', detail = 'parameters')
		config.setState(False, 'resync', detail = 'parameters')
		resync_needed = False
		psrc_hash = self._psrc_raw.get_psrc_hash()
		self._psrc_hash_stored = config.get('parameter hash', psrc_hash, persistent = True)
		if self._psrc_hash_stored != psrc_hash:
			resync_needed = True # Resync needed if parameters have changed
			self._log.info('Parameter hash has changed')
			self._log.debug('\told hash: %s', self._psrc_hash_stored)
			self._log.debug('\tnew hash: %s', psrc_hash)
			config.setState(True, 'init', detail = 'config')
		do_resync = (resync_requested or resync_needed) and not do_init

		if not (do_resync or do_init): # Reuse old mapping
			activity = Activity('Loading cached parameter information')
			self._read_job_num2pnum()
			activity.finish()
			return
		elif do_resync: # Perform sync
			self._psrc_hash_stored = None
			self._resync_state = self.resync(force = True)
		elif do_init: # Write current state
			self._write_job_num2pnum(self._path_job_num2pnum)
			ParameterSource.getClass('GCDumpParameterSource').write(self._path_params,
				self.get_job_len(), self.get_job_metadata(), self.iter_jobs())
		config.set('parameter hash', self._psrc_raw.get_psrc_hash())

	def get_job_content(self, job_num, pnum = None): # Perform mapping between job_num and parameter number
		pnum = self._map_job_num2pnum.get(job_num, job_num)
		if (self._psrc.get_parameter_len() is None) or (pnum < self._psrc.get_parameter_len()):
			result = BasicParameterAdapter.get_job_content(self, job_num, pnum)
		else:
			result = {ParameterInfo.ACTIVE: False}
		result['GC_JOB_ID'] = job_num
		return result

	def _read_job_num2pnum(self):
		fp = ZipFile(self._path_job_num2pnum, 'r')
		try:
			int(fp.readline()) # max number of jobs
			map_info = ifilter(identity, imap(str.strip, fp.readline().split(',')))
			self._map_job_num2pnum = dict(imap(lambda x: tuple(imap(lambda y: int(y.lstrip('!')), x.split(':'))), map_info))
			self._can_submit_map = {}
		finally:
			fp.close()

	def _resync(self): # This function is _VERY_ time critical!
		tmp = self._psrc_raw.resync_psrc() # First ask about psrc changes
		(result_redo, result_disable, size_change) = (set(tmp[0]), set(tmp[1]), tmp[2])
		psrc_hash_new = self._psrc_raw.get_psrc_hash()
		psrc_hash_changed = self._psrc_hash_stored != psrc_hash_new
		self._psrc_hash_stored = psrc_hash_new
		if not (result_redo or result_disable or size_change or psrc_hash_changed):
			return ParameterSource.EmptyResyncResult()

		pa_old = ParameterAdapter(None, ParameterSource.create_instance('GCDumpParameterSource', self._path_params))
		pa_new = ParameterAdapter(None, self._psrc_raw)

		(map_job_num2pnum, pspi_list_added, pspi_list_missing) = diff_pspi_list(pa_old, pa_new, result_redo, result_disable)
		# Reorder and reconstruct parameter space with the following layout:
		# NNNNNNNNNNNNN OOOOOOOOO | source: NEW (==self) and OLD (==from file)
		# <same><added> <missing> | same: both in NEW and OLD, added: only in NEW, missing: only in OLD
		if pspi_list_added:
			extend_map_job_num2pnum(map_job_num2pnum, pa_old.get_job_len(), pspi_list_added)
		if pspi_list_missing: # extend the parameter source by placeholders for the missing parameter space points
			psrc_missing = create_placeholder_psrc(pa_old, pa_new, map_job_num2pnum, pspi_list_missing, result_disable)
			self._psrc = ParameterSource.create_instance('ChainParameterSource', self._psrc_raw, psrc_missing)

		self._map_job_num2pnum = map_job_num2pnum # Update Job2PID map
		# Write resynced state
		self._write_job_num2pnum(self._path_job_num2pnum + '.tmp')
		ParameterSource.getClass('GCDumpParameterSource').write(self._path_params + '.tmp',
			self.get_job_len(), self.get_job_metadata(), self.iter_jobs())
		os.rename(self._path_job_num2pnum + '.tmp', self._path_job_num2pnum)
		os.rename(self._path_params + '.tmp', self._path_params)

		result_redo = result_redo.difference(result_disable)
		if result_redo or result_disable:
			map_pnum2job_num = dict(ismap(utils.swap, self._map_job_num2pnum.items()))
			translate = lambda pNum: map_pnum2job_num.get(pNum, pNum)
			return (set(imap(translate, result_redo)), set(imap(translate, result_disable)), size_change)
		return (set(), set(), size_change)

	def _write_job_num2pnum(self, fn):
		fp = ZipFile(fn, 'w')
		try:
			fp.write('%d\n' % (self._psrc_raw.get_parameter_len() or 0))
			data = ifilter(lambda job_num_pnum: job_num_pnum[0] != job_num_pnum[1], self._map_job_num2pnum.items())
			datastr = lmap(lambda job_num_pnum: '%d:%d' % job_num_pnum, data)
			fp.write('%s\n' % str.join(',', datastr))
		finally:
			fp.close()
