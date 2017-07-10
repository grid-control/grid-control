# | Copyright 2013-2017 Karlsruhe Institute of Technology
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
from grid_control.gc_plugin import ConfigurablePlugin
from grid_control.parameters.psource_base import ParameterError, ParameterInfo, ParameterMetadata, ParameterSource  # pylint:disable=line-too-long
from grid_control.utils import ensure_dir_exists
from grid_control.utils.activity import Activity
from grid_control.utils.algos import filter_dict, get_list_difference, reverse_dict
from grid_control.utils.data_structures import make_enum
from grid_control.utils.file_tools import GZipTextFile
from grid_control.utils.parsing import str_time_short
from grid_control.utils.user_interface import UserInputInterface
from hpfwk import APIError
from python_compat import ifilter, iidfilter, imap, irange, itemgetter, lfilter, lmap, md5_hex, set, sort_inplace, sorted  # pylint:disable=line-too-long


# TrackingInfo enum values == fast resync tuple indices
TrackingInfo = make_enum(['ACTIVE', 'HASH', 'pnum'], use_hash=False)  # pylint:disable=invalid-name


class ParameterAdapter(ConfigurablePlugin):
	def __init__(self, config, source):
		ConfigurablePlugin.__init__(self, config)
		self._log = logging.getLogger('parameters.adapter')
		self._psrc = source

	def __repr__(self):
		return '%s(%r)' % (self.__class__.__name__, self._psrc)

	def can_finish(self):
		return self._psrc.can_finish()

	def can_submit(self, jobnum):
		return self.get_job_content(jobnum)[ParameterInfo.ACTIVE]

	def get_job_content(self, jobnum, pnum=None):
		if pnum is None:
			pnum = jobnum
		if jobnum is None:
			raise APIError('Unable to process job number None!')
		result = {ParameterInfo.ACTIVE: True, ParameterInfo.REQS: []}
		result['GC_JOB_ID'] = jobnum
		result['GC_PARAM'] = pnum
		self._psrc.fill_parameter_content(pnum, result)
		return filter_dict(result, value_filter=lambda x: x != '')

	def get_job_len(self):
		return self._psrc.get_parameter_len()

	def get_job_metadata(self):
		result = lmap(lambda k: ParameterMetadata(k, untracked=True), ['GC_JOB_ID', 'GC_PARAM'])
		self._psrc.fill_parameter_metadata(result)
		return result

	def get_used_psrc_list(self):
		return self._psrc.get_used_psrc_list()

	def iter_jobs(self):
		for jobnum in irange(self.get_job_len() or 0):
			yield self.get_job_content(jobnum)

	def resync(self, force=False):
		return self._psrc.resync_psrc()

	def show(self):
		return self._psrc.show_psrc()


class ResyncParameterAdapter(ParameterAdapter):
	def __init__(self, config, source):
		ParameterAdapter.__init__(self, config, source)
		self._psrc_hash = source.get_psrc_hash()
		self._resync_state = ParameterSource.get_empty_resync_result()

	def resync(self, force=False):
		source_hash = self._psrc.get_psrc_hash()
		do_resync = (source_hash != self._psrc_hash) or self._psrc.get_resync_request() or force
		# Do not overwrite resync results - eg. from external or init trigger
		if (self._resync_state == ParameterSource.get_empty_resync_result()) and do_resync:
			activity = Activity('Syncronizing parameter information')
			t_start = time.time()
			try:
				self._resync_state = self._resync()
			except Exception:
				raise ParameterError('Unable to resync parameters!')
			self._psrc_hash = self._psrc.get_psrc_hash()
			activity.finish()
			self._log.log(logging.INFO, 'Finished resync of parameter source (%s)',
				str_time_short(time.time() - t_start))
		result = self._resync_state
		self._resync_state = ParameterSource.get_empty_resync_result()
		return result

	def _resync(self):
		return self._psrc.resync_psrc()


class BasicParameterAdapter(ResyncParameterAdapter):
	alias_list = ['basic']

	def __init__(self, config, source):
		ResyncParameterAdapter.__init__(self, config, source)
		self._can_submit_map = {}

	def can_submit(self, jobnum):  # Use caching to speed up job manager operations
		if jobnum not in self._can_submit_map:
			self._can_submit_map[jobnum] = ParameterAdapter.can_submit(self, jobnum)
		return self._can_submit_map[jobnum]

	def resync(self, force=False):
		result = ResyncParameterAdapter.resync(self, force)
		if result not in (None, ParameterSource.get_empty_resync_result()):
			self._can_submit_map = {}  # invalidate cache on changes
		return result


class TrackedParameterAdapter(BasicParameterAdapter):
	alias_list = ['tracked']

	# Parameter parameter adapter that tracks changes in the underlying parameter source
	def __init__(self, config, source):
		self._psrc_raw = source
		BasicParameterAdapter.__init__(self, config, source)
		self._map_jobnum2pnum = {}
		ensure_dir_exists(config.get_work_path(), 'parameter storage directory', ParameterError)
		self._path_jobnum2pnum = config.get_work_path('params.map.gz')
		self._path_params = config.get_work_path('params.dat.gz')

		# Find out if init should be performed - overrides resync_requested!
		init_requested = config.get_state('init', detail='parameters')
		init_needed = False
		if not (os.path.exists(self._path_params) and os.path.exists(self._path_jobnum2pnum)):
			init_needed = True  # Init needed if no parameter log exists
		if init_requested and not init_needed and (source.get_parameter_len() is not None):
			self._log.warning('Re-Initialization will overwrite the current mapping ' +
				'between jobs and parameter/dataset content! This can lead to invalid results!')
			user_msg = ('Do you want to perform a syncronization between ' +
				'the current mapping and the new one to avoid this?')
			if UserInputInterface().prompt_bool(user_msg, True):
				init_requested = False
		do_init = init_requested or init_needed

		# Find out if resync should be performed
		resync_by_user = config.get_state('resync', detail='parameters')
		config.set_state(False, 'resync', detail='parameters')
		psrc_hash = self._psrc_raw.get_psrc_hash()
		self._psrc_hash_stored = config.get('parameter hash', psrc_hash, persistent=True)
		psrc_hash_changed = self._psrc_hash_stored != psrc_hash  # Resync if parameters have changed
		resync_by_psrc = self._psrc_raw.get_resync_request()

		if do_init:  # Write current state
			self._write_jobnum2pnum(self._path_jobnum2pnum)
			ParameterSource.get_class('GCDumpParameterSource').write(self._path_params,
				self.get_job_len(), self.get_job_metadata(), self.iter_jobs())
		elif resync_by_user or resync_by_psrc or psrc_hash_changed:  # Perform sync
			if psrc_hash_changed:
				self._log.info('Parameter hash has changed')
				self._log.debug('\told hash: %s', self._psrc_hash_stored)
				self._log.debug('\tnew hash: %s', psrc_hash)
				self._log.log(logging.DEBUG1, '\tnew src: %s', self._psrc_raw)
				config.set_state(True, 'init', detail='config')
			elif resync_by_psrc:
				self._log.info('Parameter source requested resync')
				self._log.debug('\t%r', str.join(', ', imap(repr, resync_by_psrc)))
			elif resync_by_user:
				self._log.info('User requested resync')
			self._psrc_hash_stored = None
			self._resync_state = self.resync(force=True)
		else:  # Reuse old mapping
			activity = Activity('Loading cached parameter information')
			self._read_jobnum2pnum()
			activity.finish()
			return  # do not set parameter hash in config
		config.set('parameter hash', self._psrc_raw.get_psrc_hash())

	def get_job_content(self, jobnum, pnum=None):
		# Perform mapping between jobnum and parameter number
		pnum = self._map_jobnum2pnum.get(jobnum, jobnum)
		if (self._psrc.get_parameter_len() is None) or (pnum < self._psrc.get_parameter_len()):
			result = BasicParameterAdapter.get_job_content(self, jobnum, pnum)
		else:
			result = {ParameterInfo.ACTIVE: False}
		result['GC_JOB_ID'] = jobnum
		return result

	def _read_jobnum2pnum(self):
		fp = GZipTextFile(self._path_jobnum2pnum, 'r')
		try:
			def _translate_info(jobnum_pnum_info):
				return tuple(imap(lambda x: int(x.lstrip('!')), jobnum_pnum_info.split(':', 1)))

			int(fp.readline())  # max number of jobs
			jobnum_pnum_info_iter = iidfilter(imap(str.strip, fp.readline().split(',')))
			self._map_jobnum2pnum = dict(imap(_translate_info, jobnum_pnum_info_iter))
			self._can_submit_map = {}
		finally:
			fp.close()

	def _resync(self):  # This function is _VERY_ time critical!
		tmp = self._psrc_raw.resync_psrc()  # First ask about psrc changes
		(result_redo, result_disable, size_change) = (set(tmp[0]), set(tmp[1]), tmp[2])
		psrc_hash_new = self._psrc_raw.get_psrc_hash()
		psrc_hash_changed = self._psrc_hash_stored != psrc_hash_new
		self._psrc_hash_stored = psrc_hash_new
		if not (result_redo or result_disable or size_change or psrc_hash_changed):
			return ParameterSource.get_empty_resync_result()

		ps_old = ParameterSource.create_instance('GCDumpParameterSource', self._path_params)
		pa_old = ParameterAdapter(None, ps_old)
		pa_new = ParameterAdapter(None, self._psrc_raw)
		return self._resync_adapter(pa_old, pa_new, result_redo, result_disable, size_change)

	def _resync_adapter(self, pa_old, pa_new, result_redo, result_disable, size_change):
		(map_jobnum2pnum, pspi_list_added, pspi_list_missing) = _diff_pspi_list(pa_old, pa_new,
			result_redo, result_disable)
		# Reorder and reconstruct parameter space with the following layout:
		# NNNNNNNNNNNNN OOOOOOOOO | source: NEW (==self) and OLD (==from file)
		# <same><added> <missing> | same: both in NEW and OLD, added: only in NEW, missing: only in OLD
		if pspi_list_added:
			_extend_map_jobnum2pnum(map_jobnum2pnum, pa_old.get_job_len(), pspi_list_added)
		if pspi_list_missing:
			# extend the parameter source by placeholders for the missing parameter space points
			psrc_missing = _create_placeholder_psrc(pa_old, pa_new,
				map_jobnum2pnum, pspi_list_missing, result_disable)
			self._psrc = ParameterSource.create_instance('ChainParameterSource',
				self._psrc_raw, psrc_missing)

		self._map_jobnum2pnum = map_jobnum2pnum  # Update Job2PID map
		# Write resynced state
		self._write_jobnum2pnum(self._path_jobnum2pnum + '.tmp')
		ParameterSource.get_class('GCDumpParameterSource').write(self._path_params + '.tmp',
			self.get_job_len(), self.get_job_metadata(), self.iter_jobs())
		os.rename(self._path_jobnum2pnum + '.tmp', self._path_jobnum2pnum)
		os.rename(self._path_params + '.tmp', self._path_params)

		result_redo = result_redo.difference(result_disable)
		if result_redo or result_disable:
			map_pnum2jobnum = reverse_dict(self._map_jobnum2pnum)

			def _translate_pnum(pnum):
				return map_pnum2jobnum.get(pnum, pnum)
			result_redo = set(imap(_translate_pnum, result_redo))
			result_disable = set(imap(_translate_pnum, result_disable))
			return (result_redo, result_disable, size_change)
		return (set(), set(), size_change)

	def _write_jobnum2pnum(self, fn):
		fp = GZipTextFile(fn, 'w')
		try:
			fp.write('%d\n' % (self._psrc_raw.get_parameter_len() or 0))
			data = ifilter(lambda jobnum_pnum: jobnum_pnum[0] != jobnum_pnum[1],
				self._map_jobnum2pnum.items())
			datastr = lmap(lambda jobnum_pnum: '%d:%d' % jobnum_pnum, data)
			fp.write('%s\n' % str.join(',', datastr))
		finally:
			fp.close()


def _create_placeholder_psrc(pa_old, pa_new, map_jobnum2pnum, pspi_list_missing, result_disable):
	# Construct placeholder parameter source with missing parameter entries and intervention state
	psp_list_missing = []
	missing_pnum_start = pa_new.get_job_len()
	sort_inplace(pspi_list_missing, key=itemgetter(TrackingInfo.pnum))
	for (idx, pspi_missing) in enumerate(pspi_list_missing):
		map_jobnum2pnum[pspi_missing[TrackingInfo.pnum]] = missing_pnum_start + idx
		psp_missing = pa_old.get_job_content(missing_pnum_start + idx, pspi_missing[TrackingInfo.pnum])
		psp_missing.pop('GC_PARAM')
		if psp_missing[ParameterInfo.ACTIVE]:
			psp_missing[ParameterInfo.ACTIVE] = False
			result_disable.add(missing_pnum_start + idx)
		psp_list_missing.append(psp_missing)
	meta_list_new = pa_new.get_job_metadata()
	meta_name_list_new = lmap(lambda key: key.value, meta_list_new)
	meta_list_old = pa_old.get_job_metadata()
	meta_list_missing = lfilter(lambda key: key.value not in meta_name_list_new, meta_list_old)
	return ParameterSource.create_instance('InternalParameterSource',
		psp_list_missing, meta_list_missing)


def _diff_pspi_list(pa_old, pa_new, result_redo, result_disable):
	map_jobnum2pnum = {}

	def _handle_matching_pspi(pspi_list_added, pspi_list_missing, pspi_list_same, pspi_old, pspi_new):
		map_jobnum2pnum[pspi_old[TrackingInfo.pnum]] = pspi_new[TrackingInfo.pnum]
		if not pspi_old[TrackingInfo.ACTIVE] and pspi_new[TrackingInfo.ACTIVE]:
			result_redo.add(pspi_new[TrackingInfo.pnum])
		if pspi_old[TrackingInfo.ACTIVE] and not pspi_new[TrackingInfo.ACTIVE]:
			result_disable.add(pspi_new[TrackingInfo.pnum])
	# pspi_list_changed is ignored, since it is already processed by the change handler above
	(pspi_list_added, pspi_list_missing, _) = get_list_difference(
		_translate_pa2pspi_list(pa_old), _translate_pa2pspi_list(pa_new),
		itemgetter(TrackingInfo.HASH), _handle_matching_pspi)
	return (map_jobnum2pnum, pspi_list_added, pspi_list_missing)


def _extend_map_jobnum2pnum(map_jobnum2pnum, jobnum_start, pspi_list_added):
	# assign sequential job numbers to the added parameter entries
	sort_inplace(pspi_list_added, key=itemgetter(TrackingInfo.pnum))
	for (pspi_idx, pspi_added) in enumerate(pspi_list_added):
		if jobnum_start + pspi_idx != pspi_added[TrackingInfo.pnum]:
			map_jobnum2pnum[jobnum_start + pspi_idx] = pspi_added[TrackingInfo.pnum]


def _translate_pa2pspi_list(padapter):
	# Reduces parameter adapter output to essential information for diff - faster than keying
	meta_iter = ifilter(lambda k: not k.untracked, padapter.get_job_metadata())
	meta_list = sorted(meta_iter, key=lambda k: k.value)

	for psp in padapter.iter_jobs():  # Translates parameter space point into hash
		psp_item_iter = imap(lambda meta: (meta.value, psp.get(meta.value)), meta_list)
		hash_str = md5_hex(repr(lfilter(itemgetter(1), psp_item_iter)))
		yield (psp[ParameterInfo.ACTIVE], hash_str, psp['GC_PARAM'])
