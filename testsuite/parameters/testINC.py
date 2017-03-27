from testfwk import create_config, str_dict_testsuite
from grid_control.backends import WMS
from grid_control.datasets import DataProvider
from grid_control.parameters import ParameterInfo, ParameterMetadata, ParameterSource
from grid_control.parameters.padapter import ParameterAdapter
from testDS import modDS
from python_compat import identity, ifilter, imap, irange, lfilter, lmap, set, sorted


SimpleParameterSource = ParameterSource.get_class('SimpleParameterSource')
class TestParameterSource(SimpleParameterSource):
	def __init__(self, key, values, sync):
		SimpleParameterSource.__init__(self, key, values)
		self._resyncResult = sync

	def get_resync_request(self):
		return [self]

	def resync_psrc(self):
		return self._resyncResult


def orderedKeys(keys, showJob = True, showPNum = True):
	result = lfilter(lambda k: k.value not in ['GC_JOB_ID', 'GC_PARAM'], sorted(keys, key = lambda k: k.value))
	if showJob:
		result.extend(ifilter(lambda k: k.value in ['GC_JOB_ID'], sorted(keys, key = lambda k: k.value)))
	if showPNum:
		result.extend(ifilter(lambda k: k.value in ['GC_PARAM'], sorted(keys, key = lambda k: k.value)))
	return result

def printJobInfo(ps, idx, keys = None, showJobPrefix = True):
	msg = ''
	if showJobPrefix:
		msg = '%d ' % idx
	info = ps.get_job_content(idx)

	def replace_key(old_key, new_key, format = identity):
		if old_key in info:
			info[new_key.value] = format(info.pop(old_key))
		if keys and (old_key in keys):
			keys.remove(old_key)
			keys.insert(0, new_key)
	def req_format(value):
		if value is not None:
			return lmap(lambda kv: (WMS.enum2str(kv[0], kv[0]), kv[1]), value)
	replace_key(ParameterInfo.REQS, ParameterMetadata('<REQS>'), req_format)
	replace_key(ParameterInfo.HASH, ParameterMetadata('<HASH>'))
	replace_key(ParameterInfo.ACTIVE, ParameterMetadata('<ACTIVE>'))
	untracked_dict = dict(imap(lambda k: (k.value, k.untracked), keys))
	for key in dict(info):
		if untracked_dict.get(key, True):
			info['!' + key] = info.pop(key)
	new_keys = []
	for key in keys:
		if key.untracked:
			new_keys.append('!' + key.value)
		else:
			new_keys.append(key.value)
	return msg + str_dict_testsuite(info, new_keys)

def testPA(pa, showJob = True, showPNum = True, showMetadata = True, showJobPrefix = True,
		showKeys = True, showUntracked = True, showIV = True, manualKeys = None, newlineEvery = 1):
	iv = pa.resync()
	print(pa.get_job_len())

	keys = orderedKeys(pa.get_job_metadata(), showJob, showPNum)

	def _format_key(key):
		if key.untracked:
			return key.value
		return '%s [trk]' % key.value

	if showKeys:
		print('Keys = %s' % str.join(', ', imap(_format_key, keys)))
	if not showUntracked:
		keys = lfilter(lambda k: not k.untracked, keys)
	if showMetadata:
		keys = [ParameterInfo.ACTIVE, ParameterInfo.HASH, ParameterInfo.REQS] + keys
	if manualKeys != None:
		keys = sorted(ifilter(lambda k: k.lower() in imap(str.lower, manualKeys), pa.get_job_metadata()))

	if pa.get_job_len() == None:
		print(printJobInfo(pa, 1, keys, showJobPrefix = showJobPrefix))
		print(printJobInfo(pa, 11, keys, showJobPrefix = showJobPrefix))
	else:
		msg = []
		for jobnum in irange(pa.get_job_len()):
			msg.append(printJobInfo(pa, jobnum, keys, showJobPrefix = showJobPrefix))
			if jobnum % newlineEvery == (newlineEvery - 1):
				msg.append('\n')
			else:
				msg.append(' ')
		print(str.join('', msg).rstrip() or '<no parameter space points>')

	if showIV:
		if iv != None:
			print('redo: %s disable: %s size: %s' % (sorted(iv[0]), sorted(iv[1]), iv[2]))
		else:
			print(str(iv))

def display_ps_str2ps_str(ps_str):
	for surrogate in ['ZIP', 'RANGE']:
		ps_str = ps_str.replace(surrogate, surrogate.lower())
	return ps_str

def ps_str2display_ps_str(ps_str):
	for surrogate in ['ZIP', 'RANGE']:
		ps_str = ps_str.replace(surrogate.lower(), surrogate)
	return ps_str

def norm_ps_display(ps):
	print(ps_str2display_ps_str(repr(ps)))

def testPS(ps, showJob = False, showHash = True, showPS = True):
	if showPS:
		norm_ps_display(ps)
	if showHash:
		print(ps.get_psrc_hash() or '<no hash>')
	pa = ParameterAdapter(create_config(), ps)
	testPA(pa, showJob = showJob)

def updateDS(data_raw, modstr):
	DataProvider.save_to_file('dataset.tmp', modDS(data_raw, modstr))
