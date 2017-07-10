# | Copyright 2016-2017 Karlsruhe Institute of Technology
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

import logging, threading
from grid_control.utils.algos import filter_dict
from grid_control.utils.thread_tools import GCLock, with_lock
from hpfwk import APIError, get_thread_name
from python_compat import imap, rsplit, set


class Activity(object):
	lock = GCLock()
	counter = 0
	running_by_thread_name = {}
	callbacks = []

	def __init__(self, msg=None, level=logging.INFO, name=None, parent=None, fmt='%(msg)s...',
			log=False, logger=None):  # log == None - only at start/finish; log == True - always
		(self._level, self._msg_dict, self._fmt) = (level, {'msg': ''}, fmt)
		(self.name, self._parent, self._children) = (name, None, [])
		(self._log, self._logger) = (log, logger or logging.getLogger())
		if (self._log is not False) and msg:
			self._logger.log(level, msg)
		self._current_thread_name = get_thread_name()

		with_lock(Activity.lock, self._add_activity, parent)
		self.depth = len(list(self.get_parents()))

		if self._parent:
			self._parent.add_child(self)
		self.update(msg)

	def __del__(self):
		self.finish()

	def __repr__(self):
		parent_name = None
		if self._parent:
			parent_name = self._parent.name
		return '%s(name: %r, msg_dict: %r, lvl: %s, depth: %d, parent: %s)' % (
			self.__class__.__name__, self.name, self._msg_dict, self._level, self.depth, parent_name)

	def add_child(self, value):
		self._children.append(value)

	def finish(self):
		for child in list(self._children):
			child.finish()
		if self._parent:
			self._parent.remove_child(self)
		running_list = Activity.running_by_thread_name.get(self._current_thread_name, [])
		if self in running_list:
			running_list.remove(self)
			if self._log is not False:
				self._logger.log(self._level, self.get_msg() + ' finished')

	def get_children(self):
		for child in self._children:
			yield child
			for subchild in child.get_children():
				yield subchild

	def get_msg(self, truncate=None, last=35):
		msg = (self._fmt % self._msg_dict).strip()
		if (truncate is not None) and (len(msg) > truncate):
			msg = msg[:truncate - last - 3] + '...' + msg[-last:]
		return msg

	def get_parents(self):
		if self._parent is not None:
			for parent in self._parent.get_parents():
				yield parent
			yield self._parent

	def remove_child(self, value):
		if value in self._children:
			self._children.remove(value)

	def update(self, msg):
		self._set_msg(msg=msg)

	def _add_activity(self, parent):
		Activity.counter += 1
		# search parent:
		self._cleanup_running()  # cleanup list of running activities
		for parent_candidate in self._iter_possible_parents(self._current_thread_name):
			if (parent is None) or (parent == parent_candidate.name):
				self._parent = parent_candidate
				break
		if (parent is not None) and (self._parent is None):
			raise APIError('Invalid parent given!')
		# set this activity as topmost activity in the current thread
		Activity.running_by_thread_name.setdefault(self._current_thread_name, []).append(self)

	def _cleanup_running(self):
		# clean running activity list
		running_thread_names = set(imap(get_thread_name, threading.enumerate()))
		for thread_name in list(Activity.running_by_thread_name):
			if thread_name not in running_thread_names:
				finished_activities = Activity.running_by_thread_name.get(thread_name, [])
				while finished_activities:
					finished_activities[-1].finish()
				Activity.running_by_thread_name.pop(thread_name, None)

	def _iter_possible_parents(self, current_thread_name):
		# yield activities in current and parent threads
		stack = list(Activity.running_by_thread_name.get(current_thread_name, []))
		stack.reverse()  # in reverse order of creation
		for item in stack:
			yield item
		if '-' in current_thread_name:
			for item in self._iter_possible_parents(rsplit(current_thread_name, '-', 1)[0]):
				yield item

	def _set_msg(self, **kwargs):
		self._msg_dict.update(filter_dict(kwargs, value_filter=lambda value: value is not None))
		for callback in Activity.callbacks:
			callback()
		if self._log:
			self._logger.log(self._level, self.get_msg())
Activity.root = Activity('Running grid-control', name='root')  # <global-state>


class ProgressActivity(Activity):
	def __init__(self, msg=None, progress_max=None, progress=None,
			level=logging.INFO, name=None, parent=None, fmt='%(msg)s %(progress_str)s...'):
		(self._progress, self._progress_max, self._progress_msg) = (progress, progress_max, msg)
		Activity.__init__(self, msg, level, name, parent, fmt)

	def update(self, msg):
		self._set_msg(msg=msg, progress_str=self._get_progress_str() or '')

	def update_progress(self, progress, progress_max=None, msg=None):
		if progress_max is not None:
			self._progress_max = progress_max
		self._progress = progress
		self.update(msg)

	def _get_progress_str(self):
		if self._progress is not None:
			if self._progress_max in (None, 0):
				return '[%d]' % (self._progress + 1)
			return '[%d / %d]' % (self._progress + 1, self._progress_max)
