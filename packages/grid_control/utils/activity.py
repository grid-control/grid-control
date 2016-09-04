# | Copyright 2016 Karlsruhe Institute of Technology
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
from grid_control.utils.thread_tools import GCLock
from hpfwk import APIError
from python_compat import get_current_thread, get_thread_name, imap, rsplit, set

class Activity(object):
	def __init__(self, message = None, level = logging.INFO, name = None, parent = None):
		(self.name, self._level, self._message, self._parent, self._children) = (name, level, None, None, [])
		self._current_thread_name = get_thread_name(get_current_thread())

		Activity.lock.acquire()
		try:
			self._id = Activity.counter
			Activity.counter += 1
			# search parent:
			self._cleanup_running() # cleanup list of running activities
			for parent_candidate in self._iter_possible_parents(self._current_thread_name):
				if (parent is None) or (parent == parent_candidate.name):
					self._parent = parent_candidate
					break
			if (parent is not None) and (self._parent is None):
				raise APIError('Invalid parent given!')
			# set this activity as topmost activity in the current thread
			Activity.running_by_thread_name.setdefault(self._current_thread_name, []).append(self)
		finally:
			Activity.lock.release()
		self.depth = len(list(self.get_parents()))

		if message is not None:
			self.update(message)
		if self._parent:
			self._parent.add_child(self)

	def __repr__(self):
		pname = None
		if self._parent:
			pname = self._parent.name
		return '%s(name: %r, msg: %r, lvl: %s, depth: %d, parent: %s)' % (self.__class__.__name__, self.name, self._message, self._level, self.depth, pname)

	def getMessage(self):
		return self._message

	def _iter_possible_parents(self, current_thread_name): # yield activities in current and parent threads
		stack = list(Activity.running_by_thread_name.get(current_thread_name, []))
		stack.reverse() # in reverse order of creation
		for item in stack:
			yield item
		if '-' in current_thread_name:
			for item in self._iter_possible_parents(rsplit(current_thread_name, '-', 1)[0]):
				yield item

	def add_child(self, value):
		self._children.append(value)

	def remove_child(self, value):
		if value in self._children:
			self._children.remove(value)

	def _cleanup_running(self):
		# clean running activity list
		running_thread_names = set(imap(get_thread_name, threading.enumerate()))
		for thread_name in list(Activity.running_by_thread_name):
			if thread_name not in running_thread_names:
				finished_activities = Activity.running_by_thread_name.get(thread_name, [])
				while finished_activities:
					finished_activities[-1].finish()
				Activity.running_by_thread_name.pop(thread_name, None)

	def _set_message(self, message):
		self._message = message
		for cb in Activity.callbacks:
			cb()
		return self

	def update(self, message):
		return self._set_message(message)

	def finish(self):
		for child in list(self._children):
			child.finish()
		if self._parent:
			self._parent.remove_child(self)
		running_list = Activity.running_by_thread_name.get(self._current_thread_name, [])
		if self in running_list:
			running_list.remove(self)

	def __del__(self):
		self.finish()

	def get_parents(self):
		if self._parent is not None:
			for parent in self._parent.get_parents():
				yield parent
			yield self._parent

	def get_children(self):
		for child in self._children:
			yield child
			for subchild in child.get_children():
				yield subchild

Activity.lock = GCLock()
Activity.counter = 0
Activity.running_by_thread_name = {}
Activity.callbacks = []
Activity.root = Activity('Running grid-control', name = 'root')
