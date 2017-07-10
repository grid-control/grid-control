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

import math, random
from grid_control.gc_exceptions import InstallationError
from grid_control.job_db import Job, JobClass
from grid_control.report import ImageReport
from grid_control_gui.geodb import get_geo_match
from python_compat import BytesBuffer, imap, irange, lfilter, lmap, lzip, sorted


class MapReport(ImageReport):
	alias_list = ['map']

	def show_report(self, job_db, jobnum_list):
		try:
			import numpy
		except Exception:
			raise InstallationError('numpy is not installed!')
		try:
			from mpl_toolkits.basemap import Basemap
		except Exception:
			raise InstallationError('basemap is not installed!')

		hostname_dict = _get_hostname_dict(job_db, jobnum_list)
		pos_list = _get_positions(hostname_dict)
		bounds = _get_bl_tr(pos_list, margin=10)
		aspect = (bounds[1][0] - bounds[0][0]) / (bounds[1][1] - bounds[0][1])

		buffer = BytesBuffer()
		(fig, axis) = _setup_figure(aspect)
		base_map = Basemap(projection='cyl', lat_0=0, lon_0=0,
			llcrnrlon=bounds[0][0], llcrnrlat=bounds[0][1],
			urcrnrlon=bounds[1][0], urcrnrlat=bounds[1][1])
		_draw_map(numpy, fig, axis, buffer, base_map, pos_list)
		self._show_image('map.png', buffer)
		buffer.close()


def _draw_map(numpy, fig, axis, buffer, base_map, pos_list):
	_map_positions(base_map, pos_list)
	# pos_list = _remove_all_overlap(pos_list)

	# base_map.bluemarble()
	base_map.etopo()
	for pos in pos_list:
		_draw_pie(numpy, axis, pos['info'], (pos['x'], pos['y']), pos['size'])
		axis.text(pos['x'] + 1, pos['y'] + 1, pos['site'], color='white', fontsize=12)
	fig.savefig(buffer, dpi=300, format='png')


def _draw_pie(numpy, axis, js_dict, pos, size, piecolor=None):
	def _sum(job_class):
		return sum(imap(js_dict.get, job_class.state_list))

	piecolor = piecolor or ['red', 'orange', 'green', 'blue', 'purple']
	breakdown = lmap(_sum, [JobClass.FAILING, JobClass.RUNNING,
		JobClass.SUCCESS, JobClass.DONE, JobClass.ATWMS])
	breakdown = [0] + list(numpy.cumsum(breakdown) * 1.0 / sum(breakdown))
	for idx in irange(len(breakdown) - 1):
		fracs = numpy.linspace(2 * math.pi * breakdown[idx], 2 * math.pi * breakdown[idx + 1], 20)
		loc_x = [0] + numpy.cos(fracs).tolist()
		loc_y = [0] + numpy.sin(fracs).tolist()
		axis.scatter(pos[0], pos[1], marker=(lzip(loc_x, loc_y), 0),
			s=size, facecolor=piecolor[idx % len(piecolor)])


def _get_bl_tr(pos_list, margin, aspect_goal=16 / 10.):
	(lon_l, lat_l) = (lon_h, lat_h) = pos_list[0]['pos']
	for pos in pos_list:
		(lon, lat) = pos['pos']
		(lon_l, lon_h) = (min(lon_l, lon), max(lon_h, lon))
		(lat_l, lat_h) = (min(lat_l, lat), max(lat_h, lat))
	(lon_l, lat_l) = (lon_l - margin, lat_l - margin)
	(lon_h, lat_h) = (lon_h + margin, lat_h + margin)
	aspect = (lon_h - lon_l) / (lat_h - lat_l)
	if aspect > aspect_goal:
		dlat = (lon_h - lon_l) / aspect_goal / 2.
		lat_h += dlat
		lat_l -= dlat
	else:
		dlon = (lat_h - lat_l) * aspect_goal / 2
		lon_h += dlon
		lon_l -= dlon
	aspect = (lon_h - lon_l) / (lat_h - lat_l)
	return [(lon_l, max(-85, lat_l)), (lon_h, min(85, lat_h))]


def _get_hostname_dict(job_db, jobnum_list):
	hostname_dict = {}
	for jobnum in jobnum_list:
		job_obj = job_db.get_job_transient(jobnum)
		hostname = job_obj.get('wn')
		hostname_dict.setdefault(hostname, dict.fromkeys(Job.enum_value_list, 0))
		hostname_dict[hostname][job_obj.state] += 1
	return hostname_dict


def _get_positions(hostname_dict):
	result = []
	for hostname in hostname_dict:
		(site, lat, lon) = get_geo_match(hostname) or (hostname, None, None)
		if lat is not None:
			hostname_js_dict = hostname_dict[hostname]
			weight = math.log(sum(hostname_js_dict)) / math.log(2) + 1
			result.append({'pos': (lon, lat), 'weight': weight,
				'size': 100 * weight, 'site': site, 'info': hostname_js_dict})
	return result


def _map_positions(mfun, pos_list):
	for pos in pos_list:
		loc_x, loc_y = mfun(*pos['pos'])
		pos['x'] = loc_x
		pos['y'] = loc_y


def _remove_all_overlap(data):
	def _center_of_mass(data):
		wsum_x = sum(imap(lambda pt: pt['x'] * pt['weight'], data))
		wsum_y = sum(imap(lambda pt: pt['y'] * pt['weight'], data))
		sum_w = sum(imap(lambda pt: pt['weight'], data))
		return {'x': wsum_x / sum_w, 'y': wsum_y / sum_w}

	def _check_overlap(pos_a, pos_b):
		return _dist_sqr(pos_a, pos_b) < (pos_a['weight'] + pos_b['weight']) ** 2

	def _dist_sqr(pos_a, pos_b):
		return (pos_a['x'] - pos_b['x']) ** 2 + (pos_a['y'] - pos_b['y']) ** 2

	def _remove_overlap(fix, pos_a):
		vec = {'x': pos_a['x'] + fix['x'], 'y': pos_a['y'] + fix['y']}
		norm = math.sqrt(_dist_sqr(vec, {'x': 0, 'y': 0})) * 1000
		vec = {'x': vec['x'] / norm, 'y': vec['y'] / norm}
		for pos_ref in result:
			while _check_overlap(pos_ref, pos_a):
				pos_a['x'] = pos_a['x'] + vec['x'] * (random.random() - 0.25)
				pos_a['y'] = pos_a['y'] + vec['y'] * (random.random() - 0.25)
		return pos_a

	result = []
	data = sorted(data, key=lambda x: -x['weight'])
	for pos_ref in data:
		collisions = lfilter(lambda x: _check_overlap(x, pos_ref), result)
		if collisions:
			result.append(_remove_overlap(_center_of_mass(collisions), pos_ref))
		else:
			result.append(pos_ref)
	return result


def _setup_figure(aspect):
	try:
		from matplotlib.pyplot import subplots_adjust, figure, subplot
	except Exception:
		raise InstallationError('matplotlib is not installed!')
	fig = figure(figsize=(3 * aspect, 3))
	axis = subplot(111)
	subplots_adjust(left=0, right=1, top=1, bottom=0, wspace=0, hspace=0)
	return (fig, axis)
