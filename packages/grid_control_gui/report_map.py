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

try:
	import matplotlib
	import matplotlib.pyplot
	from mpl_toolkits.basemap import Basemap
except ImportError:
	matplotlib = None
	BaseMap = None  # pylint:disable=invalid-name
try:
	import numpy
except ImportError:
	numpy = None
import os, math, random
from grid_control_gui.geodb import get_geo_match
from python_compat import ifilter, imap, irange, lfilter, lmap, lzip, sorted


def draw_map(report):
	test_entries = {'unl.edu': [276, 0, 246, 0], 'desy.de': [107, 0, 0, 0],
		'fnal.gov': [16, 0, 294, 0], 'N/A': [0, 0, 0, 0]}
	entries = _get_site_status(report) or test_entries
	pos_list = _get_positions(entries)

	test_bounds = [(-60, -120), (60, 120)]
	test_bounds = [(30, -10), (60, 40)]
	bounds = _get_bounds(pos_list, margin=10) or test_bounds

	matplotlib.pyplot.subplots_adjust(left=0, right=1, top=1, bottom=0, wspace=0, hspace=0)
	fig = matplotlib.pyplot.figure(figsize=(12, 6))
	axis = matplotlib.pyplot.subplot(111)
	base_map = Basemap(projection='cyl', lat_0=0, lon_0=0,
		llcrnrlon=bounds[0][0], llcrnrlat=bounds[0][1],
		urcrnrlon=bounds[1][0], urcrnrlat=bounds[1][1])

	_map_positions(base_map, pos_list)
	if False:
		pos_list = _remove_all_overlap(pos_list)

	base_map.bluemarble()
	for pos in pos_list:
		_draw_pie(axis, pos['info'], (pos['x'], pos['y']), pos['size'])
		axis.text(pos['x'] + 5, pos['y'] + 5, pos['site'], color='white', fontsize=8)
	fig.savefig(os.path.expanduser('~/map.png'), dpi=300)


def _draw_pie(axis, breakdown, pos, size, piecolor=None):
	piecolor = piecolor or ['red', 'orange', 'green', 'blue', 'purple']
	breakdown = [0] + list(numpy.cumsum(breakdown) * 1.0 / sum(breakdown))
	for idx in irange(len(breakdown) - 1):
		fracs = numpy.linspace(2 * math.pi * breakdown[idx], 2 * math.pi * breakdown[idx + 1], 20)
		loc_x = [0] + numpy.cos(fracs).tolist()
		loc_y = [0] + numpy.sin(fracs).tolist()
		axis.scatter(pos[0], pos[1], marker=(lzip(loc_x, loc_y), 0),
			s=size, facecolor=piecolor[idx % len(piecolor)])


def _get_bounds(pos_list, margin):
	(lon_l, lat_l) = (lon_h, lat_h) = pos_list[0]['pos']
	for pos in pos_list:
		lon, lat = pos['pos']
		lon_l = min(lon_l, lon)
		lon_h = max(lon_h, lon)
		lat_l = min(lat_l, lat)
		lat_h = max(lat_h, lat)
	return [(lon_l - margin, lat_l - margin), (lon_h + margin, lat_h + margin)]


def _get_positions(entries):
	result = []
	for hostname in entries:
		entry = get_geo_match(hostname)
		if not entry:
			continue
		(site, lat, lon) = entry
		stateinfo = entries[hostname]
		weight = math.log(sum(stateinfo)) / math.log(2) + 1
		size = 20 * weight
		result.append({'pos': (lon, lat), 'weight': weight,
			'size': size, 'site': site, 'info': stateinfo})
	return result


def _get_site_status(report):
	siteinfo = report.getWNInfos()
	states = ['FAILED', 'WAITING', 'SUCCESS', 'RUNNING']
	sites = ifilter(lambda x: x not in states, siteinfo)
	return dict(imap(lambda site: (site,
		lmap(lambda state: siteinfo[site][state]['COUNT'], states)), sites))


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
		return _dist_sqr(pos_a, pos_b) < (pos_a['weight'] + pos_b['weight'])**2

	def _dist_sqr(pos_a, pos_b):
		return (pos_a['x'] - pos_b['x'])**2 + (pos_a['y'] - pos_b['y'])**2

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
