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
	BaseMap = None
try:
	import numpy
except ImportError:
	numpy = None
import os, math, random
from grid_control_gui.geodb import getGeoMatch
from python_compat import ifilter, imap, irange, lfilter, lmap, lzip, sorted

def remove_all_overlap(data):
	dist2 = lambda a, b: (a['x'] - b['x'])**2 + (a['y'] - b['y'])**2
	check_overlap = lambda a, b: dist2(a, b) < (a['weight'] + b['weight'])**2
	def remove_overlap(fix, a):
		vec = {'x': a['x'] + fix['x'], 'y': a['y'] + fix['y']}
		norm = math.sqrt(dist2(vec, {'x': 0, 'y': 0})) * 1000
		vec = {'x': vec['x'] / norm, 'y': vec['y'] / norm}
		for pt in result:
			while check_overlap(pt, a):
				a['x'] = a['x'] + vec['x'] * (random.random() - 0.25)
				a['y'] = a['y'] + vec['y'] * (random.random() - 0.25)
		return a
	def center_of_mass(data):
		wsum_x = sum(imap(lambda pt: pt['x']*pt['weight'], data))
		wsum_y = sum(imap(lambda pt: pt['y']*pt['weight'], data))
		sum_w = sum(imap(lambda pt: pt['weight'], data))
		return {'x': wsum_x / sum_w, 'y': wsum_y / sum_w}

	result = []
	data = sorted(data, key = lambda x: -x['weight'])
	for pt in data:
		collisions = lfilter(lambda x: check_overlap(x, pt), result)
		if collisions:
			result.append(remove_overlap(center_of_mass(collisions), pt))
		else:
			result.append(pt)
	return result

def draw_pie(ax, breakdown, pos, size, piecolor = None):
	piecolor = piecolor or ['red', 'orange', 'green', 'blue', 'purple']
	breakdown = [0] + list(numpy.cumsum(breakdown)* 1.0 / sum(breakdown))
	for i in irange(len(breakdown)-1):
		x = [0] + numpy.cos(numpy.linspace(2 * math.pi * breakdown[i], 2 * math.pi * breakdown[i+1], 20)).tolist()
		y = [0] + numpy.sin(numpy.linspace(2 * math.pi * breakdown[i], 2 * math.pi * breakdown[i+1], 20)).tolist()
		ax.scatter(pos[0], pos[1], marker=(lzip(x, y), 0), s = size, facecolor = piecolor[i % len(piecolor)])

def get_positions(entries):
	result = []
	for hostname in entries:
		entry = getGeoMatch(hostname)
		if not entry:
			continue
		(site, lat, lon) = entry
		stateinfo = entries[hostname]
		weight = math.log(sum(stateinfo)) / math.log(2) + 1
		size = 20 * weight
		result.append({'pos': (lon, lat), 'weight': weight, 'size': size, 'site': site, 'info': stateinfo})
	return result

def get_bounds(posList, margin):
	(lon_l, lat_l) = (lon_h, lat_h) = posList[0]['pos']
	for pos in posList:
		lon, lat = pos['pos']
		lon_l = min(lon_l, lon)
		lon_h = max(lon_h, lon)
		lat_l = min(lat_l, lat)
		lat_h = max(lat_h, lat)
	return [(lon_l - margin, lat_l - margin), (lon_h + margin, lat_h + margin)]

def map_positions(m, posList):
	for pos in posList:
		x, y = m(*pos['pos'])
		pos['x'] = x
		pos['y'] = y

def get_site_status(report):
	siteinfo = report.getWNInfos()
	states = ['FAILED', 'WAITING', 'SUCCESS', 'RUNNING']
	sites = ifilter(lambda x: x not in states, siteinfo)
	return dict(imap(lambda site: (site, lmap(lambda state: siteinfo[site][state]['COUNT'], states)), sites))

def drawMap(report):
	entries = get_site_status(report)
#	entries = {'unl.edu': [276, 0, 246, 0], 'desy.de': [107, 0, 0, 0], 'fnal.gov': [16, 0, 294, 0], 'N/A': [0, 0, 0, 0]}
	posList = get_positions(entries)

	#bounds = [(-60, -120), (60, 120)]
	#bounds = [(30, -10), (60, 40)]
	bounds = get_bounds(posList, margin = 10)

	matplotlib.pyplot.subplots_adjust(left=0, right=1, top=1, bottom=0, wspace=0, hspace=0)
	fig = matplotlib.pyplot.figure(figsize=(12, 6))
	ax = matplotlib.pyplot.subplot(111)
	m = Basemap(projection='cyl', lat_0=0, lon_0=0,
		llcrnrlon=bounds[0][0], llcrnrlat=bounds[0][1],
		urcrnrlon=bounds[1][0], urcrnrlat=bounds[1][1])

	map_positions(m, posList)
	#posList = remove_all_overlap(posList)
	#print posList

	m.bluemarble()
	for pos in posList:
		draw_pie(ax, pos['info'], (pos['x'], pos['y']), pos['size'])
		ax.text(pos['x']+5, pos['y']+5, pos['site'], color='white', fontsize=8)
	fig.savefig(os.path.expanduser('~/map.png'), dpi=300)
