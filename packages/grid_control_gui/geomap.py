import matplotlib.pyplot as plt
import geodb
import numpy, math, random, os

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
		wsum_x = sum(map(lambda pt: pt['x']*pt['weight'], data))
		wsum_y = sum(map(lambda pt: pt['y']*pt['weight'], data))
		sum_w = sum(map(lambda pt: pt['weight'], data))
		return {'x': wsum_x / sum_w, 'y': wsum_y / sum_w}

	result = []
	data = sorted(data, key = lambda x: -x['weight'])
	for pt in data:
		collisions = filter(lambda x: check_overlap(x, pt), result)
		if collisions:
			result.append(remove_overlap(center_of_mass(collisions), pt))
		else:
			result.append(pt)
	return result

def draw_pie(ax, breakdown, pos, size, piecolor = ['red', 'orange', 'green', 'blue', 'purple']):
	breakdown = [0] + list(numpy.cumsum(breakdown)* 1.0 / sum(breakdown))
	for i in xrange(len(breakdown)-1):
		x = [0] + numpy.cos(numpy.linspace(2 * math.pi * breakdown[i], 2 * math.pi * breakdown[i+1], 20)).tolist()
		y = [0] + numpy.sin(numpy.linspace(2 * math.pi * breakdown[i], 2 * math.pi * breakdown[i+1], 20)).tolist()
		ax.scatter(pos[0], pos[1], marker=(zip(x, y),0), s = size, facecolor = piecolor[i % len(piecolor)])

def drawMap(report):
	from mpl_toolkits.basemap import Basemap
	siteinfo = report.getWNInfos()
	states = ['FAILED', 'WAITING', 'SUCCESS', 'RUNNING']
	sites = filter(lambda x: x not in states, siteinfo)
	entries = dict(map(lambda site: (site, map(lambda state: siteinfo[site][state]['COUNT'], states)), sites))
#	entries = {'unl.edu': [276, 0, 246, 0], 'desy.de': [107, 0, 0, 0], 'fnal.gov': [16, 0, 294, 0], 'N/A': [0, 0, 0, 0]}

	posList = []
	for hostname in entries:
		entry = geodb.getGeoMatch(hostname)
		if not entry:
			continue
		lat, lon = geodb.geoDict[entry]
		stateinfo = entries[hostname]
		weight = math.log(sum(stateinfo)) / math.log(2) + 1
		size = 20 * weight
		posList.append({'pos': (lon, lat), 'weight': weight, 'size': size, 'site': entry, 'info': stateinfo})

	(lon_l, lat_l) = (lon_h, lat_h) = posList[0]['pos']
	for pos in posList:
		lon, lat = pos['pos']
		lon_l = min(lon_l, lon)
		lon_h = max(lon_h, lon)
		lat_l = min(lat_l, lat)
		lat_h = max(lat_h, lat)
	#bounds = [(-60, -120), (60, 120)]
	#bounds = [(30, -10), (60, 40)]
	print (lon_l, lat_l)
	print (lon_h, lat_h)

	plt.subplots_adjust(left=0, right=1, top=1, bottom=0, wspace=0, hspace=0)
	fig = plt.figure(figsize=(12, 6))
	ax = plt.subplot(111)
	m = Basemap(projection='cyl', lat_0=0, lon_0=0,
		llcrnrlat=lat_l-10, urcrnrlat=lat_h+10,
		llcrnrlon=lon_l-10, urcrnrlon=lon_h+10)

	for pos in posList:
		x, y = m(*pos['pos'])
		pos['x'] = x
		pos['y'] = y

	#posList = remove_all_overlap(posList)
	#print posList

	axi = m.bluemarble()
	for pos in posList:
		draw_pie(ax, pos['info'], (pos['x'], pos['y']), pos['size'])
		ax.text(pos['x']+5, pos['y']+5, pos['site'], color='white', fontsize=8)
	fig.savefig(os.path.expanduser('~/map.png'), dpi=300)
