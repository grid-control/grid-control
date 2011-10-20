from grid_control import ConfigOverlay
import sys

class CMSOverlay(ConfigOverlay):
	def __init__(self, config):
		config.set('jobs', 'monitor', 'dashboard', override = False)

		# Rewrite cms storage urls:
		proxy = VomsProxy(config)
		hnName = dn2hn(proxy.get('identity'))
		if not hnName:
			raise ConfigError('Unable to map grid certificate to hypernews name!')
		sePaths = config.getList('storage', 'se path', [])
		seInputPaths = config.getList('storage', 'se input path', sePaths)
		seOutputPaths = config.getList('storage', 'se output path', sePaths)
		def rewriteSEurl(url):
			if url.startswith('cms://'): # cms://T2_DE_DESY/project/subdir
				site, url = (url.rstrip('/') + '/').replace('cms://', '').lstrip('/').split('/', 1)
				return lfn2pfn('/store/user/%s/%s' % (hnName, url.rstrip('/')), site)
			return url
		config.set('storage', 'se path', str.join('\n', map(rewriteSEurl, sePaths)))
		config.set('storage', 'se input path', str.join('\n', map(rewriteSEurl, seInputPaths)))
		config.set('storage', 'se output path', str.join('\n', map(rewriteSEurl, seOutputPaths)))
