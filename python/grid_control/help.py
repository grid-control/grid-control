from python_compat import *
import os, time, random

class Help(object):
	def listVars(self, module):
		print "\nIn these files:\n\t",
		print str.join(', ', map(os.path.basename, module.getSubstFiles()))
		print "\nthe following expressions will be substituted:\n"
		print "Variable".rjust(25), ":", "Value"
		print "%s=%s" % ("=" * 26, "=" * 26)

		try:
			job0cfg = module.getJobConfig(0)
		except:
			job0cfg = {}
		try:
			job3cfg = module.getJobConfig(3)
		except:
			job3cfg = {}

		vars = module.getVarMapping().items()
		vars += [('RANDOM', 'RANDOM')]
		for (keyword, variable) in sorted(vars):
			print ("__%s__" % keyword).rjust(25), ":",
			try:
				print module.getTaskConfig()[variable]
			except:
				try:
					print "<example for job 0: %s>" % job0cfg[variable]
				except:
					if keyword == 'DATE':
						print '<example: %s>' % time.strftime("%F")
					elif keyword == 'TIMESTAMP':
						print '<example: %s>' % time.strftime("%s")
					elif keyword == 'RANDOM':
						print '<example: %d>' % random.randrange(0, 900000000)
					elif keyword == 'GUID':
						hex = str.join("", map(lambda x: "%02x" % random.randrange(256), range(16)))
						print '<example: %s-%s-%s-%s-%s>' % (hex[:8], hex[8:12], hex[12:16], hex[16:20], hex[20:])
					else:
						print '<not determinable>'
						continue
				try:
					job3 = job3cfg[variable]
					print " "*25, " ", "<example for job 3: %s>" % job3
				except:
					pass


	def getConfig(self, config, printDefault):
		print
		if printDefault:
			print "These are all used config options:"
		else:
			print "This is the minimal set of config options necessary:"
		print
		print ";", "="*60
		print ";", "grid-control",
		if printDefault:
			print "complete",
		else:
			print "reduced",
		print "config file"
		print ";", "="*60
		print
		for section in config.protocol:
			header = False
			prevNL = False
			for (key, (value, default, volatile)) in config.protocol[section].iteritems():
				if (not printDefault and (str(value) != str(default))) or printDefault:
					if value == 'DEPRECATED':
						continue
					if not header:
						print "[%s]" % section
						header = True
					value = str(value).replace("\n", "\n\t")
					print "%s = %s" % (str(key), str(value))
					prevNL = False
					if default != None and not printDefault:
						print "; Default setting: %s = %r" % (key, default)
						print
						prevNL = True
						
			if header and not prevNL:
				print
