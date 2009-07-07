class Help(object):
	def listVars(self, module):
		print "\nIn these files:\n\t",
		print str.join(', ', module.getSubstFiles())
		print "\nthe following expressions will be substituted:\n"
		print "Variable".rjust(25), " ", "Value"

		vars = module.getVarMapping()
		vars += [('RANDOM', 'RANDOM')]
		vars.sort()
		for var in vars:
			print ("__%s__" % var[0]).rjust(25), ":",
			try:
				print module.getTaskConfig()[var[1]]
			except:
				try:
					print "<example for job 1: %s> " % module.getJobConfig(1)[var[1]]
				except:
					if var[0] in map(lambda x: "SEED_%d" % x, xrange(10)):
						print "<example for job 1: %d>" % (module.seeds[int(var[0].split("_")[1])] + 1)
					else:
						print '<not yet determinable>'
