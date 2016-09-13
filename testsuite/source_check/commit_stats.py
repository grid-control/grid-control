# -*- coding: utf-8 -*-
import sys, subprocess, os, getFiles

if __name__ == '__main__':
	os.system('git log --pretty="%H|%aN|%aE" --no-merges -w --numstat > addHEADER.log')
	email_dict = {}
	commits_dict = {}
	changes_a_dict = {}
	changes_fn_dict = {}
	for line in map(str.strip, open('addHEADER.log')):
		if ('|' in line) and ('@' in line):
			author, email = line.split('|')[1:]
			email_dict[author] = email
			commits_dict[author] = commits_dict.get(author, 0) + 1
		elif line:
			add, rm, fn = line.split()
			if add == '-':
				continue
			if getFiles.matchFile(fn, showExternal = False):
				prev_a = changes_a_dict.get(author, (0, 0))
				changes_a_dict[author] = (prev_a[0] + int(add), prev_a[1] + int(rm))
				prev_fn = changes_fn_dict.get(fn, (0, 0))
				changes_fn_dict[fn] = (prev_fn[0] + int(add), prev_fn[1] + int(rm))
#	print email_dict
	email_dict['Manuel Zeise'] = 'manuel.zeise@sap.com'

#	for fn in sorted(changes_fn_dict, key = lambda x: changes_fn_dict[x]):
#	for fn in sorted(changes_fn_dict):
#		print '(%5d:%5d)' % changes_fn_dict[fn], fn
#	print len(changes_fn_dict)
#	for a in sorted(changes_a_dict, key = lambda x: (-(changes_a_dict[x][0] + changes_a_dict[x][1]), x.split()[1])):
#	print changes_a_dict[']
	def write_hiscore(stream):
		for a in sorted(changes_a_dict, key = lambda x: (-(changes_a_dict[x][0] - 0.5*changes_a_dict[x][1]), x.split()[1])):
#			print '(%5d:%5d)' % changes_a_dict[a],
#			print changes_a_dict[a][0] - 0.5*changes_a_dict[a][1],
#			print (changes_a_dict[a][0] - changes_a_dict[a][1]),
#			print (changes_a_dict[a][0] + changes_a_dict[a][1]),
			msg = ' '*15 + '%-23s <%s>\n' % (a.decode('utf-8'), email_dict[a].replace('stober@cern.ch', 'mail@fredstober.de').lower())
			import math
			print '%s:%s' % (a.replace(' ', ''), int(1+1*math.log(1+changes_a_dict[a][0] - 0.5*changes_a_dict[a][1])))
			yield msg
		yield '\n'
		yield ' ' * 15 + '(in order of development activity - excluding libraries)'

	fp_r = open('../../docs/NOTICE')
	fp_w = open('../../docs/NOTICE.new', 'w')
	wrote_hiscore = False
	for line in fp_r:
		if not line.startswith(' ' * 15):
			fp_w.write(line)
		elif not wrote_hiscore:
			for hs in write_hiscore(fp_w):
				if not 'Fred Stober' in hs:
					fp_w.write(hs.encode('utf-8'))
			wrote_hiscore = True
	os.rename('../../docs/NOTICE.new', '../../docs/NOTICE')
