try:
	# hashlib = Python 2.5
	import hashlib
	md5 = hashlib.md5
except:
	import md5
	md5 = md5.md5

try:
	# set = Python 2.4
	set = set
except:
	import sets
	set = sets.Set

try:
	# sorted = Python 2.4
	sorted = sorted
except:
	def sorted(list, comp=None, key=None):
		"""Sort list by either using the standard comparison method cmp()
		or, if supplied, the function comp.  The optional argument key
		is a function that returns the key to sort by - default is the
		identity function.

		>>> sorted([4, 3, 1, 5, 2])
		[1, 2, 3, 4, 5]

		>>> sorted([4, 3, 1, 5, 2], comp=lambda a, b: -cmp(a, b))
		[5, 4, 3, 2, 1]

		>>> sorted(['spam', 'ham', 'cheese'], key=len)
		['ham', 'spam', 'cheese']

		>>> sorted(['spam', 'ham', 'cheese'], comp=lambda a, b: -cmp(a, b), key=len)
		['cheese', 'spam', 'ham']
		"""

		tmp = list[:]
		tmp_cmp = comp

		if key and comp:
			tmp_cmp = lambda x, y: comp(key(x), key(y))
		elif key:
			tmp_cmp = lambda x, y: cmp(key(x), key(y))

		if tmp_cmp != None:
			tmp.sort(tmp_cmp)
		else:
			tmp.sort()
		return tmp

if __name__ == '__main__':
	import doctest
	doctest.testmod()

__all__ = ["md5", "set", "sorted"]
