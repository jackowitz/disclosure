#!/usr/bin/env python
import sys

def gen_input(n_clients, limit=1000):
	for i in range(n_clients):
		with open('input/%d.csv' % i, 'w') as f:
			for j in (x for x in range(limit) if x % 8 == i % 8):
				f.write(str(j) + '\n')

if __name__ == '__main__':
	argc = len(sys.argv)
	if argc < 2:
		print 'usage: gen_input.py n_clients [n_elements]'
		exit(1);
	else:
		if argc > 2:
			gen_input(int(sys.argv[1]), int(sys.argv[2]))
		else:
			gen_input(int(sys.argv[1]))
