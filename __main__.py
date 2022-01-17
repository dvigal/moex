import sys, os


__dir__ = os.path.dirname(__file__)
path = os.path.join(__dir__, 'moex-0.0.3', 'moex')
sys.path.insert(0, path)

print('Hello, World!')
