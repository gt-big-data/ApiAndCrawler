from pathlib import Path
import os
import requests

def test_HTML_validator():
	rootDir = os.path.dirname(os.path.abspath(__file__))
	pathlist = Path(rootDir + '/../api/templates')
	for file in list(pathlist.glob('*.html')):
		with file.open() as f: 
			r = requests.post('https://validator.w3.org/nu/', 
                    data=f.read(), 
                    params={'out': 'json'}, 
                    headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.101 Safari/537.36', 
                    'Content-Type': 'text/html; charset=UTF-8'})
			assert(len(r.json()['messages']) == 0)


def test_credentials():
	# Do this locally, I just wrote this here to remind you to do it
	assert(True)

# if __name__ == '__main__':
# 	test_HTML_validator()