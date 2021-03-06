Pydoop
======

[![Build Status](https://travis-ci.org/airekans/Pydoop.png?branch=master)](https://travis-ci.org/airekans/Pydoop) 
[![status](https://sourcegraph.com/api/repos/github.com/airekans/Pydoop/badges/status.png)](https://sourcegraph.com/github.com/airekans/Pydoop)

A python concurrent job execution library.

Basic Usage
======

To begin use pydoop, you can just run it as a command line script. Pydoop will run the function written by user.
Suppose you've got a list of file url in a file separated by newlines, and you want to download it concurrently.
So you can write the following python module:

```python
# download.py
import urllib2

def download(url):
    url = url.strip()
    try:
        web_file = urllib2.urlopen(url)
    except urllib2.URLError:
        return
    
    file_name = url.split('/')[-1]
    fd = open(file_name, 'w')
    fd.writelines(web_file)
    fd.close()
```

And suppose the urls are saved in the file `urls.txt`, then you can run the your `download` function with pydoop via the following command:

```bash
$ python pydoop.py -w 4 -f download download.py urls.txt
```

Now there will be 4 worker processes running your `download` function. You've got the concurrency with little effort!

Advanced Usage
======

You can also use pydoop as a module. The most important class in pydoop is `Pool`. For example, to download all urls as above, you can write the following code:

```python
import pydoop
import urllib2

def download(url):
    url = url.strip()
    try:
        web_file = urllib2.urlopen(url)
    except urllib2.URLError:
        return
    
    file_name = url.split('/')[-1]
    fd = open(file_name, 'w')
    fd.writelines(web_file)
    fd.close()

if __name__ == '__main__':
    pool = pydoop.Pool(4) # there will be 4 worker processes in the pool
    in_fd = open('urls.txt') # open the input file
    
    pool.run(download, in_fd) # pool will run your download function concurrently.
```
