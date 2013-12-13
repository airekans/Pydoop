Pydoop
======

A python concurrent job execution library.

Basics
======

To begin use pydoop, you can just run it as a command line script. Pydoop will run the function written by user.
Suppose we've got a list of file url in a file separated by newlines, and we want to download it concurrently.
So we write the following python module:

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

Now there will be 4 worker processes running your `download` function. We've got the concurrency with little effort!
