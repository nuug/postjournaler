import urllib2

def scrape(url):
    print "Scraping %s" % url
    if -1 != url.find("file://"):
        f = open(url.replace("file://", ""), "r")
        content = f.read()
        f.close()
        return content
    else:
        response = urllib2.urlopen(url)
        html = response.read()
        return html

def pdftoxml(pdfcontent, options):
    return pdfcontent

def swimport(scrapername):
    return None
