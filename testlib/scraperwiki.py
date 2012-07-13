# The real version is available from
# https://bitbucket.org/ScraperWiki/scraperwiki/src/85cbc82c32f2/scraperlibs/python/scraperwiki/

import tempfile
import urllib2
import os

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

def pdftoxml(pdfdata, options=''):
    """converts pdf file to xml file"""
    pdffout = tempfile.NamedTemporaryFile(suffix='.pdf')
    pdffout.write(pdfdata)
    pdffout.flush()

    xmlin = tempfile.NamedTemporaryFile(mode='r', suffix='.xml')
    tmpxml = xmlin.name # "temph.xml"
    cmd = '/usr/bin/pdftohtml -xml -nodrm -zoom 1.5 -enc UTF-8 -noframes %s "%s" "%s"' % (options, pdffout.name, os.path.splitext(tmpxml)[0])
    cmd = cmd + " >/dev/null 2>&1" # can't turn off output, so throw away even stderr yeuch
    os.system(cmd)

    pdffout.close()
    #xmlfin = open(tmpxml)
    xmldata = xmlin.read()
    xmlin.close()
    return xmldata

def swimport(scrapername):
    return None
