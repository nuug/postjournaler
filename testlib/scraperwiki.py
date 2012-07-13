def scrape(url):
    print "Scraping %s" % url
    if -1 != url.find("file://"):
        f = open(url.replace("file://", ""), "r")
        content = f.read()
        f.close()
        return content
    else:
        return ""

def pdftoxml(pdfcontent, options):
    return pdfcontent
