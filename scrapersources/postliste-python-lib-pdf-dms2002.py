# -*- coding: utf-8 -*-
#
# Python library for parsing public post journals (postlister) in Norway
#
# This parser is for the format currently known as 
# "DMS2002 - Software Innovation"
#
# Based on the scraper advanced-scraping-pdf and postliste-python-lib
#
# Possible sources using format:
# http://www.hig.no/om_hig/offentleg_journal (week 34 2014 and onwards)
# khib.no
# hbv.no
# www.bystyret.oslo.kommune.no
# www.spesialenheten.no
# www.frogn.kommune.no

# Google search to find more: "Offentlig journal" "Ansvarlig enhet" Arkivdel "Dok. dato" Avskrevet filetype:pdf


import scraperwiki
import string
import re
from BeautifulSoup import BeautifulSoup
import datetime
import dateutil.parser
import sys
jp=scraperwiki.swimport('postliste-python-lib')

class PDFJournalParser(jp.JournalParser):
    pagetable = "unparsedpages"
    brokenpagetable = "brokenpages"
    hiddentext = False
    breakonfailure = True
    debug = False
    def __init__(self, agency, hiddentext=False, debug=False):
        self.hiddentext = hiddentext
        self.debug = debug
        jp.JournalParser.__init__(self, agency=agency)

    def sync(self):
        sys.stdout.flush()
        sys.stderr.flush()

    def dprint(self, msg):
        if self.debug:
            print(msg)
            self.sync()

    def parse_page(self, pdfurl, pagenum, pagecontent):
        self.sync()
        print "Scraping " + pdfurl + " page " + str(pagenum)
        s = BeautifulSoup(pagecontent)
        datastore = []
        text = []
        linecount = 0
        #dprint(s)
        # Find all text-blobs and number them
        for t in s.findAll('text'):
            if t.text != " ":
                text.append(t.text)
                #self.dprint(str(linecount) + ": " + t.text)
                #self.dprint(str(linecount) + ": " + ":".join("{:02x}".format(ord(c)) for c in t.text))
            linecount = linecount + 1

        #self.dprint("Found " + str(linecount) + " lines/text fragments in the PDF")
        if len(text) < linecount:
            raise  ValueError("[ERROR] Found %s interresting lines, but only saved %s?" % (linecount, len(text)))

        # Count how many entries to expect on this page, to be able to
        # verify that all of them were found.
        entrycount = 0
        i = 0
        while i < len(text):
            if 'Avskrevet:' == text[i]:
                entrycount = entrycount + 1
            i = i + 1
        self.dprint("We found %s entries on page %s ('Avskrevet:')" % (entrycount, pagenum))

        if(entrycount > 6):
            self.dprint("[WARNING] We found %s entries on page %s, more that 6 is not normal" % (entrycount, pagenum))

        if(entrycount < 1):
            raise  ValueError("[ERROR] No entries found on page %s" % (pagenum))

        i = 0
        found_entries = 0
        entry_start = -1
        entry_stop = -1
        while i < len(text):
            if 'Avsender:' == text[i] or 'Mottaker:' == text[i]:
                entry_start = i - 1
                if (entry_start < 0):
                    entry_start = 0
                #self.dprint("ESTART")
            
            if 'Arkivdel:' == text[i]:
                #self.dprint("EEND")
                if(entry_start == -1):
                    self.dprint("[ERROR] Found end of entry (line %s) before start of entry on page %s" % (i, pagenum))
                    raise ValueError("[ERROR] Found end of entry before start of entry on page %s" % (pagenum))
                entry_end = i + 2
                if (entry_end > len(text)):
                    entry_end = len(text)
                found_entries = found_entries + 1
                entry = self.pdfparser(text[entry_start:entry_end], pdfurl, pagenum, found_entries)
                entry_start = -1
                entry_stop = -1
            i = i + 1
        if (found_entries != entrycount):
            self.dprint("[ERROR] We expected %s but found %s entries on page %s" % (found_entries, entrycount, pagenum))
            raise ValueError("[ERROR] We expected %s but found %s entries on page %s" % (found_entries, entrycount, pagenum))
        self.dprint("We found %s of %s expected entries on page %s" %(found_entries, entrycount, pagenum))
        s = None
        raise ValueError("parse_page not implemented")

    def pdfparser(self, entrytext, pdfurl, pagenum, num_entry):
        FIELDS_IN_ENTRY = 10
        field_order = {'Arkivdel:': 10, 'Arkivkode:': 7, 'Sak:': 2, 'Dok.:': 3, 'Tilg. kode:': 5, 'Dok. dato:': 9, 'Avskrevet:': 6, 'Avsender:': 1, 'Mottaker:': 1, 'Journaldato:': 4, 'Saksbehandler:': 8}
        fields = {'Avsender:', 'Mottaker:', 'Sak:', 'Dok.:', 'Journaldato:', 'Tilg. kode:', 'Avskrevet:', 'Arkivkode:', 'Saksbehandler:', 'Dok. dato:', 'Arkivdel:' }
        num_fields_found = 0
        for text in entrytext:
            if text in field_order:
                num_fields_found = num_fields_found + 1
                if (field_order[text] != num_fields_found): # Sanity check
                    self.dprint("[ERROR] Field '%s' is normally field #%s, but was #%s on page %s, entry %s" % (text, field_order[text], num_fields_found, pagenum, num_entry))
                    raise ValueError("[ERROR] Field '%s' is normally field #%s, but was #%s on page %s, entry %s" % (text, field_order[text], num_fields_found, pagenum, num_entry))
        self.dprint("All fields appeared in the expected order")
        if (num_fields_found != FIELDS_IN_ENTRY): # Sanity check
            self.dprint("[ERROR] Found %s fields, expected %s on page %s, field %s!" % (num_fields_found, FIELDS_IN_ENTRY, pagenum, num_entry))
            raise ValueError("[ERROR] Found %s fields, expected %s on page %s, field %s!" % (num_fields_found, FIELDS_IN_ENTRY, pagenum, num_entry))
        else:
            self.dprint("Found %s/10 fields in entry %s on page %s" % (num_fields_found, num_entry, pagenum))
        #print field_order

    def process_pages(self):
        brokenpages = 0
        try:
            sqlselect = "* from " + self.pagetable + " limit 1"
            pageref = scraperwiki.sqlite.select(sqlselect)
            while pageref:
                scrapedurl = pageref[0]['scrapedurl']
                pagenum = pageref[0]['pagenum']
                pagecontent = pageref[0]['pagecontent']
                try:
                    sqldelete = "delete from " + self.pagetable + " where scrapedurl = '" + scrapedurl + "' and pagenum = " + str(pagenum)
                    self.parse_page(scrapedurl, pagenum, pagecontent)
                    sys.stdout.flush()
                    sys.stderr.flush()
                    scraperwiki.sqlite.execute(sqldelete)
                except ValueError, e:
                    brokenpage = {
                        'scrapedurl' : scrapedurl,
                        'pagenum' : pagenum,
                        'pagecontent' : pagecontent,
                        'failstamp' : datetime.datetime.now(),
                    }
                    #print "Unsupported page %d from %s" % (pagenum, scrapedurl)
                    brokenpages = brokenpages + 1
                    scraperwiki.sqlite.save(unique_keys=['scrapedurl', 'pagenum'], data=brokenpage, table_name=self.brokenpagetable)
                scraperwiki.sqlite.execute(sqldelete)
                #scraperwiki.sqlite.commit()
                #exit(0)
                pageref = scraperwiki.sqlite.select(sqlselect)
        except scraperwiki.sqlite.SqliteError, e:
            print str(e)
            raise
        if 0 < brokenpages:
            raise ValueError("Found %d pages with unsupported format" % brokenpages)

    # Check if we recognize the page content, and throw if not
    def is_valid_page(self, pdfurl, pagenum, pagecontent):
        s = BeautifulSoup(pagecontent)
        for t in s.findAll('text'):
            if t.text != " ":
                if 'Dok.:' == t.text:
                    s = None
                    return True
        s = None
        self.dprint("Unrecognized page format for " + pdfurl)
        #raise ValueError("Unrecognized page format for " + pdfurl)

    # Split PDF content into pages and store in SQL table for later processing.
    # The process is split in two to better handle parge PDFs (like 600 pages),
    # without running out of CPU time without loosing track of what is left to
    # parse.
    def preprocess(self, pdfurl, pdfcontent):
        print "Preprocessing PDF " + pdfurl
        if not pdfcontent:
            raise ValueError("No pdf content passed for " + pdfurl)
        if self.hiddentext:
            options = '-hidden'
        else:
            options = ''
        xml=scraperwiki.pdftoxml(pdfcontent, options)
        #self.dprint("The XMLK:")
        #self.dprint(xml)
        pages=re.findall('(<page .+?</page>)',xml,flags=re.DOTALL)
        xml=None

        pagecount = 0
        datastore = []
        for page in pages:
            pagecount = pagecount + 1
            self.is_valid_page(pdfurl, pagecount, page)
            data = {
                'scrapedurl' : pdfurl,
                'pagenum' : pagecount,
                'pagecontent' : page,
            }
            datastore.append(data)
        self.dprint("Found %s pages, %s added to database" % (pagecount, len(datastore)))
        if 0 < len(datastore):
            scraperwiki.sqlite.save(unique_keys=['scrapedurl', 'pagenum'], data=datastore, table_name=self.pagetable)
        else:
            raise ValueError("Unable to find any pages in " + pdfurl)
        pages = None
