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
                #print(t['top'])
                #print(t.text)
                text.append(t)
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
            if 'Avskrevet:' == text[i].text:
                entrycount = entrycount + 1
            i = i + 1
        self.dprint("We found %s entries on page %s ('Avskrevet:')" % (entrycount, pagenum))

        if(entrycount > 6):
            self.dprint("[WARNING] We found %s entries on page %s, more that 6 is not normal" % (entrycount, pagenum))

        if(entrycount < 1):
            raise  ValueError("[ERROR] No entries found on page %s" % (pagenum))

        i = 0
        found_entries = 0

        entries = {}
        while i < len(text):
            if 'Avsender:' == text[i].text or 'Mottaker:' == text[i].text:
                found_entries = found_entries + 1
                entries[found_entries] = {}
                entries[found_entries]['avsender/mottager.top'] = text[i]['top']
            elif 'Sak:' == text[i].text:
                entries[found_entries]['sak.top'] = text[i]['top']
            elif 'Dok.:' == text[i].text:
                entries[found_entries]['dok.top'] = text[i]['top']
            elif 'Avskrevet:' == text[i].text:
                entries[found_entries]['avskrevet.top'] = text[i]['top']
            elif 'Arkivkode:' == text[i].text:
                entries[found_entries]['arkivkode.top'] = text[i]['top']
                entries[found_entries]['arkivkode.height'] = text[i]['height']
            elif 'Dok. dato:' == text[i].text:
                entries[found_entries]['dokdato.top'] = text[i]['top']
                entries[found_entries]['dokdato.height'] = text[i]['height']
                if not ('avsender/mottager.top' in entries[found_entries] and
                    'sak.top' in entries[found_entries] and
                    'dok.top' in entries[found_entries] and
                    'avskrevet.top' in entries[found_entries] and
                    'arkivkode.top' in entries[found_entries] and
                    'arkivkode.height' in entries[found_entries] and
                    'dokdato.top' in entries[found_entries] and
                    'dokdato.height' in entries[found_entries]):
                    error = "[ERROR] Missing one or more element positions in entry %s on page %s" % (found_entries, pagenum)
                    self.dprint(error)
                    raise ValueError(error)
            i = i + 1

        def add_entry(ent, key, value):
            if key not in entry:
                ent[key] = ""
            if value is None:
                ent[key] = "" # At least Mottager/Avsender can be None
            else:
                ent[key] = "%s%s" % (ent[key], value)

        def add_entry_date(ent, key, value):
            if key in entry:
                ent[key] = None
                error = "[ERROR] It appears we found %s more than once. Overwriting." % (key)
                self.dprint(error)
                raise ValueError(error)
            ent[key] = value

        FIELD_ORDER= {'Arkivdel:': 10, 'Arkivkode:': 7, 'Sak:': 2, 'Dok.:': 3, 'Tilg. kode:': 5, 'Dok. dato:': 9,
                       'Avskrevet:': 6, 'Avsender:': 1, 'Mottaker:': 1, 'Journaldato:': 4, 'Saksbehandler:': 8}

        i = 0
        POST_MOTTAGER = 0; POST_SAK = 1; POST_DOK = 2; POST_JOURNALDATO = 3;
        POST_TILGKODE = 4; POST_ARKIVKODE = 5; POST_SAKSBEHANDER = 6; POST_DOKDATO = 7;
        POST_ARKIVDEL = 8; POST_ARKIVKODE_TEXT = 9; POST_AVSKREVET_TEXT = 10; POST_SAKSBEHANDLER_TEXT = -1;

        state = -1;
        entry = {}
        found_entries = 0
        page = []
        at_end = False
        while i < len(text) and not at_end:
            #self.dprint("%s: %s" %(i, text[i].text))

            if (state == POST_ARKIVDEL):
                if len(entries) < found_entries+1:
                    at_end = True
                    self.dprint("We are at end of page %s" % (pagenum))
                    continue

                next_dok_top = entries[found_entries+1]['dok.top']
                if text[i]['top'] >= next_dok_top:
                    #self.dprint(entry)
                    #self.dprint("Found item with top lower than next top, assume new entry")
                    state = POST_SAKSBEHANDLER_TEXT

                if state == POST_ARKIVKODE_TEXT:
                    pass
                if state == POST_AVSKREVET_TEXT:
                    pass
                if state == POST_SAKSBEHANDLER_TEXT:
                    page.append(entry)
                    entry = {}

            if 'Arkivdel:' == text[i].text:
                state = POST_ARKIVDEL;

            if 'Dok. dato:' == text[i].text:
                state = POST_DOKDATO;

            if (state == POST_SAKSBEHANDER):
                add_entry(entry, "arkivdel", text[i].text)
                #self.dprint("Fant arkivdel-tekst")

            if 'Saksbehandler:' == text[i].text:
                state = POST_SAKSBEHANDER;

            if (state == POST_ARKIVKODE):
                add_entry_date(entry, "dokdato", dateutil.parser.parse(text[i].text))
                #self.dprint("Fant doc dato-text")

            if 'Arkivkode:' == text[i].text:
                state = POST_ARKIVKODE;

            if 'Tilg. kode:' == text[i].text:
                state = POST_TILGKODE;

            if(state == POST_JOURNALDATO):
                add_entry_date(entry, "journaldato", dateutil.parser.parse(text[i].text))
                #self.dprint("Fant journaldato-tekst")

            if 'Journaldato:' == text[i].text:
                # Time to split dok:
                m = re.search('^(.*), ([0-9]{2})/([0-9]{5})-([0-9]{1,4}) (.*)$', entry["dok"]) # 13/00088-2
                add_entry(entry, "doctype?", m.group(1))
                add_entry(entry, "doc-1?", m.group(2))
                add_entry(entry, "doc-2?", m.group(3))
                add_entry(entry, "doc-3?", m.group(4))
                entry["dok"] = m.group(5)
                state = POST_JOURNALDATO;

            if(state == POST_DOK):
                # her er 'Dok.:'
                # og deretter 'Tilg. kode:'
                sak_top = entries[found_entries]['sak.top']
                if text[i]['top'] < sak_top:
                    # Dok ser ut til å inneholde tre datapunkter, deles opp etter at vi finner Journaldato ^
                    add_entry(entry, "dok", text[i].text)
                    #self.dprint("Fant dok-tekst")
                else:
                    add_entry(entry, "tilgkode", text[i].text)
                    #self.dprint("Fant tilgkode-tekst")

            if 'Dok.:' == text[i].text:
                state = POST_DOK;

            if 'Sak:' == text[i].text:
                state = POST_SAK;

            if(state == POST_MOTTAGER):
                add_entry(entry, "sak",text[i].text)
                #self.dprint("Fant sak-tekst")

            if 'Avsender:' == text[i].text or 'Mottaker:' == text[i].text:
                found_entries = found_entries + 1
                #if(state > POST_MOTTAGER):
                #    print(entry)
                #    exit(1)
                am = entry.pop("avsender/mottager", None)
                #self.dprint("am is %s" % (am))
                if 'Avsender:' == text[i].text:
                    add_entry(entry, "avsender", am)
                else:
                    add_entry(entry, "mottaker", am)
                state = POST_MOTTAGER;
                #self.dprint("TAG Avsender/Mottager (state %s)" %(state))

            if(state == POST_SAKSBEHANDLER_TEXT):
                # Everything above avsender/mottager is a avsender/mottager
                #self.dprint("Fant en mottager/avsender")
                add_entry(entry, "avsender/mottager", text[i].text)

            i = i + 1;

        if (found_entries != entrycount):
            self.dprint("[ERROR] We expected %s but found %s entries on page %s" % (found_entries, entrycount, pagenum))
            raise ValueError("[ERROR] We expected %s but found %s entries on page %s" % (found_entries, entrycount, pagenum))
        self.dprint("We found %s of %s expected entries on page %s" %(found_entries, entrycount, pagenum))
        for ent in page:
            self.print_entry(ent);
        s = None
        #raise ValueError("parse_page not implemented")

    def print_entry(self, entry):
        #print(entry)
        print("")
        print("Dok.:\t\t" + entry['doctype?'] + ", " + entry['doc-1?'] + "/" + entry['doc-2?'] + "-"
              + entry['doc-3?'] + " " + entry['dok'])
        print("Sak:\t\t" + entry['sak'])
        if "avsender" in entry:
            print("Avsender:\t%s" % (entry['avsender']))
        else:
            print("Mottaker:\t%s" % (entry['mottaker']))

        print("Journaldato:\t%s\tTilg. kode:\t%s\tSaksbehandler:\t%s" % (entry['journaldato'].strftime('%d.%m.%Y'), entry['tilgkode'], "#saksbehandler"))
        print("Dok. dato:\t%s\tArkivdel:\t%s\tArkivkode:\t%s" % (entry['dokdato'].strftime('%d.%m.%Y'), entry['arkivdel'], "#arkivkode"))
        print("Avskrevet:\t%s" % ("# avskrevet"))
        print("-------------------------------------------------------------------------------------")

    def verify_entry2(self, entrytext, pagenum, num_entry):
        FIELDS_IN_ENTRY = 10

        FIELD_ORDER= {'Arkivdel:': 10, 'Arkivkode:': 7, 'Sak:': 2, 'Dok.:': 3, 'Tilg. kode:': 5, 'Dok. dato:': 9,
                       'Avskrevet:': 6, 'Avsender:': 1, 'Mottaker:': 1, 'Journaldato:': 4, 'Saksbehandler:': 8}
        fields_checked = 0
        num_fields_found = 0
        for text in entrytext:
            fields_checked = fields_checked + 1
            if text.text in FIELD_ORDER:
                num_fields_found = num_fields_found + 1
                if (FIELD_ORDER[text.text] != num_fields_found): # Sanity check
                    error = "[ERROR] Field '%s' is normally field #%s, but was #%s on page %s, entry %s" % \
                                (text, FIELD_ORDER[text], num_fields_found, pagenum, num_entry)
                    self.dprint(error)
                    raise ValueError(error)
                print("%s: %s" % (fields_checked, text.text))
            else:
                print("%s: %s" % (fields_checked, text.text))
                # 1 = Mottager, 3 = Sak,  6 = Dok., 7 = Tilg. kode, 9 = Journaldato/Dok. dato
                # 13  = Journaldato/Dok. dato, 15 = Arkivdel, 18 = Saksbehandler

        self.dprint("All fields appeared in the expected order")

        if (num_fields_found != FIELDS_IN_ENTRY): # Sanity check
            error = "[ERROR] Found %s fields, expected %s on page %s, field %s!" % \
                    (num_fields_found, FIELDS_IN_ENTRY, pagenum, num_entry)
            self.dprint(error)
            raise ValueError(error)
        else:
            self.dprint("Found %s/10 fields in entry %s on page %s" % (num_fields_found, num_entry, pagenum))

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
                    print("Puttinh in brokenpages: %s" % (e));
                    brokenpage = {
                        'scrapedurl' : scrapedurl,
                        'pagenum' : pagenum,
                        'pagecontent' : pagecontent,
                        'failstamp' : datetime.datetime.now(),
                    }
                    #print "Unsupported page %d from %s" % (pagenum, scrapedurl)
                    brokenpages = brokenpages + 1
                    #scraperwiki.sqlite.save(unique_keys=['scrapedurl', 'pagenum'], data=brokenpage, table_name=self.brokenpagetable)
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
