            # Last, try some of the broken pages again, in case we got support for handling them in the mean time
            try:
                # First, check if the table exist
                scraperwiki.sqlite.execute("select * from " + self.brokenpagetable)

                newtrystamp = datetime.datetime.now()
                sqlselect = "* from " + self.brokenpagetable + " where failstamp is NULL or failstamp < '" + str(newtrystamp) + "'" + " limit 1"
                try:
                    pageref = scraperwiki.sqlite.select(sqlselect)
                except scraperwiki.sqlite.SqliteError, e:
                    scraperwiki.sqlite.execute("ALTER TABLE " + self.brokenpagetable + " ADD COLUMN failstamp")
                    scraperwiki.sqlite.commit()
                    pageref = scraperwiki.sqlite.select(sqlselect)

                pagelimit = 10
                while pageref and 0 < pagelimit:
                    pagelimit = pagelimit - 1
                    scrapedurl = pageref[0]['scrapedurl']
                    pagenum = pageref[0]['pagenum']
                    pagecontent = pageref[0]['pagecontent']
#                    print "Found " + scrapedurl + " page " + str(pagenum) + " length " + str(len(pagecontent))
                    try:
                        sqldelete = "delete from " + self.brokenpagetable + " where scrapedurl = '" + scrapedurl + "' and pagenum = " + str(pagenum)
                        self.parse_page(scrapedurl, pagenum, pagecontent)
#                    print "Trying to: " + sqldelete
                        scraperwiki.sqlite.execute(sqldelete)
                    except ValueError, e:
                        brokenpage = {
                            'scrapedurl' : scrapedurl,
                            'pagenum' : pagenum,
                            'pagecontent' : pagecontent,
                            'failstamp' : newtrystamp,
                        }
                    
                        print "Still unsupported page %d from %s" % (pagenum, scrapedurl)
                        brokenpages = brokenpages + 1
                        scraperwiki.sqlite.save(unique_keys=['scrapedurl', 'pagenum'], data=brokenpage, table_name=self.brokenpagetable)
                    scraperwiki.sqlite.commit()
                    pageref = scraperwiki.sqlite.select(sqlselect)
            except:
                True # Ignore missing brokenpages table
