#!/usr/bin/env python

""" Query that returns all the inputs for processccd.py
    matching the calibrations by date and version to the image """

import argparse
import sys

from despymisc import miscutils
from intgutils import queryutils
from despydb import desdbi

######################################################################
def query_db(args):

    rawinfo = {}
    dbh = desdbi.DesDbi(None, args.section)
    #sql = "select filename,taiObs,expId,pointing,visit,dateObs,frameId,filter,field,pa,exptime,ccdtemp,ccd,proposal,config,autoguider from image where filetype='hsc_raw' and visit=%s and ccd=%s" % (dbh.get_named_bind_string('visit'), dbh.get_named_bind_string('ccd')) 

    rawfields = miscutils.fwsplit(args.raw_select,',')
    calibfields = miscutils.fwsplit(args.calib_select,',')

    imgselect = []
    for f in rawfields:
        imgselect.append('i.%s as img_%s' % (f,f))
        
    calibselect = []
    for f in calibfields:
        calibselect.append('c.%s as calib_%s' % (f,f))

    fromtables = ['ops_calibration_lookup l', 'calibration c', 'ccd_overlap_patch o', 'image i']
    if args.visittag is not None:
        fromtables.append('visit_tag vt')

    # Note do not need blacklist in main from clause
        
    sql = """select distinct o.tract, %s, %s 
             from %s
             where c.filename=l.filename and i.filetype=%s and i.visit=o.visit and 
             i.ccd=o.ccd and l.min_date <= to_date(i.dateobs, 'YYYY-MM-DD') and 
             to_date(i.dateobs, 'YYYY-MM-DD') <= l.max_date and i.ccd=c.ccd and 
             (c.filter is null or c.filter=i.filter) and o.version=%s and l.version=%s and o.tract = %s""" % \
          (','.join(imgselect), 
           ','.join(calibselect), 
           ','.join(fromtables),
           dbh.get_named_bind_string('raw_filetype'), 
           dbh.get_named_bind_string('overlap_version'), 
           dbh.get_named_bind_string('calib_version'),
           dbh.get_named_bind_string('tract'))
    bindvals = {'tract': args.tract, 'raw_filetype': args.raw_filetype, 
                'overlap_version': args.overlap_version,
                'calib_version': args.calib_version}
    
    if args.filter is not None:
        sql += " and i.filter in ('%s') " % ("','".join(miscutils.fwsplit(args.filter, ',')))

    if args.patch is not None:
        sql += " and o.patch = %s " % dbh.get_named_bind_string('patch')
        bindvals['patch'] = args.patch

    if args.visit is not None:
        sql += " and o.visit in ('%s') " % ("','".join(miscutils.fwsplit(args.visit, ',')))
        #sql += " and o.visit = %s " % dbh.get_named_bind_string('visit')
        #bindvals['visit'] = args.visit

    if args.ccd is not None:
        sql += " and o.ccd = %s " % dbh.get_named_bind_string('ccd')
        bindvals['ccd'] = args.ccd

    if args.visittag is not None:
        sql += " and vt.tag in ('%s') and vt.visit=i.visit and vt.ccd=i.ccd" % ("','".join(miscutils.fwsplit(args.visittag, ',')))

    if not args.blacklist:
        sql += " and not exists (select visit from blacklist bl where i.visit=bl.visit and i.ccd=bl.ccd) "

    if args.blacklist_code is not None:
        sql += " and not exists (select visit from blacklist bl where i.visit=bl.visit and i.ccd=bl.ccd and bl.reason_code in (%s)) " % (",".join(miscutils.fwsplit(args.blacklist_code, ',')))

    print "sql =", sql

    curs = dbh.cursor()
    curs.prepare(sql)


    flabelset = set({'raw'})
    curs.execute(None, bindvals)
    desc = [d[0].lower() for d in curs.description]
    for row in curs:
        #print row
        cinfo = dict(zip(desc, row))

        # since writing to WCL, convert NULL to __KEEP__NONE__
        for k in cinfo:
            if cinfo[k] is None:
                cinfo[k] = '__KEEP__NONE__' 

        
        if cinfo['img_filename'] not in rawinfo:
            rawinfo[cinfo['img_filename']] = {}
            rawdict = {'imgname': cinfo['img_filename']}
            for f in rawfields:
                rawdict[f] = cinfo['img_%s' % f]
             
            rawinfo[cinfo['img_filename']]['raw'] = rawdict
             
        calibdict = {'imgname': cinfo['img_filename']}
        for f in calibfields: 
            calibdict[f] = cinfo['calib_%s' % f]
        rawinfo[cinfo['img_filename']][calibdict['filetype']] = calibdict
        flabelset.add(calibdict['filetype'])
                
    return rawinfo, list(flabelset)

######################################################################
def reformat_data(rawinfo, filelabels):
    """ change data structure of queried data to meet requirements of convert_multiple_files_to_lines """

    newdata = []
    for rdict in rawinfo.values():
        lineinfo = []
        for label in filelabels:
            lineinfo.append(rdict[label])
        newdata.append(lineinfo)

    return newdata
    

######################################################################
def main(argv):
    parser = argparse.ArgumentParser(description='Dummy query that reads fake query results from a semi-colon separated file')
    parser.add_argument('--qoutfile', required=True, action='store')
    parser.add_argument('--qouttype', action='store', default='wcl')
    parser.add_argument('--section', action='store')

    parser.add_argument('--overlap_version', required=True, action='store')
    parser.add_argument('--calib_version', required=True, action='store')
    parser.add_argument('--raw_filetype', default='hsc_raw', action='store')
    parser.add_argument('--calib_select', default='filename, filter, ccd, filetype, calib_date', action='store')
    parser.add_argument('--raw_select', default='filename, filter, visit, ccd, pointing, dateobs, field', action='store')
    parser.add_argument('--tract', required=True, action='store', type=int)
    parser.add_argument('--patch', required=False, action='store', help='Must match the patch string format in overlap table')
    parser.add_argument('--filter', required=False, action='store', help="Include only certain filters, comma-separated list")
    parser.add_argument('--visit', required=False, action='store', help="Include only certain visits, comma-separated list")
    parser.add_argument('--ccd', required=False, action='store', help="Include only certain ccds - note doesn't support visit+ccd pairs")
    parser.add_argument('--visittag', required=False, action='store')
    parser.add_argument('--blacklist', required=False, action='store_false', help="Doesn't include if exists in blacklist table")
    parser.add_argument('--blacklist_code', required=False, action='store', help="Use if only want to blacklist for certain codes, comma-separated list")
    args = parser.parse_args(argv)

    data, filelabels = query_db(args)
    #print data

    filelist = reformat_data(data, filelabels)

    lines = queryutils.convert_multiple_files_to_lines(filelist, filelabels, initcnt=1)
    queryutils.output_lines(args.qoutfile, lines, args.qouttype)


if __name__ == '__main__':
    main(sys.argv[1:])
