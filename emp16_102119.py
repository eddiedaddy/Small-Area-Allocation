'''

It code does
  1. allocates the ct2 emp16 (in 13+2 SEDsector) to parcels (in 19 COMsector), and
  2. re-aggregate the allocation into SPZ (in 19 COMsector)

But few exceptions are found ...
  1. there are CT2s that could not be identified by parcels (not_found_by_parcel), or
  2. CT2s with emp16 but no trace of emp from the seed data (not_found_by_infousa)

  for those exceptional ct2, pre-assigned [scaguid16 and its spz] is provided

'''

#%%
from collections import OrderedDict
from sectorconverter import convert, COMSector
import csv
import psycopg2


#%%
not_found = {'203951004400008': ['0370247050', '203951440000001', 1110, 1110],
             '204221004400008': ['0370308178', '204221440000001', 3000, 3000],
             '204841004400008': ['0370319628', '204841440000001', 1265, 1260],
             '208341004400008': ['0370790865', '208341440000001', 1265, 1260],
             '330255003677059': ['0590676507', '330255367700001', 1800, 1800],
             '328302003922059': ['0590567460', '328302392200001', 1220, 1220],
             '330464004825659': ['0590147969', '330464482560001', 1810, 1810],
             '330467004825659': ['0590147969', '330467482560001', 1810, 1810],
             '330655006508459': ['0590605247', '330655650840001', 1800, 1800],
             '330656006508459': ['0590147490', '330656650840001', 1800, 1800],
             '330657006508459': ['0590147490', '330657650840001', 1800, 1800],
             '327221009999959': ['0590142238', '327221000590001', 1320, 1320],
             '330721009999959': ['0590630691', '330721000590001', 1880, 1880],
             '433033005670003': ['0650270952', '433033567000001', 3100, 1110],
             '434315009999903': ['0650756052', '434315000030001', 3100, 3100],
             '536046001321471': ['0710059264', '536046132140001', 1413, 1413],
             '600244006504215': ['1110252267', '600244650420002', 8888, 1110],
             '601763009999915': ['1110207573', '601763000150001', 2400, 1800],

#not_found_by_infousa = {
             '202863009999906': ['0370557916', '202863000060010', 2110, 1240],
             '203364009999906': ['0370586518', '203361000060003', 3100, 1150],
             '325111009999959': ['0590017198', '325111000590001', 1110, 1110],
             '325191009999959': ['0590312507', '325191000590001', 1110, 1110],
             '326764003600059': ['0590030222', '326763360000003', 1233, 1230],
             '327885003677059': ['0590685422', '327885367700005', 1820, 1110],
             '329037009999959': ['0590689543', '329037000590001', 1200, 1600],
             '330581004825659': ['0590690999', '330581482560001', 1110, 1110],
             '434172007812003': ['0650755423', '434172781200001', 1111, 1112],
             '538493005996271': ['0710111437', '538493599620002', 1900, 1800],
             '600191009999915': ['1110021419', '600191000150002', 2120, 1800],
             '601424007201615': ['1110176446', '601424720160002', 1800, 1800]}

countyfips = {0:25, 1:37, 2:59, 3:65, 4:71, 5:111, 8:37}

#%%
def get_geoid():
    '''
    establish a clean relationship between ct2 and spz.  SPZ shape file has two more ct2 that are not shown in ct2 SED files
     (434713009999903, 537051002468071)

    So, read the ct2 from an existing SED file, and read/append spzid from the geo file only for the ct2s found in SED file

    :return: geoid = {ct2:[spzs...]}
    '''


    # dead-fixed ct2 list from 2016 data
    finn = open('C:/SED2020/LocalInput/emp16_070819/emp16_13sec_ct2_070819.csv', 'r')
    fcsv = csv.reader(finn)
    _ = fcsv.next()

    geoid = {}
    for r in fcsv:
        ct2 = r[4]
        geoid[ct2] = []
    finn.close()

    # append spzids from the geo file
    finn = open('C:/SED2020/Reference/spz_mjp_geo.csv', 'r')
    fcsv = csv.reader(finn)
    _ = fcsv.next()

    for r in fcsv:
        ct2 = r[15]
        spzid = r[8]

        # let's see if all ct2 in SPZ_geo file are also in ct2 SED
        try:
            geoid[ct2].append(spzid)
        except:
            pass

    finn.close()
    return geoid

#%%
def fill_the_seed(emps_ct2, byprcl):

    seed = []
    nrow = len(byprcl)
    for emps in byprcl.itervalues():
        mxemp = emps[19]
        oneprcl = emps[:-1]
        sumone = sum(oneprcl)

        if mxemp == 0:
            for j in range(19):
                oneprcl[j] = 0.001

        elif sumone == 0:
            for j in range(19):
                oneprcl[j] = 1.0 * mxemp / 19

        else:
            for j in range(19):
                if oneprcl[j] == 0:
                    oneprcl[j] = 0.0001

        seed.append(oneprcl)

    for j in range(19):
        tot_ct2 = emps_ct2[j]
        if tot_ct2 == 0:
            for i in range(nrow):
                seed[i][j] = 0
        else:
            sumseed = 0
            for i in range(nrow):
                sumseed += seed[i][j]

            ratio = 1.0 * tot_ct2 / sumseed
            isum = 0
            residual = 0.0
            for i in range(nrow):
                fvalue = 1.0 * seed[i][j] * ratio
                ivalue = int(fvalue)
                if residual + fvalue - ivalue > 0.5:
                    ivalue += 1
                residual += fvalue - ivalue
                seed[i][j] = ivalue
                isum += ivalue

    return seed


if __name__ == '__main__':
    geoid = get_geoid()

    finn = open('C:/SED2020/LocalInput/emp16_070819/emp16_13sec_ct2_070819.csv', 'r')
    foutspz = open('C:/SED2020/LocalInput/emp16_070819/emp16_20sec_spz_070819.csv', 'w')
    foutprl = open('C:/SED2020/LocalInput/emp16_070819/emp16_20sec_parcel_070819.csv', 'w')

    fcsv = csv.reader(finn)
    csvspz = csv.writer(foutspz, lineterminator='\n')
    csvprl = csv.writer(foutprl, lineterminator='\n')

    csvspz.writerow(['ct2', 'spzid'] + COMSector.name)
    csvprl.writerow(['ct2', 'spzid', 'scaguid16', 'iemp', 'comsect'])

    _ = fcsv.next()

    conn = psycopg2.connect(database='working', user='postgres', password='postgres', host='localhost')
    cursor = conn.cursor()

    toCom_index = COMSector.to_index

#%%
    count = 0
    for r in fcsv:
        count += 1
        if count==3: break
        ct2 = r[4]
        sum_ct2 = int(r[5])
        sed_ct2 = [int(x) for x in r[6:21]]
        icounty = 1 if ct2[0] == '9' else int(ct2[0]) - 1
        emps_ct2 = convert(icounty, sed_ct2, 'to_com')
        allocated_sum = 0

        if sum_ct2 == 0:
            # all the member spzs are zero
            # no parcel is reported for emp allocation
            for spzid in geoid[ct2]:
                csvspz.writerow([ct2, spzid] + [0] * 19)

        elif ct2 in not_found:
            uid, spzid, _, _ = not_found[ct2]

            # one spz
            csvspz.writerow([ct2, spzid] + emps_ct2)
            allcated_sum = sum(emps_ct2)

            # one parcel
            s = 0
            for emp in emps_ct2:
                if emp > 0:
                    csvprl.writerow([ct2, spzid, uid, emp, s])
                s += 1

        else:
            byprcl = OrderedDict()

            # first, try info_es_2016 for seed
            sqlstr  = "select p.spzid22, i.scaguid16, i.sect20, i.sect13, i.emp "
            sqlstr += "from public.info_es_2016 i, parcel16.parcel16_nov18 p "
            sqlstr += "where p.county=%d and i.scaguid16=p.scaguid16 and i.ct2='%s' " % (countyfips[icounty], ct2)
            sqlstr += "order by spzid22, scaguid16;"
            cursor.execute(sqlstr)

            for r in cursor.fetchall():
                spzid, uid, sect20, sect13, iemp = r
                comsector = toCom_index[(sect20, sect13)]
                if comsector > 18: continue   # skip military or etc

                spzuid = (spzid, uid)
                if spzuid not in byprcl:
                    byprcl[spzuid] = [0] * 20 # one more to sum

                byprcl[spzuid][comsector] += iemp
                byprcl[spzuid][19] += iemp


            if len(byprcl) == 0:
                sqlstr  = "select spzid22, scaguid16, coalesce(mxes, 0) from rtp20.by_parcel "
                sqlstr += "where gp16 > 1199 and ct2 = '%s' " % ct2
                sqlstr += "order by spzid22, scaguid16;"
                cursor.execute(sqlstr)

                for r in cursor.fetchall():
                    spzid, uid, mxemp = r
                    spzuid = (spzid, uid)
                    byprcl[spzuid] = [0] * 19 + [mxemp]


            if len(byprcl) == 0:  # residential parcels only in this ct2.  then which housing should hold employment?
                sqlstr  = "select spzid22, scaguid16, coalesce(mxes, 0) from rtp20.by_parcel "
                sqlstr += "where ct2 = '%s' " % ct2
                sqlstr += "order by spzid22, gp16, scaguid16;"
                cursor.execute(sqlstr)

                for r in cursor.fetchall():
                    spzid, uid, mxemp = r
                    spzuid = (spzid, uid)
                    byprcl[spzuid] = [0] * 19 + [mxemp]


            if len(byprcl) == 0:
                print 'no information for ct2 %s' % ct2
                continue


            seed = fill_the_seed(emps_ct2, byprcl)
            byspz = {}
            i = 0
            for spzuid in byprcl.iterkeys():
                spzid, uid = spzuid
                if spzid not in byspz:
                    byspz[spzid] = [0]* 19
                for j in range(19):
                    if seed[i][j] > 0:
                        byspz[spzid][j] += seed[i][j]
                        csvprl.writerow([ct2, spzid, uid, seed[i][j], j])
                i += 1


            for spzid in geoid[ct2]:
                if spzid in byspz:
                    csvspz.writerow([ct2, spzid] + byspz[spzid])
                    allocated_sum += sum(byspz[spzid])
                else:
                    csvspz.writerow([ct2, spzid] + [0] * 19)

        print ct2, sum_ct2, allocated_sum

    cursor = None
    conn = None

    finn.close()
    foutspz.close()
    foutprl.close()

    print 'done'
