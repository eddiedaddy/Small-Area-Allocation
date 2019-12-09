
from collections import OrderedDict
from sectorconverter import convert, COMSector
import csv
import psycopg2
import multiprocessing

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

countyfips = {0: 25, 1: 37, 2: 59, 3: 65, 4: 71, 5: 111, 8: 37}


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


def writer(outQ):
    foutprl = open('C:/SED2020/LocalInput/emp16_070819/emp16_20sec_parcel_070819.csv', 'w')
    foutprl.write('ct2,spzid,scaguid16,iemp,comsect\n')

    while 1:
        message = outQ.get()
        if message == 'KILL':
            break
        else:
            ct2, spzid, uid, emp, sector = message
            foutprl.write('%s,%s,%s,%d,%d\n' % (ct2, spzid, uid, emp, sector))
            foutprl.flush()
            # print(ct2)

    foutprl.close()
    print('listener out')

    return

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

            residual = 0.0
            for i in range(nrow):
                fvalue = 1.0 * seed[i][j] * ratio
                ivalue = int(fvalue)
                if residual + fvalue - ivalue >= 0.5:
                    ivalue += 1
                residual += fvalue - ivalue
                seed[i][j] = ivalue

    return seed


def theworker(in_Q, outQ):
    conn = psycopg2.connect(database='working', user='postgres', password='postgres', host='localhost')
    cursor = conn.cursor()
    toCom_index = COMSector.to_index

    while 1:
        if in_Q.empty():
            break

        input = in_Q.get()
        if input == 'KILL':
            break

        ct2, emps_ct2 = input
        icounty = int(ct2[0])-1
        if ct2 in not_found:
            uid, spzid, _, _ = not_found[ct2]
            s = 0
            for emp in emps_ct2:
                if emp > 0:
                    outQ.put((ct2, spzid, uid, emp, s))
                s += 1

        else:
            byprcl = OrderedDict()

            # first, try info_es_2016 for seed
            sqlstr = "select p.spzid22, i.scaguid16, i.sect20, i.sect13, i.emp "
            sqlstr += "from public.info_es_2016 i, parcel16.parcel16_nov18 p "
            sqlstr += "where p.county=%d and i.scaguid16=p.scaguid16 and i.ct2='%s' " % (countyfips[icounty], ct2)
            sqlstr += "order by spzid22, scaguid16;"
            cursor.execute(sqlstr)

            for r in cursor.fetchall():
                spzid, uid, sect20, sect13, iemp = r
                comsector = toCom_index[(sect20, sect13)]
                if comsector > 18: continue  # skip military or etc

                spzuid = (spzid, uid)
                if spzuid not in byprcl:
                    byprcl[spzuid] = [0] * 20  # one more to sum

                byprcl[spzuid][comsector] += iemp
                byprcl[spzuid][19] += iemp

            if len(byprcl) == 0:
                sqlstr = "select spzid22, scaguid16, coalesce(mxes, 0) from rtp20.by_parcel "
                sqlstr += "where gp16 > 1199 and ct2 = '%s' " % ct2
                sqlstr += "order by spzid22, scaguid16;"
                cursor.execute(sqlstr)

                for r in cursor.fetchall():
                    spzid, uid, mxemp = r
                    spzuid = (spzid, uid)
                    byprcl[spzuid] = [0] * 19 + [mxemp]

            if len(byprcl) == 0:  # residential parcels only in this ct2.  then which housing should hold employment?
                sqlstr = "select spzid22, scaguid16, coalesce(mxes, 0) from rtp20.by_parcel "
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
            i = 0
            for spzuid in byprcl.iterkeys():
                spzid, uid = spzuid
                for j in range(19):
                    if seed[i][j] > 0:
                        outQ.put((ct2, spzid, uid, seed[i][j], j))
                i += 1
        print ct2
    return


if __name__ == '__main__':

    nprocessors = 6
    manager = multiprocessing.Manager()
    in_Q    = manager.Queue()
    outQ    = manager.Queue()
    pool    = multiprocessing.Pool(processes = nprocessors)
    watcher = pool.apply_async(writer, (outQ,))

    jobs = []
    for p in range(nprocessors):
        job = pool.apply_async(theworker, (in_Q, outQ,))
        jobs.append(job)

    #geoid = get_geoid()
    finn = open('C:/SED2020/LocalInput/emp16_070819/emp16_13sec_ct2_070819.csv', 'r')
    fcsv = csv.reader(finn)
    _ = fcsv.next()

    for r in fcsv:
        ct2 = r[4]
        sum_ct2 = int(r[5])
        if sum_ct2 == 0: continue

        sed_ct2 = [int(x) for x in r[6:21]]
        icounty = 1 if ct2[0]=='9' else int(ct2[0])-1
        emps_ct2 = convert(icounty, sed_ct2, 'to_com')
        in_Q.put((ct2, emps_ct2,))
    finn.close()

    for job in jobs:
        job.get()

    for p in range(nprocessors-1):
        in_Q.put('KILL')
    outQ.put('KILL')

    pool.terminate()
    pool.join()
    print('done')
