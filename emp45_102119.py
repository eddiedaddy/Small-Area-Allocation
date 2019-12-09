'''

    emp16_102119.py creates emp16 in common sector at parcel (and restored into the PGDB).

    The used parcels in the base year should be recycled for the forecasting.  And same idea should be applied to any intermediate years.
    For example, if emp20@ct2 is available, then emp16@parcel could be referred to find out used parcels.  And then emp25@ct2 should be
    the allocated based on emp20@parcel.

    However, while the SED work is on the way, it is unknown which intermediate years would be done or available.
    So, this code uses generic names of "emp0_prl" to refer to the previous emp @ parcels, and "emp1_ct2" to forecast emp to be allocated into parcels.
    In an extreme case of scenario, emp0_prl could be use emp16@prl, and emp1_ct2 is emp45@ct2.

    This code does following

    = get the emp forecast at ct2 in sector 13(+2), convert to the common sector (20)  This is emp1_ct2.
    = get emp0_prl from DB in common sector, and create the emp0 table, and get the column sum
    = marginal = emp1_ct2 - sum(emp0_prl)
        - if marginal < 0:
            just work with the emp0 table, reduce by the fraction of margin/sum(emp0_prl)
        - if marginal > 0:
            . read mxes from rtp20.by_parcel for additional capcity from last RTP scenarios.
                if there is enough capacity, use them all (enough means margin/residual capcity < 2)
                if it is not enough, use total capacity and total demand for allocation (to use the ratio of existing allocation)

    = what to do about the IGR?
        - few CT2 in LA county have preallocation @ spz.  for those ct2, search for parcels within spzs....  not clear yet

'''

#%%
# from collections import OrderedDict
from sectorconverter import convert, COMSector
import csv
import iIPF
import psycopg2
import tqdm

countyfips = {1:25, 2:37, 3:59, 4:65, 5:71, 6:111, 9:37}
db_emp0 = 'rtp20.emp16_070819'  # ct2, spzid22, scaguid16, iemp, com_sect

notfound = {'203572005515606':['203572551560004', '0370536287',   1],
            '328984005398059':['328984539800002', '0590508531',   1],
            '330802006900059':['330802690000002', '0590660305',   4],
            '327221009999959':['327221000590001', '0590142238',  71],
            '330721009999959':['330721000590002', '0590147711',  16],
            '431241002123003':['431241212300001', '0650072172', 201],
            '433705006711203':['433705671120001', '0650373281',   3],
            '432601009999903':['432601000030001', '0650158386',   1],
            '539051000029671':['539051002960003', '0710705439',   1],
            '539221000029671':['539221002960001', '0710627346',   4],
            '538602003343471':['538602334340001', '0710426336', 169],
            '539389003343471':['539389334340001', '0710827308', 528],
            '539523003343471':['539523334340001', '0710582456',   4],
            '537431006046671':['537431604660003', '0710221441', 566],
            '538552009999971':['538552000710002', '0710160297',   1]}

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


def get_emp0(ct2, cursor):
    sqlstr = "select spzid22, scaguid16, com_sect, iemp from %s where ct2='%s';" % (db_emp0, ct2)
    cursor.execute(sqlstr)
    emp0 = {}

    for r in cursor.fetchall():
        spzid, uid, com_sect, iemp = r
        spzuid = (spzid, uid)
        if spzuid not in emp0:
            emp0[spzuid] = [0] * 19

        emp0[spzuid][com_sect] = iemp

    return emp0


def get_emp1(ct2, cursor):
    sqlstr  = "select spzid22, scaguid16, gp16, coalesce(mxes, 0)::integer, shape_area from rtp20.by_parcel "
    sqlstr += "where ct2='%s' ;" % ct2
    cursor.execute(sqlstr)

    count = 0
    prcl1 = {}  # only non-res parcels
    for r in cursor.fetchall():
        spzid, uid, gp16, maxemp, farea = r
        if 1199 < gp16 < 2000 or maxemp > 0:
            if maxemp < 4:
                if count % 4 == 0:
                    spzuid = (spzid, uid)
                    prcl1[spzuid] = (maxemp*0.1, farea, gp16)
                count += 1
            else:
                spzuid = (spzid, uid)
                prcl1[spzuid] = (maxemp*0.1, farea, gp16)

    return prcl1


def theWorker(input, cursor, outcsv):
    ct2, emp1_ct2 = input

    emp0_prl = get_emp0(ct2, cursor)  # baseyear seed
    emp1_    = get_emp1(ct2, cursor)  # part of future year rowsum

    # Up to this point, we have followings
    # 1. emp1_ct2 : pr_colsum -- target to match
    # 2. emp0_prl : by_seed
    # 3. emp1_    : part of pr rowsum.

    # get the whole list of possible spz+parcels
    idset = sorted(set(emp0_prl.keys() + emp1_.keys()))
    nspzuid = len(idset)

    # just create seed table with only emp0_prl (where there is some in baseyear), and zeros for newly added parcels
    seed = []
    colsum = [0] * 19
    for spzuid in idset:
        if spzuid in emp0_prl:
            seed.append(emp0_prl[spzuid])
            for j in range(19):
                colsum[j] += emp0_prl[spzuid][j]
        else:
            seed.append([0] * 19)

    # now compare the baseyear and forecast year's rowsum and colsum
    colmargin = [x - y for x, y in zip(emp1_ct2, colsum)]

    rowsum_positive = [0] * nspzuid
    rowsum_negative = [0] * nspzuid  # if colmargin is negative

    for i in range(nspzuid):
        for j in range(19):
            if colmargin[j] >= 0:
                rowsum_positive[i] += seed[i][j]
            elif colmargin[j] < 0:
                rowsum_negative[i] += seed[i][j]

    # 1. let's deal the negatives first because it is easier
    #    leave the output in the seed - base year table
    for j in range(19):
        if colmargin[j] >= 0: continue
        ratio = 1.0 * emp1_ct2[j] / colsum[j]
        assert 0 <= ratio < 1, 'something wrong with negative colmargin %s' % ct2
        residual = 0.0
        for i in range(nspzuid):
            fvalue = seed[i][j] * ratio
            ivalue = int(fvalue)
            if residual + fvalue - ivalue >= 0.5:
                ivalue += 1
            seed[i][j] = ivalue
            residual += fvalue - ivalue

    # 2. now it is time to deal with positive growth.
    # 2.1 but let's if there are something to rely on to allocate the growth
    #     1) in seed, the sum of cells of which colmargin > 0 is the total of base year allocation
    #     2) sum(emp1_[0]) is addtional capacity
    #     3) sum(emp1_[1])/ 101.82 is possible emp over the space
    #   1) is already in the seed, so, let's use as it is.
    #   get 2).  if 1) + 2) is zero, then get 3) and use it.  This is case where base year = 0, and growth > 0.

    sum_emp0_plus = sum(rowsum_positive)
    sum_emp1_mxes, sum_emp1_farea = 0, 0
    for val in emp1_.itervalues():
        sum_emp1_mxes  += val[0]
        # sum_emp1_farea += val[1] / 101.82 * 0.01   # 101.82 = sqmeter /emp.  0.01 = reducing factor

    rowmargin = []
    if sum_emp0_plus + sum_emp1_mxes == 0:  # need the area-based capacity
        for i in range(nspzuid):
            if idset[i] in emp1_:
                rowmargin.append(emp1_[idset[i]][1] / 101.82 * 0.01)
            else:
                rowmargin.append(0.0)
    else:
        for i in range(nspzuid):
            sum_negative = 0
            sum_positive = 0
            for j in range(19):
                if colmargin[j] < 0:
                    sum_negative += seed[i][j]
                else:
                    sum_positive += seed[i][j]

            if idset[i] in emp1_:
                rowmargin.append(max(0, sum_positive, emp1_[idset[i]][0]))
            else:
                rowmargin.append(max(0, sum_positive))


    # # the other columns for positive growth.  Let's do iIPF all of the positive columns together
    # rowmargin = []
    # seedrowsum = []  # just in case when parcel capa has no addition
    # for i in range(nspzuid):
    #     sum_negative = 0
    #     sum_positive = 0
    #     for j in range(19):
    #         if colmargin[j] < 0:
    #             sum_negative += seed[i][j]
    #         else:
    #             sum_positive += seed[i][j]
    #
    #     if idset[i] in emp1_:
    #         rowmargin.append(max(0, sum_positive, 0.1*(emp1_[idset[i]][0]-sum_negative)))  # reduce importance of additional capacity.  rely more on the base year allocation
    #     else:
    #         rowmargin.append(max(0, sum_positive))
    #     seedrowsum.append(sum_positive)


    #  matching the rowmargin to the colmargin
    sumcolmargin = sum([x if x > 0 else 0 for x in colmargin])
    sumrowmargin = sum(rowmargin)
    if sumrowmargin == 0:
        if ct2 in notfound:
            for j in range(19):
                if emp1_ct2[j] == 0: continue
                outcsv.writerow([ct2, notfound[ct2][0], notfound[ct2][1], j, emp1_ct2[j]])
        else:
            print 'no parcel to add ct2=%s, %d' % (ct2, sum(emp1_ct2))
        return

    residual = 0.0
    ratio    = 1.0 * sumcolmargin / sumrowmargin
    for i in range(nspzuid):
        fvalue = rowmargin[i] * ratio
        ivalue = int(fvalue)
        if residual + fvalue - ivalue >= 0.5:
            ivalue += 1
        rowmargin[i] = ivalue
        residual += fvalue - ivalue


    tempseed = []
    temprow = []
    for i in range(nspzuid):
        onerow = []
        for j in range(19):
            if colmargin[j] > 0:
                onerow.append(seed[i][j])
        tempseed.append(onerow)
    for j in range(19):
        if colmargin[j] > 0:
            temprow.append(colmargin[j])


    ipf = iIPF.IPF_2dInteger(tempseed, rowmargin, temprow)
    if ipf is None:
        print 'something is wrong with ipf for %s' % ct2
        exit()
    ipf.report = True
    ipf.iIPF_column_base()
    growth = ipf.intA


    for i in range(nspzuid):
        spzuid = idset[i]
        k = 0
        for j in range(19):
            if colmargin[j] <= 0:
                if seed[i][j] > 0:
                    outcsv.writerow([ct2, spzuid[0], spzuid[1], j, seed[i][j]])
            elif colmargin[j] > 0:
                if seed[i][j] + growth[i][k] > 0:
                    outcsv.writerow([ct2, spzuid[0], spzuid[1], j, seed[i][j] + growth[i][k]])
                k += 1
    return


if __name__=='__main__':

    geoid = get_geoid()

    finn = open('C:/SED2020/LocalInput/emp16_070819/Emp45_LI_070819_controlled.csv', 'r')
    foutprl = open('C:/SED2020/LocalInput/emp16_070819/emp45_LI_parcel.csv', 'w')

    csvfinn = csv.reader(finn)
    csvfout = csv.writer(foutprl, lineterminator='\n')

    csvfout.writerow(['ct2', 'spzid', 'scaguid16', 'comsect', 'iemp'])

    conn = psycopg2.connect(database='working', user='postgres', password='postgres', host='localhost')
    cursor = conn.cursor()
    toCom_index = COMSector.to_index

    _ = csvfinn.next()

#%%
    #pbar = tqdm.tqdm(total=13257)
    count = 0
    for r in csvfinn:
        #pbar.update(1)
        ct2 = r[4]
        if ct2 != '140691000971025': continue
        sum_ct2 = int(r[5])
        if sum_ct2 == 0: continue

        count += 1
        print count, ct2
        sed_ct2 = [int(x) for x in r[6:21]]
        icounty = 1 if ct2[0] == '9' else int(ct2[0])-1
        emp1_ct2 = convert(icounty, sed_ct2, 'to_com')
        # in_Q.put((ct2, emp1_ct2,))
        theWorker((ct2, emp1_ct2), cursor, csvfout)

    finn.close()
    print '\ndone'
