'''
Like emp45 considers the sectors via brute-forced IPF, it is desired to maintain the housing type
in allocating the households.

However, it is not clear if the PopSyn maintains its consistency (although the T2 level control does)
So, at this point, if the demand by housing type is stable enough to assume.
Therefore, just try to match the number of household at each parcel - parcel-level number of household
follows the direction of TAZ controls (and thus PopSyn output of number of households)
Then, if TAZ increases, parcel household is same or greater than base year.
Or, if TAZ decreases, households from base year allocation are taken out.


The controls are given at ct2, so Increase/Decrease can be checked at ct2 level
But the demand (popsyn output) is given at T2.

So loop strucuture for list out the supply is
    for each t2
        for each ct2 in t2
            if h45  > h16: search for additional supply
            if h45 == h16: take over the scaguid16 used by h16
            if h45  < h16: pick samples scaguid16 from h16

        supply = union of all the parcels from above three cases
        demand = from popsyn
        match all together at T2 level.
        
It is desired to be in multiprocessing, but was not able at this time.
- It is bit too much to use a pipe to deliver the individual synthetic household data to each processor for its sheer size.
- It is hard to bring the reading step of synthetic data from a CSV into each processor because the disorganized nature
  (synthetic data was also created from pypPopSyn-III  which is a multiprocessing process)
- Synthetic data should be re-organized, or read from a DB by Tier2 to make use of multiprocessing.

'''


import psycopg2
import csv
import tqdm

basetable = 'rtp20.syn16_070819'
pwd = 'C:/SED2020/Scenarios/2_Local_Input/'

#                    ct2:       seq        ct2            uid          spzid           lu16  value bldg unit
not_found = {'140861000987825':[['0250031845', '140861098780001', 2100,   0,   0,   1]],
             '203951004400008':[['0370247050', '203951440000001', 1110,   0,   0, 150]],
             '431802009992603':[['0650072175', '431802999260001', 1800,   0,   0,   5]],
             '433033005670003':[['0650270952', '433033567000001', 3100,   0,   0, 110]],
             '434315009999903':[['0650756052', '434315000030001', 3100,   0,   0,   6]],
             '600244006504215':[['1110026843', '600244650420001', 3100,   0,   0,  37]] }

# this one was already included in syn16 via the syn16_102119.py
# '601763009999915':['601763009999915', '1110207573', '601763000150001', 2400, 0, 0, 2]



def ct2control():
    # The last popsyn run was based on master_ct2_070819.  it is about 5K more than master_ct2_SouthGate_070819
    finn = open('C:/SED2020/Scenarios/2_Local_Input/master_ct2_070819.csv', 'r')
    fcsv = csv.reader(finn)

    hcontrol = {}  # {t2:[(ct2, h45, h16), ...]}
    _ = fcsv.next()
    for r in fcsv:
        t2 = r[3]
        ct2 = r[4]
        h45 = int(r[5])
        h16 = int(r[9])

        if t2 not in hcontrol:
            hcontrol[t2] = []

        hcontrol[t2].append((ct2, h45, h16))

    finn.close()
    return hcontrol


def get_baseallocation(cursor, ct2):
    sqlstr  = "with x as (select spzid22, scaguid16, count(*) as ho16 from %s " % basetable
    sqlstr += "               where ct2='%s' " % ct2
    sqlstr += "               group by spzid22, scaguid16 order by scaguid16), "
    sqlstr += "     y as (select scaguid16, sum(val_assd) as value, "
    sqlstr += "               sum(building_sqft) as bldg from rtp20.dmphu_by_property "
    sqlstr += "               where ct2='%s' group by scaguid16), " % ct2
    sqlstr += "     z as (select scaguid16, lu16 from parcel16.parcel16_nov18 where ct2='%s') " % ct2
    sqlstr += "select x.scaguid16, x.spzid22, z.lu16, coalesce(y.value, 0)::integer as value,"
    sqlstr += "     coalesce(y.bldg, 0)::integer as bldg, x.ho16 "
    sqlstr += "    from x left join y on x.scaguid16=y.scaguid16 left join z on x.scaguid16=z.scaguid16; "
    cursor.execute(sqlstr)

    # r = (scaguid16, spzid22, lu16, value, bldg, ho16)
    return cursor.fetchall()


def get_additionalCapacity(cursor, ct2, sumho45):
    if ct2 in not_found:
        not_found[ct2][0][5] = sumho45
        return not_found[ct2]

    supply_attr = []
    supply_unit = []

    sqlstr  = "with x as (select scaguid16, spzid22, lu16, gp16_scag as gp16, zn16_scag as zn16, shape_area as area"
    sqlstr += "               from parcel16.parcel16_nov18 where ct2='%s'), " % ct2
    sqlstr += "     y as (select scaguid16, sum(coalesce(est_units,0)) as units, sum(val_assd) as value, "
    sqlstr += "               sum(building_sqft) as bldg from rtp20.dmphu_by_property "
    sqlstr += "               where ct2='%s' group by scaguid16), " % ct2
    sqlstr += "     z as (select scaguid16, spzid22, count(*) as ho16 from %s " % basetable
    sqlstr += "               where ct2='%s' group by spzid22, scaguid16 ) " % ct2
    sqlstr += "select x.scaguid16, x.spzid22, x.lu16, x.gp16, x.zn16, x.area, "
    sqlstr += "     coalesce(b.ho12, 0)::integer as ho12, coalesce(b.mxho, 0)::integer as mxho, "
    sqlstr += "     coalesce(y.units, 0)::integer as units, coalesce(y.value, 0)::bigint as value, "
    sqlstr += "     coalesce(y.bldg, 0)::bigint as bldg, coalesce(z.ho16, 0)::integer as ho16 "
    sqlstr += "from x left join rtp20.by_parcel b on x.scaguid16=b.scaguid16 "
    sqlstr += "       left join y on x.scaguid16=y.scaguid16"
    sqlstr += "       left join z on x.scaguid16=z.scaguid16;"
    cursor.execute(sqlstr)

    sumho16, summxho, summaxh = 0, 0, 0
    for r in cursor.fetchall():
        ho12  = r[6]
        mxho  = r[7]
        hu    = r[8]
        value = r[9]
        bldg  = r[10]
        ho16  = r[11]

        if ho12<2 and mxho<2 and hu==1 and (bldg==0 and value==0):
            maxh = max(ho16, mxho)
        else:
            maxh = max(ho12, ho16, mxho, hu, 1)

        #if maxh == 0: continue

        sumho16 += ho16
        summxho += mxho
        summaxh += maxh

        uid, spzid, lu16, value, bldg = r[0], r[1], r[2], r[9], r[10]
        supply_attr.append([uid, spzid, lu16, value, bldg, 0])  # 0 to placehold for matching units
        supply_unit.append([ho16, mxho, maxh])


    if len(supply_unit) == 1 and summaxh == 0:
        summaxh = 1
        supply_unit[0][2] = 1

    if sumho16 + summxho + summaxh == 0:
        print '%s has no space to allocated %d' % (ct2, sumho45)
        exit(0)

    if sumho45 <= sumho16:
        ratio = 1.0 * sumho45 / sumho16
        funct = eval('lambda i: supply_unit[i][0] * ratio', locals())
    elif sumho45 <= summxho:
        ratio = 1.0 * (sumho45 - sumho16) / (summxho - sumho16)
        funct = eval('lambda i: supply_unit[i][0] + (supply_unit[i][1] - supply_unit[i][0]) * ratio', locals())
    else:
        if summaxh == summxho:
            if summaxh == sumho16:
                ratio = 1.0 * sumho45 / sumho16
                funct = eval('lambda i: supply_unit[i][0] * ratio', locals())
            else:
                ratio = 1.0 * (sumho45 - sumho16) / (summaxh - sumho16)
                funct = eval('lambda i: supply_unit[i][0] + (supply_unit[i][2] - supply_unit[i][0]) * ratio', locals())
        else:
            ratio = 1.0 * (sumho45 - summxho) / (summaxh - summxho)
            funct = eval('lambda i: supply_unit[i][1] + (supply_unit[i][2] - supply_unit[i][1]) * ratio', locals())

    nparcel = len(supply_unit)
    residual = 0.0
    for i in range(nparcel):
        fvalue = funct(i)
        ivalue = int(fvalue)
        if residual + fvalue - ivalue >= 0.5:
            ivalue += 1

        supply_attr[i][5] = ivalue
        residual += fvalue - ivalue

    returning = [x for x in supply_attr if x[5] > 0]

    return returning


def do_the_work(cursor, controls, Demand, ocsv):

    Supply = []
    seq = 0
    for ct2, ho45, ho16 in controls:
        if ho45 == ho16:
            if ho16 > 0:
                syn16 = get_baseallocation(cursor, ct2)
                for r in syn16:
                    Supply.append([seq, ct2]+list(r))
                    seq += 1

        else: # ho45 != ho16
            maxcapac = get_additionalCapacity(cursor, ct2, ho45)
            for r in maxcapac:
                Supply.append([seq, ct2]+r)
                seq += 1

    dscore = [x[0] for x in sorted(Demand, key=lambda x: (x[3], -x[4], -x[5]))]  # by the order of htype, hinc(desc), hsize(desc)
    sscore = [x[0] for x in sorted(Supply, key=lambda x: (x[4], -x[5], -x[6]))]  # by the order of lu16, value(desc), bldg(desc)

    d = 0
    for s in sscore:
        _, ct2, uid, spzid, _, _, _, nunits = Supply[s]
        for j in range(nunits):
            _, hhid, serial, htype, hinc, hsize = Demand[dscore[d]]
            onerow = [hhid, serial, htype, hinc, hsize, ct2, spzid, uid]
            ocsv.writerow(onerow)
            #print onerow
            d += 1


if __name__ == '__main__':

    conn = psycopg2.connect(database='working', user='postgres', password='postgres', host='localhost')
    cursor = conn.cursor()

    hcontrol = ct2control()
    finn = open(pwd + 'expand_hh_45_LI_070819.csv', 'r')
    fcsv = csv.reader(finn)

    fout = open(pwd + 'syn_parcel_070819.csv', 'w')
    ocsv = csv.writer(fout, lineterminator='\n')
    ocsv.writerow(['hhid','serial','htype','hinc','hsize','ct2','spzid22','scaguid16'])

    _ = fcsv.next()
    prevT2 = ''
    t2demand = []

    pbar = tqdm.tqdm(total=10665)
    nt2done = 0
    while 1:
        try:
            r = fcsv.next()
        except StopIteration:
            break

        t2 = r[4]
        if prevT2 != t2:
            if len(t2demand) > 0:
                controls = hcontrol[prevT2]
                nt2done += 1
                #print nt2done, prevT2
                pbar.update(1)
                #if prevT2 != '14086100':
                #    t2demand = []
                #    prevT2 = t2
                #    count = 0
                #    continue
                do_the_work(cursor, controls, t2demand, ocsv)

            t2demand = []
            prevT2 = t2
            count = 0

        hhid = int(r[0])
        serial = r[1]
        hsize = int(r[7])
        hinc = int(r[8])
        htype = int(r[9])

        t2demand.append((count, hhid, serial, htype, hinc, hsize))
        count += 1

    controls = hcontrol[prevT2]
    nt2done += 1
    #print nt2done, t2
    do_the_work(cursor, controls, t2demand, ocsv)
    pbar.update(1)
    pbar = None

    fout.close()
    print('\ndone')
