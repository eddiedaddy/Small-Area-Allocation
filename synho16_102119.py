'''
allocate extended synthetic households for 2016 to parcels

demand control = master_ct2_SouthGate_070819
    This is 141 HO less than the synthetic households (070819 version).
    South Gate base year had updated after the PopSyn


demand :
    C:/SED2020/Scenarios/Baseyear/expand_hh_16_BY_070819.csv
    weight = 1, so the number of rows = 6011672

    but South Gate base year (after 070819) is less 141 ho in the master file (master_ct2_SouthGate_070819)
    so, it is better to read the master file to determine how many HOs should be allocated.

    Columns =
    hhid(0), SERIALNO(1), HCOUNTY(2), PUMA10(3), HTIER2TAZID(=t2, 4), HTIER2TAZSEQ(5), RT(6),
    HHSIZE(7), HHINC(8), HTYPE(9), TEN(10)

    there is no direct way to split the the synthetic ho into ct2.  It is done by control the supply


supply : table from PG DB

    parcel16.parcel16_nov18 = whole list of the parcels in the ct2
    rtp20.by_parcel = parcels used in previous RTP for 2012 and all other scenarios
    rtp20.dmphu_by_property = parcels with (building) units from 2016 parcel property

    query for each member ct2 in the t2a.

    sqlstr =
    with x as (select scaguid16, spzid22, lu16, gp16_scag as gp16, zn16_scag as zn16, shape_area as area
               from parcel16.parcel16_nov18 where ct2='328071000094759'),
         y as (select scaguid16, sum(coalesce(est_units,0)) as units, sum(val_assd) as value,
               sum(building_sqft) as bldg
               from rtp20.dmphu_by_property where ct2='328071000094759' group by scaguid16)
    select x.scaguid16, x.spzid22, x.lu16, x.gp16, x.zn16, x.area, b.ho12, b.mxho, y.units, y.value, y.bldg
    from x left join rtp20.by_parcel b on x.scaguid16=b.scaguid16
           left join y on x.scaguid16=y.scaguid16;

    scagui16(0), spzid22(1), lu16(2), gp16(3), zn16(4), area(5), ho12(6), mxho(7), units(8), value(9), bldg(10)


clearing:
    loop for t2:
        loop for ct2 in t2

    0. calculate max from ho12, mxho and units in supply side
        if(and(ho12<2, mxho<2, units<2), mxho, max(ho12, mxho, units))
        meaning that units cannot be trustyworth when there was nothing or very small in ho12 or mxho

    1.1 matching supply to demand (of ct2) by the order of ho12, mxho, and max
        compare ho16 to one by one in the order of sum(ho12), sum(mxho), and sum(max)
        if h16@ct2 < sum(ho12) then calculate fraction to make sum(ho12) * fraction = h16
        elif h16@ct2 < sum(mxho), then supply for sum(ho12) + {sum(mxho) - sum(ho12)}*fraction = h16
        elif h16@ct2 < sum(max) then sum(mxho) + {sum(max) - sum(mxho)}*fraction = h16
        else sum(mxho) * fraction = h16

    1.2 integerization

    2. sort demand by htype(desc), hincome(asc), hsize(asc)
    3. sort supply by lu16(desc), value(asc), bldg(asc), matching units
    4. writing out the match



the allocation of forecast consider 2016 allocation done by this code

'''

import csv
import psycopg2
import tqdm


#                    ct2:               ct2            uid          spzid           lu16  value bldg unit
not_found = {'601763009999915':['601763009999915', '1110207573', '601763000150001', 2400, 0, 0, 2]}


def ct2control():
    # master_ct2_SouthGate_070819
    finn = open('C:/SED2020/Scenarios/Baseyear/master_ct2_SouthGate_070819.csv', 'r')
    fcsv = csv.reader(finn)

    hcontrol = {}  # {t2:[(ct2, h16), ...]}
    _ = fcsv.next()
    for r in fcsv:
        t2 = r[3]
        ct2 = r[4]
        h16 = int(r[5])

        if t2 not in hcontrol:
            hcontrol[t2] = []

        hcontrol[t2].append((ct2, h16))

    finn.close()
    return hcontrol


def getSupply(cursor, controls):

    seq = 0
    supply_t2 = []
    for ct2, ho16 in controls:
        if ho16 == 0: continue

        if ct2 in not_found:
            supply_t2.append([seq] + not_found[ct2])
            seq += 1
            continue

        supply_attr = []
        supply_unit = []

        sqlstr  = "with x as (select scaguid16, spzid22, lu16, gp16_scag as gp16, zn16_scag as zn16, shape_area as area"
        sqlstr += "           from parcel16.parcel16_nov18 where ct2='%s'), " % ct2
        sqlstr += "     y as (select scaguid16, sum(coalesce(est_units,0)) as units, sum(val_assd) as value, "
        sqlstr += "           sum(building_sqft) as bldg from rtp20.dmphu_by_property "
        sqlstr += "           where ct2='%s' group by scaguid16) " % ct2
        sqlstr += "select x.scaguid16, x.spzid22, x.lu16, x.gp16, x.zn16, x.area, "
        sqlstr += "       coalesce(b.ho12, 0)::integer as ho12, coalesce(b.mxho, 0)::integer as mxho, "
        sqlstr += "       coalesce(y.units, 0)::integer as units, coalesce(y.value, 0)::bigint as value, "
        sqlstr += "       coalesce(y.bldg, 0)::bigint as bldg "
        sqlstr += "from x left join rtp20.by_parcel b on x.scaguid16=b.scaguid16 "
        sqlstr += "       left join y on x.scaguid16=y.scaguid16;"
        cursor.execute(sqlstr)

        sumho12, summxho, summaxh = 0, 0, 0
        for r in cursor.fetchall():
            ho12 = r[6]
            mxho = r[7]
            hu   = r[8]
            value = r[9]
            bldg = r[10]

            if ho12<2 and mxho<2 and hu==1 and (bldg==0 and value==0):
                maxh = mxho
            else:
                maxh = max(ho12, mxho, hu, 1)

            #if maxh == 0: continue

            sumho12 += ho12
            summxho += mxho
            summaxh += maxh

            uid, spzid, lu16, value, bldg = r[0], r[1], r[2], r[9], r[10]
            supply_attr.append([ct2, uid, spzid, lu16, value, bldg, 0])  # 0 to placehold for matching units
            supply_unit.append([ho12, mxho, maxh])

        if len(supply_unit) == 1 and summaxh == 0:
            summaxh = 1
            supply_unit[0][2] = 1

        if sumho12 + summxho + summaxh == 0:
            print '%s has no space to allocated %d' % (ct2, ho16)
            exit(0)

        if ho16 <= sumho12:
            ratio = 1.0 * ho16 / sumho12
            funct = eval('lambda i: supply_unit[i][0] * ratio', locals())
        elif ho16 <= summxho:
            ratio = 1.0 * (ho16 - sumho12) / (summxho - sumho12)
            funct = eval('lambda i: supply_unit[i][0] + (supply_unit[i][1] - supply_unit[i][0]) * ratio', locals())
        else:
            if summaxh == summxho:
                if summaxh == sumho12:
                    ratio = 1.0 * ho16 / sumho12
                    funct = eval('lambda i: supply_unit[i][0] * ratio', locals())
                else:
                    ratio = 1.0 * (ho16 - sumho12) / (summaxh - sumho12)
                    funct = eval('lambda i: supply_unit[i][0] + (supply_unit[i][2] - supply_unit[i][0]) * ratio', locals())
            else:
                ratio = 1.0 * (ho16 - summxho) / (summaxh - summxho)
                funct = eval('lambda i: supply_unit[i][1] + (supply_unit[i][2] - supply_unit[i][1]) * ratio', locals())

        nparcel = len(supply_unit)
        residual = 0.0
        for i in range(nparcel):
            fvalue = funct(i)
            ivalue = int(fvalue)
            if residual + fvalue - ivalue >= 0.5:
                ivalue += 1

            supply_attr[i][6] = ivalue
            residual += fvalue - ivalue

        # append the ct2 supply to t2-level list
        for supply in supply_attr:
            if supply[6] > 0:
                supply_t2.append([seq] + supply)
                seq += 1

    return supply_t2


def do_the_work(cursor, controls, Demand, output):
    # synthetic ho could be more than the control.
    # keep all the synthetic ho without dropping any.
    # The supply (units on parcels) are established controlled
    # so, just match the supply to the demand (i.e., looping over the supply)

    t2ho16 = 0
    for _, ho16 in controls:
        t2ho16 += ho16

    Supply = getSupply(cursor, controls)

    # matching

    dscore = [x[0] for x in sorted(Demand, key=lambda x: (x[3], -x[4], -x[5]))]  # by the order of htype, hinc(desc), hsize(desc)
    sscore = [x[0] for x in sorted(Supply, key=lambda x: (x[4], -x[5], -x[6]))]  # by the order of lu16, value(desc), bldg(desc)

    d = 0
    for s in sscore:
        _, ct2, uid, spzid, _, _, _, nunits = Supply[s]
        for j in range(nunits):
            _, hhid, serial, htype, hinc, hsize = Demand[dscore[d]]
            onerow = [hhid, serial, htype, hinc, hsize, ct2, spzid, uid]
            output.writerow(onerow)
            #print onerow
            d += 1


if __name__ == '__main__':

    conn = psycopg2.connect(database='working', user='postgres', password='postgres', host='localhost')
    cursor = conn.cursor()

    hcontrol = ct2control()
    finn = open('C:/SED2020/LocalInput/hh16_070819/expand_hh_16_BY_070819.csv', 'r')
    fcsv = csv.reader(finn)

    fout = open('C:/SED2020/LocalInput/hh16_070819/syn_parcel_070819.csv', 'w')
    ocsv = csv.writer(fout, lineterminator='\n')
    ocsv.writerow(['hhid','serial','htype','hinc','hsize','ct2','spzid22','scaguid16'])

    pbar = tqdm.tqdm(total = 10479)
    _ = fcsv.next()
    prevT2 = ''
    t2demand = []

    while 1:
        try:
            r = fcsv.next()
        except StopIteration:
            break

        t2 = r[4]
        if prevT2 != t2:
            pbar.update(1)
            if len(t2demand) > 0:
                controls = hcontrol[prevT2]
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
    do_the_work(cursor, controls, t2demand, ocsv)

    fout.close()
    print('\ndone')
