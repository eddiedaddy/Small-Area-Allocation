# Small-Area-Allocation
Breaking down the zonal SED to smaller - parcel geography

## Background
Socio-Economic data (i.e., number of households, population and employment - who are working in the geography) are produced in aggregated form for given zonal boundary system.  Since the ultimate use of data is for transportation model in MPOs, it used to be organized in the Traffic Analysis Zone (TAZ) system. As recent modeling requires finer granualty of data, its associated geography could be much finer.  For example, the Activity Based Model (ABM) in MPOs models travel schedules of individual households and its members.  So the population data is in virtually person-level.  Since finer geographic boundary is available via County's tax assessors' parcel data, it is natural to allocate the individuals to know smallest geography for base year.  Assuming there is no change in parcel boundary, forecasted SED can also be allocated too.  

Technically it is not easy to maintain consistency of SED at the parcel level. Many of models/techniques in SED development are focusing on manintain spatial consistency.  In aggregated form, forecast of TAZ level aggregation deals with marginals - growth or reduction so there are stable relationship between the base year and future.  When dealing with indivual entities - households and parcel, if a parcel has been used by a households, future should also use that specific parcel, while the household character should be changed for the planinng horizon.

Finding out the future land use of employment is even more fuzzy process. 
... will work on later

## This package is
This package containes four scripts, and each one is for the alloction of base year household, base year employment, forecasted household and forecasted employment on parcel

For households, this code relies on the results from PopSyn - individual household records at TAZ.
For employment, it is use the TAZ level employment estimate and forecast in 15 (+2) sectors.

## How ?
