/* 
Author: Matthew Stern
Date: 2.27.2019

Purpose:
	This do file selects specific variables from a selection of tables
	in the ZTRAX database, then subsets them to within a certain 
	county, specified by FIPS code. It then cleans the data and outputs 
	the selected transaction records as various .dta files for analysis.
	
Computational needs:
	Loading in the largest tables (at least for state of Illinois) seems 
	to require 32 GB of ram. Output file will be multible GB upon 
	completion. 
	
	Running this entire file, correctly specified, for Illinois and
	subsetting for Cook County (FIPS 17031), takes 2.5 hours. 


Important Notes:
	This program uses a sub function. Make sure both do files are available and the
	sub-function is specified below
	
	This program requires the 'missings' software package be installed.

	The variables specified to keep in the various 'keepvars' fields are shortened
	to account for limits on the length of varnames in Stata. Abbreviations currently
	specified are:
		Borrower --> Brwr
		Original --> Orig
		Address  --> Addr
		Census   --> ''

Future improvements to make:
	-Auto-install missings.ado?
	-Improve clarity of output_name local variables--probably just remove them 
	 and replace with "ZTrans" + "`using_fips'" etc...
*/

**cause error if missings.ado is not installed
findfile missings.ado

**set big picture variables
local loc_subfn "Z:\Chicago\MATT_CODE\import_sub_function.do"	//location of import_sub_function.do 
local loc_layout "Z:\Chicago\17_new\layout.xlsx"				//.xlsx file with ZTRAX table layouts. Should have sheets called 'ZAsmt' and 'ZTrans'
local loc_data "Z:\Chicago\17_new" 								//directory where ZTRAX data is stored. Should have folders called 'ZAsmt' and 'ZTrans'
local loc_output "Z:\Chicago\DataExtracts"						//directory to output data and logs into
local using_fips = 17031										//county to restrict data to
local parcellist = "Z:\Chicago\MATT_CODE\study_parcels.csv" 	//location of specific parcels list for final matching output

local tolog = 1		// = 1 if user wants to log to the output folder.


*start logging using current datetime, if desired
if `tolog' == 1 {
local c_date = c(current_date)
local c_time = c(current_time)
local c_time_date = "`c_date'"+"_" +"`c_time'"
local datetime = subinstr("`c_time_date'", ":", "", .)
local datetime = subinstr("`datetime'", " ", "", .)
local logfile = "`loc_output'"+"\"+"log_"+"`datetime'"+".smcl"
log using `logfile'
* display local variables already set, for the record
display "`loc_subfn'"
display "`loc_layout'"
display "`loc_data'"
display "`loc_output'"
display "`using_fips'"
}

**program definitions.
local output_name = "ZTrans" + "`using_fips'"
local output_name2 = "ZAsmt" + "`using_fips'"
local output_name3 = "ZCombined" + "`using_fips'"
set more off

cd `loc_output'

local run_this = 0
*************************
***** TEST FUNCTION *****

if `run_this' == 1 {
*this tests the sub function on a very small table.

local table = "BorrowerMailAddress"
local keepvars ""TransId BrwrMailCareOfName BrwrMailHouseNumber BrwrMailHouseNumberExt""
do `loc_subfn' `table' "ZTrans" `loc_layout' `loc_data' `keepvars'
}


local run_this = 1
*********************************
**** Import ZAsmt Main table ****

if `run_this' == 1 {
local table = "Main"
local keepvars ""RowID ImportParcelID FIPS AssessorParcelNumber UnformattedAssessorParcelNumber PropertyZoningDescription TaxAmount TaxYear LotSizeSquareFeet""

di c(current_time)
*Starting import subfunction. This could take a while.
run `loc_subfn' `table' "ZAsmt" `loc_layout' `loc_data' `keepvars'
di c(current_time)

keep if FIPS == `using_fips'

save `output_name2'.dta, replace
}


local run_this = 1
**********************************
**** Import ZAsmt Value table ****

if `run_this' == 1 {
local table = "Value"
local keepvars ""RowID LandAssessedValue ImprovementAssessedValue TotalAssessedValue AssessmentYear LandMarketValue ImprovementMarketValue TotalMarketValue MarketValueYear LandAppraisalValue ImprovementAppraisalValue TotalAppraisalValue AppraisalValueYear""

di c(current_time)
*Starting import subfunction. This could take a while.
run `loc_subfn' `table' "ZAsmt" `loc_layout' `loc_data' `keepvars'
di c(current_time)

**merge with existing dataset
merge 1:1 RowID using `output_name2'
* Where...
*    _merge==1 : record in this file but not in master file
*          (presumably because it is not in correct FIPS code)
*    _merge==2 : record in master file but there are no details to add in this file
*    _merge==3 : record in master file AND new details are in this file
		

**remove records that don't belong
drop if _merge==1
drop _merge
		
**drop fields with no values
missings dropvars, force

** fix parcelID
gen  pin_length = length(UnformattedAssessorParcelNumber)
tab pin_length
keep if pin_length == 10 | pin_length == 14
gen pin14 = UnformattedAssessorParcelNumber if pin_length == 14
replace pin14 = UnformattedAssessorParcelNumber + "0000" if pin_length == 10
gen  pin_length2 = length(pin14)
assert pin_length2 == 14
drop AssessorParcelNumber UnformattedAssessorParcelNumber pin_length pin_length2

drop FIPS TaxAmount TaxYear
order RowID ImportParcelID pin14 LotSizeSquareFeet AssessmentYear LandAssessedValue ImprovementAssessedValue TotalAssessedValue MarketValueYear TotalMarketValue 
		
**and export the master table...
save `output_name2', replace
}



local run_this = 1
********************************
** Import ZTrans PropertyInfo **

if `run_this' == 1 {
local table = "PropertyInfo"
local keepvars ""TransId FIPS AssessorParcelNumber UnformattedAssessorParcelNumber ImportParcelID AssessmentRecordMatchFlag""

di c(current_time)
*Starting import subfunction. This could take a while.
run `loc_subfn' `table' "ZTrans" `loc_layout' `loc_data' `keepvars'
di c(current_time)
	
**keeping only records in specified FIPS code...
keep if FIPS == `using_fips'

save `output_name'.dta, replace

** create a list of transactions in study area
keep TransId
duplicates drop
save  `output_name'_TransId_only.dta, replace
}


local run_this = 1
*************************
** Import ZTrans  Main **


if `run_this' == 1 {
local table = "Main"
local keepvars ""TransId DocumentTypeStndCode RecordingDate DocumentDate SignatureDate EffectiveDate SalesPriceAmount""

di c(current_time)
*Starting import subfunction. This could take a while.
run `loc_subfn' `table' "ZTrans" `loc_layout' `loc_data' `keepvars'
di c(current_time)

* could create an egen here to count number of parcels involved in each transactions
* in order to remove multi-parcel transactions later on when evaluating prices

**merge with existing dataset
merge 1:m TransId using `output_name'
* Where...
*    _merge==1 : record in this file but not in master file
*          (presumably because it is not in correct FIPS code)
*    _merge==2 : record in master file but there are no details to add in this file
*    _merge==3 : record in master file AND new details are in this file
		

**remove records that don't belong
drop if _merge==1
drop _merge
		
**and export the master table...
save `output_name', replace
}


local run_this = 1
*************************************
** Import ZTrans  BuyerMailAddress **

if `run_this' == 1 {
local table = "BuyerMailAddress"
local keepvars ""TransId BuyerMailFullStreetAddr BuyerMailCity BuyerMailZip BuyerMailSequenceNumber""

di c(current_time)
*Starting import subfunction. This could take a while.
run `loc_subfn' `table' "ZTrans" `loc_layout' `loc_data' `keepvars'
di c(current_time)

** keep only first buyer mailing address per transaction. 
** (hopefully, drop 0 or near 0 records)
drop if BuyerMailSequenceNumber > 1


merge 1:1 TransId using `output_name'_TransId_only.dta
* Where...
*    _merge==1 : address record exists but transaction is not in transactions list
*          (presumably because it is not in correct FIPS code)
*    _merge==2 : record in transactions list but no addresses associated
*    _merge==3 : record in transactions list AND address is found

**remove records that don't belong
drop if _merge != 3
drop _merge

**and export BuyerMailAddress table...
save `output_name'_BuyerMailAddress.dta, replace
}


local run_this = 1
******************************
** Import ZTrans  BuyerName **

if `run_this' == 1 {
local table = "BuyerName"
local keepvars ""TransId BuyerIndividualFullName BuyerNonIndividualName BuyerNameSequenceNumber BuyerMailSequenceNumber""

di c(current_time)
*Starting import subfunction. This could take a while.
run `loc_subfn' `table' "ZTrans" `loc_layout' `loc_data' `keepvars'
di c(current_time)

** keep only first buyer mailing address per transaction. 
** (hopefully, drop 0 or near 0 records)
drop if BuyerMailSequenceNumber > 1

** bring in buyer addresses
merge m:1 TransId using `output_name'_BuyerMailAddress.dta
* Where...
*    _merge==1 : record in name file but not in address file
*          (presumably because it is not in correct FIPS code)
*    _merge==2 : record has buyer address but not buyer name
*    _merge==3 : record has buyer name and buyer address

drop if _merge == 1
drop _merge BuyerMailSequenceNumber

** keep only first three buyer names
drop if BuyerNameSequenceNumber > 3

** generate single name field
replace BuyerNonIndividualName = BuyerNonIndividualName + " (Org)" if BuyerNonIndividualName != ""
gen BuyerName = "_noname", after(TransId)
replace BuyerName = BuyerIndividualFullName if BuyerIndividualFullName != ""
replace BuyerName = BuyerNonIndividualName if BuyerNonIndividualName != ""
drop BuyerIndividualFullName BuyerNonIndividualName 

** reshape so every transaction has only a single row
reshape wide BuyerName, i(TransId BuyerMailFullStreetAddr BuyerMailCity BuyerMailZip) j(BuyerNameSequenceNumber)


** merge with existing dataset
merge 1:m TransId using `output_name'
* Where...
*    _merge==1 : transaction has buyer details but is not in master file
*          (should be near 0)
*    _merge==2 : record in master file but there are no buyer details to add
*    _merge==3 : record in master file AND buyer details have been added

drop if _merge == 1

generate BuyerDetails = 0
replace BuyerDetails = 1 if _merge == 3
drop _merge

**and export the master table...
save `output_name', replace
}


local run_this = 1
******************************
** Import ZTrans SellerName **

if `run_this' == 1 {
local table = "SellerName"
local keepvars ""TransId SellerIndividualFullName SellerNonIndividualName SellerMailSequenceNumber SellerNameSequenceNumber""

di c(current_time)
*Starting import subfunction. This could take a while.
run `loc_subfn' `table' "ZTrans" `loc_layout' `loc_data' `keepvars'
di c(current_time)

** keep only first buyer mailing address per transaction. 
** (hopefully, drop 0 or near 0 records)
drop if SellerMailSequenceNumber > 1
drop SellerMailSequenceNumber

** keep only first three seller names
drop if SellerNameSequenceNumber > 3

** generate single seller name
replace SellerNonIndividualName = SellerNonIndividualName + " (Org)" if SellerNonIndividualName != ""
gen SellerName = "_noname", after(TransId)
replace SellerName = SellerIndividualFullName if SellerIndividualFullName != ""
replace SellerName = SellerNonIndividualName if SellerNonIndividualName != ""
drop SellerIndividualFullName SellerNonIndividualName 

** reshape so every transaction has only a single row
reshape wide SellerName, i(TransId) j(SellerNameSequenceNumber)

** merge with existing dataset
merge 1:m TransId using `output_name'
* Where...
*    _merge==1 : transaction has seller details but is not in master file
*          (presumably because it is not in correct FIPS code)
*    _merge==2 : record in master file but there are no seller details to add
*    _merge==3 : record in master file AND seller details have been added

drop if _merge == 1

generate SellerDetails = 0
replace SellerDetails = 1 if _merge == 3
drop _merge

**and export the master table...
save `output_name', replace

erase `output_name'_BuyerMailAddress.dta
}


local run_this = 1
***********************
*** Cleanup records ***

if `run_this' == 1 {

** fix parcelID
gen  pin_length = length(UnformattedAssessorParcelNumber)
tab pin_length
keep if pin_length == 10 | pin_length == 14
gen pin14 = UnformattedAssessorParcelNumber if pin_length == 14
replace pin14 = UnformattedAssessorParcelNumber + "0000" if pin_length == 10
gen  pin_length2 = length(pin14)
assert pin_length2 == 14
drop AssessorParcelNumber UnformattedAssessorParcelNumber pin_length pin_length2

** date-ify all dates
* per Zillow, DocumentDate > SignatureDate > RecordingDate
foreach var of varlist RecordingDate DocumentDate SignatureDate {
	display "working on `var'..."
	generate str4 dxyr1 = substr(`var',1,4) if `var' != ""
	generate str2 dxmo1 = substr(`var',6,7) if `var' != ""
	generate str2 dxda1 = substr(`var',9,10) if `var' != ""
	destring dx*, replace
	gen `var'_2 = mdy(dxmo1, dxda1, dxyr1)
	format `var'_2 %d
	drop dxyr1 dxmo1 dxda1 `var'
	rename `var'_2 `var'
}

** create best date
* per Zillow, DocumentDate > SignatureDate > RecordingDate
gen BestDate = DocumentDate
replace BestDate = SignatureDate if BestDate == . & SignatureDate != .
replace BestDate = RecordingDate if BestDate == . & RecordingDate != .
format BestDate %d
drop EffectiveDate SignatureDate FIPS

** create buyer type variable and remove "(Org)" from BuyerNames
gen BuyerType = ""
foreach var of varlist BuyerName* {
	display "`var'..."
	
	*create buyer type variable:
	gen str1 buyertype_builder = "-"
	replace buyertype_builder = "1" if substr(`var',-5,.)=="(Org)"
	replace buyertype_builder = "0" if buyertype_builder != "1" & `var' != ""
	replace BuyerType = BuyerType + buyertype_builder
	drop buyertype_builder
	
	*remove "(Org)"
	replace `var' = subinstr(`var', " (Org)", "", .)
}

** create seller type variable and remove "(Org)" from SellerNames
gen SellerType = ""
foreach var of varlist SellerName* {
	display "`var'..."
	
	*create buyer type variable:
	gen str1 sellertype_builder = "-"
	replace sellertype_builder = "1" if substr(`var',-5,.)=="(Org)"
	replace sellertype_builder = "0" if sellertype_builder != "1" & `var' != ""
	replace SellerType = SellerType + sellertype_builder
	drop sellertype_builder
	
	*remove "(Org)"
	replace `var' = subinstr(`var', " (Org)", "", .)
}

**order variables
order pin14 ImportParcelID TransId BestDate DocumentDate RecordingDate SalesPriceAmount DocumentTypeStndCode BuyerDetails SellerDetails BuyerMailFullStreetAddr BuyerMailCity BuyerMailZip BuyerType BuyerName1 BuyerName2 BuyerName3 SellerType SellerName1 SellerName2 SellerName3

**and export the master table...
save `output_name', replace
}


local run_this = 1
*************************************************
** Select most recent transaction per property **

if `run_this' == 1 {
** keep only most recent transaction
sort pin14 TransId
gen seq=1, before(TransId)
replace seq=seq[_n-1]+1 if pin14==pin14[_n-1]
gen nseq = -seq, before(TransId) //generates an inverse sequence rank
sort pin14 nseq //within each PIN, places last transaction first
by pin14 : keep if _n==1 //keeps first record (last transaction) per pin 
drop seq nseq

**export this table...
save `output_name'_mostrecenttrans, replace
}


local run_this = 1
****************************************
** Merge ZTrans_MostRecent with ZAsmt **

if `run_this' == 1 {
merge 1:1 pin14 using `output_name2'.dta
* Where...
*    _merge==1 : transaction in ZTrans but no record in ZAsmt
*    _merge==2 : transaction in ZAsmt but no record in ZTrans
*    _merge==3 : Record in ZAsmt and at least one trans in ZTrans

drop ImportParcelID TransId RowID AssessmentRecordMatchFlag _merge
save `output_name3'_mostrecenttrans, replace
}



local run_this = 1
**********************************************************
** Export various study area files using MostRecentTrans**

if `run_this' == 1 {
import delimited `parcellist', stringcols(2) clear 
drop objectid shape_length
rename shape_area parcel_size
merge 1:1 pin14 using `output_name3'_mostrecenttrans
* Where...
*    _merge==1 : record in study area but no transaction in ZTRAX
*    _merge==2 : record in ZTRAX but not in study area
*    _merge==3 : record in study area and has a ZTRAX transaction
drop if _merge == 2
drop _merge

** first, keep all parcels in study area
save `output_name3'_mostrecenttrans_studyarea, replace

** second, keep only residential vacant land per DPD or CMAP
keep if status_improved > 0 | landuse == "VACANT_RES" 
save `output_name3'_mostrecenttrans_studyarea_vacant, replace
export delimited using `output_name3'_mostrecenttrans_studyarea_vacant.csv, replace

** third, keep only LL transactions
keep if status_improved > 0

tab BuyerDetails
tab SellerDetails
tab DocumentTypeStndCode
tab SellerType
drop SellerName2 SellerName3

keep if DocumentTypeStndCode == "QCDE"

tab SellerName1
keep if SellerName1 == "CITY OF CHICAGO" | SellerName1 == "CITI OF CHICAGO"

tab BuyerType
drop BuyerName3 SellerType 
drop DocumentTypeStndCode BuyerDetails SellerDetails SalesPriceAmount SellerName1

drop if BestDate < date("20140101","YMD")

save `output_name3'_studyarea_LLtrans, replace
export delimited using `output_name3'_studyarea_LLtrans.csv, replace
}


local run_this = 1
***********************************************************
** Export all transactions for vacant land in study area **

if `run_this' == 1 {
import delimited `parcellist', stringcols(2) clear 
drop objectid shape_length
rename shape_area parcel_size

**merge in assessment data
merge 1:1 pin14 using `output_name2'.dta
* Where...
*    _merge==1 : Record in study area but no ZAsmt entry
*    _merge==2 : ZAsmt record, but not in study area
*    _merge==3 : Study area parcel with ZAsmt record

drop if _merge == 2
drop _merge

** merge in transaction data
merge 1:m pin14 using `output_name'.dta
* Where...
*    _merge==1 : record in study area but no transaction in ZTRAX
*    _merge==2 : record in ZTRAX but not in study area
*    _merge==3 : record in study area and has a ZTRAX transaction
drop if _merge == 2
drop _merge

drop AssessmentRecordMatchFlag RowID

** keep sales since 2000 only
gen year = year(BestDate)
drop if year < 2000

save `output_name3'_studyarea, replace

** keep if residential vacant land per DPD or CMAP
keep if status_improved > 0 | landuse == "VACANT_RES" 

save `output_name3'_studyarea_vacant, replace
}




*********************
***** Close Log *****
if `tolog' == 1 {
	log close
}
