** RUNNING IMPORT SUBFUNCTION **

*five arguments were passed to this function. Give them logical names.
local table "`1'"
local table_type "`2'"
local loc_layout "`3'"
local loc_data "`4'"
local keepvars "`5'"

**import layout and select rows based on table name
import excel "`loc_layout'", sheet("`table_type'") firstrow clear
keep if TableName == "ut" + "`table'"
drop TableName DateType MaxLength
order column_id
rename FieldName varname
rename column_id varnum

**Number of variables in table:
count
local tablelength = r(N)

**shorten variable names due to 32 character limit
replace varname = subinstr(varname,"Borrower","Brwr", .)
replace varname = subinstr(varname,"Original","Orig", .)
replace varname = subinstr(varname,"Address","Addr", .)
replace varname = subinstr(varname,"Census","", .)

**save variable names to local var (see https://www.statalist.org/forums/forum/general-stata-discussion/general/420146-rename-variables-based-on-a-key-in-another-dta-file)
local variable_names
forvalues j = 1/`tablelength' {
	local variable_names `variable_names' `=varname[`j']'
}

**Name the table to import
local import_table = "`loc_data'"+"\"+"`table_type'"+"\"+"`table'.txt"
display "`import_table'"

**import the requested table.
**this may take a while...
import delimited `import_table', delimiter("|") varnames(nonames) clear

**rename all variables
forvalues j = 1/`tablelength' {
	display "Renaming v`j': `:word `j' of `variable_names''"
	rename v`j' `:word `j' of `variable_names''
}
**keep only variables from keepvars list
display "`keepvars'"
keep `keepvars'

** END OF IMPORT SUBFUNCTION **
