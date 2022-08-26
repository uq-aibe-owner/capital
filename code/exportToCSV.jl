# include("preptables-pretable8specific.jl")
# for the time being this should be run with pwd being the gladstone parent file

using DataFrames, CSV, DelimitedFiles;

# other code directory (relative to present file)
codedir = "./"
include(codedir * "concordance.jl")

# Directory for output (and input while pulling template from .csv)
outputdir = "julia/output/"
# Name of column that contains the row indices
div = "ANZdiv"
tbl8 = "anztable8rr"
tbl5 = "anztable5rr"
# Pull in tables from .csv files, ultimately it will come from the RAS code
newtable8 = CSV.read(outputdir * tbl8 * ".csv", DataFrame)
newtable5 = CSV.read(outputdir * tbl5 * ".csv", DataFrame)
# Quickly mock up newtablediff from tables 5 and 8, ultimately it will come 
# from the RAS code #include -d at the top of this file
newtablediff = select(newtable8, Not(div))
for i in 1:nrow(newtablediff)
    for j in 1:ncol(newtablediff)
        newtablediff[i,j] = (newtablediff[i,j] - 
                select(newtable5, Not(div))[i,j])
    end
end
newtablediff[!, div] = newtable8[:, div]

# List of names of sectors, ultimately it will come from the RAS code 
# include -d at the top of this file
sectorCodes = ANZcode 
# ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", 
#    "N", "O", "P", "Q", "R", "S", "T"];
numdiv = length(sectorCodes);
#=============================================================================#
# General code to add Sector titles and convert to a df from a matrix 
# dataSet if that's what the input is (not the case right now)

# Here need to just get the data, no headings etc. in matrix form
# Comment out if they are already in this form
# newtable8 = Matrix(newtable8)
# newtable5 = Matrix(newtable5)
# newtablediff = Matrix(newtablediff)

# function addTitles(table)
#     df = DataFrame(table, sectorCodes);
#     insertcols!(df, 1, :Sectors => SectorCodes);
#     return df;
# end

# addTitles(newtable8)
# addTitles(newtable5)
# addTitles(newtablediff)

#=============================================================================#
# Function to get list of row indices for given sectors etc. Used only as an 
# input to the getSubFrame function at this stage
function getRows(dfColumn, lsOfSectors)
    rowVec = [];
    for i in 1:length(lsOfSectors)
        append!(rowVec, findall(x -> x == lsOfSectors[i], dfColumn)[1])
    end
    return rowVec
end
# General Function to get a sub-frame based on row and column names
# Makes the code for extracting each parameter much more readable
function getSubFrame(df, lsRows, lsCols);
    subdf = df[getRows(df[:,div], lsRows), append!([div],
    lsCols)]
    return subdf
end
# Function to get a 2-indexed set in the correct output format
function getMultiSubFrame(df, lsRows, lsCols);
    subdf = df[getRows(df[:,div], lsRows), append!([div],
    lsCols)]
    if length(lsRows) > 1 && length(lsCols) > 1
        dfNew = DataFrame(index0 = String3[], index1 = String3[], Value = 
                Float64[])
        for i in 1:nrow(select(subdf, div))
            for j in 1:length(names(select(subdf, Not(div))))
                push!(dfNew, [select(subdf, div)[i,1],
                    names(select(subdf, Not(div)))[j], 
                    select(subdf, Not(div))[i,j]])
            end
        end
        return dfNew
    else
        return subdf
    end
end
# Output a parameter as a CSV with the correct format for AMPL
function outputForm(paramName, subFrame);
    colNames = names(subFrame)
    colNames[length(colNames)] = paramName
    rename!(subFrame, colNames)
    CSV.write(outputdir*paramName*".csv", subFrame)
end
#=============================================================================#
# Calculate and export RAW parameters as .csv files

# RAW_CON_FLW - all sectors in the Q1 col of table 8
outputForm("RAW_CON_FLW", getSubFrame(newtable8, 
    sectorCodes, ["Q1"]));

# RAW_MED_FLW - all inter-sector flows table 8
outputForm("RAW_MED_FLW", getMultiSubFrame(newtable8, 
    sectorCodes, sectorCodes));

# RAW_KAP_OUT - all sectors in the P2 row of table 8
# First example of a row - hence the permutedims
outputForm("RAW_KAP_OUT", permutedims( 
    getSubFrame(newtable8, ["`P2"], sectorCodes), 1));

# RAW_LAB_OUT all sectors in the P1 row of table 8
outputForm("RAW_LAB_OUT", permutedims(
    getSubFrame(newtable8, ["`P1"], sectorCodes),1));

# RAW_MED_OUT - all sectors in the T1 row of table 8 (maybe use sum instead)
T1vec = vec(sum(Matrix(newtable8[1:numdiv, Between("A", "T")]), dims=1));
T1 = DataFrame("ANZdiv" => sectorCodes, "T1" =>  T1vec);
outputForm("RAW_MED_OUT", T1);

# RAW_DOM_CCON - all sectors in the Q1 col of table 5
outputForm("RAW_DOM_CCON", getSubFrame(newtable5, 
    sectorCodes, ["Q1"]));

# RAW_YSA_CCON - all sectors flows of table 8-5
outputForm("RAW_YSA_CCON", getSubFrame(newtablediff, 
    sectorCodes, ["Q1"]));

# RAW_DOM_CINV - all sectors in the Q3+Q4+Q5 col of table 5
Q345 = getSubFrame(newtable5,
                   sectorCodes, ["Q3", "Q4", "Q5"])
RAW_DOM_CINV = sum(eachcol(select(Q345, Not(div))))
RAW_DOM_CINV = DataFrame(div = sectorCodes, RAW_DOM_CINV = RAW_DOM_CINV)
outputForm("RAW_DOM_CINV", RAW_DOM_CINV);

# RAW_YSA_CINV - all sectors flows of kapflows table - not yet imported
# CSV.write(outputdir*"RAW_YSA_CINV.csv", getSubFrame(kapflows, 
#     sectorCodes, sectorCodes));

# RAW_DOM_CMED - all sectors flows of table 5
outputForm("RAW_DOM_CMED", getMultiSubFrame(newtable5, 
sectorCodes, sectorCodes));

# RAW_YSA_CMED - all sectors flows of table 8-5
outputForm("RAW_YSA_CMED", getMultiSubFrame(newtablediff, 
sectorCodes, sectorCodes));

# RAW_EXO_JOUT -  all sectors in the Q1 col of table 8
outputForm("RAW_EXO_JOUT", getSubFrame(newtable8, 
    sectorCodes, ["Q7"]));

# RAW_DOM_JOUT - all sectors in the (T6 - Q7) col of table 8
T6 = Vector(newtable8[1:numdiv, "T6"])
Q7 = Vector(newtable8[1:numdiv, "Q7"])
T5 = getSubFrame(newtable8, sectorCodes, ["Q7"])
T5[:, "Q7"] = T6 - Q7;
outputForm("RAW_DOM_JOUT", T5);

println("done saving to julia/output/*.csv")
